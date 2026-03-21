package tracker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
	streams "github.com/smartcontractkit/data-streams-sdk/go"
	"github.com/smartcontractkit/data-streams-sdk/go/report"
	v3 "github.com/smartcontractkit/data-streams-sdk/go/report/v3"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/connectors"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/connectors/chainlink"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/connectors/polymarket/gamma"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/connectors/polymarket/market"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/execution"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/logging"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/persistence"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/state"
)

type Connector struct {
	name   string
	config *TrackerConfig

	prefixToFeedID  map[string]string
	prefixToWindows map[string][]int
	prefixToSymbol  map[string]string

	gammaClient     *gamma.Client
	marketConnector *market.Connector
	chainlinkClient *chainlink.Client
	state           *state.StateStore
	persistence     *persistence.Writer
	logger          *zerolog.Logger

	execClient *execution.Client
	balance    *execution.BalanceTracker

	activeMarkets   map[string]*ActiveMarket
	scheduledTimers map[string]*time.Timer
	marketHistory   []*CompletedMarket
	pendingRedeems  []*PendingRedeem

	running bool
	mu      sync.RWMutex
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
}

func NewConnector(
	gammaClient *gamma.Client,
	marketConnector *market.Connector,
	chainlinkClient *chainlink.Client,
	cfg *TrackerConfig,
	prefixToFeedID map[string]string,
	prefixToWindows map[string][]int,
	prefixToSymbol map[string]string,
	stateStore *state.StateStore,
	persistWriter *persistence.Writer,
	baseLogger *zerolog.Logger,
) connectors.Connector {
	logger := logging.Component(baseLogger, "polymarket_tracker")

	return &Connector{
		name:            "polymarket_tracker",
		config:          cfg,
		prefixToFeedID:  prefixToFeedID,
		prefixToWindows: prefixToWindows,
		prefixToSymbol:  prefixToSymbol,
		gammaClient:     gammaClient,
		marketConnector: marketConnector,
		chainlinkClient: chainlinkClient,
		state:           stateStore,
		persistence:     persistWriter,
		logger:          logger,
		activeMarkets:   make(map[string]*ActiveMarket),
		scheduledTimers: make(map[string]*time.Timer),
		marketHistory:   make([]*CompletedMarket, 0),
		pendingRedeems:  make([]*PendingRedeem, 0),
	}
}

func (c *Connector) Name() string {
	return c.name
}

func (c *Connector) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return fmt.Errorf("connector already running")
	}
	c.running = true
	c.mu.Unlock()

	// Collect prefixes and all windows for logging
	var prefixes []string
	for prefix := range c.prefixToWindows {
		prefixes = append(prefixes, prefix)
	}

	c.logger.Info().
		Strs("prefixes", prefixes).
		Int("pre_schedule_windows", c.config.PreScheduleWindows).
		Msg("tracker: starting")

	c.logEvent(persistence.EventSystem, "connector_starting", map[string]interface{}{
		"prefix_count":         len(c.prefixToWindows),
		"pre_schedule_windows": c.config.PreScheduleWindows,
	})

	ctx, cancel := context.WithCancel(ctx)
	c.ctx = ctx
	c.cancel = cancel

	// Phase 1: fetch all market data and register active markets (no WS subscribe yet)
	var allAssetIDs []string
	for prefix, windows := range c.prefixToWindows {
		for _, window := range windows {
			currentSlug := gamma.GetCurrentSlug(prefix, window)

			c.logger.Info().
				Str("prefix", prefix).
				Int("window", window).
				Str("slug", currentSlug).
				Msg("tracker: fetching current market window")

			assetIDs, err := c.fetchAndRegisterMarket(currentSlug, prefix, window)
			if err != nil {
				c.logger.Error().
					Err(err).
					Str("slug", currentSlug).
					Msg("tracker: failed to fetch current market")
				continue
			}
			allAssetIDs = append(allAssetIDs, assetIDs...)
		}
	}

	// Phase 2: track all assets then reconnect to subscribe them in one WS message
	if len(allAssetIDs) > 0 {
		if err := c.marketConnector.SubscribeMany(allAssetIDs); err != nil {
			c.logger.Error().Err(err).Int("assets", len(allAssetIDs)).Msg("tracker: failed to track initial assets")
		}
		// Reconnect triggers ResubscribeAll which sends the full list on new connection
		c.marketConnector.Reconnect()
		c.logger.Info().Int("assets", len(allAssetIDs)).Msg("tracker: scheduled reconnect for initial subscription")
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer cancel()
		c.monitoringLoop(ctx)
	}()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.strategyAnalysisLoop(ctx)
	}()

	if c.execClient != nil {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.redeemLoop(ctx)
		}()
	}

	c.logger.Info().
		Int("active_markets", len(c.activeMarkets)).
		Msg("tracker: started successfully")

	return nil
}

func (c *Connector) Stop() error {
	c.mu.Lock()
	if !c.running {
		c.mu.Unlock()
		return nil
	}
	c.running = false
	c.mu.Unlock()

	c.logger.Info().Msg("tracker: stopping")

	for slug, timer := range c.scheduledTimers {
		timer.Stop()
		delete(c.scheduledTimers, slug)
	}

	for slug, market := range c.activeMarkets {
		if market.EndTimer != nil {
			market.EndTimer.Stop()
		}
		c.marketConnector.UnsubscribeMany([]string{
			market.YesAssetID,
			market.NoAssetID,
		})
		c.logger.Debug().Str("slug", slug).Msg("tracker: unsubscribed from market")
	}

	if c.cancel != nil {
		c.cancel()
	}

	c.wg.Wait()

	c.logEvent(persistence.EventSystem, "connector_stopped", map[string]interface{}{
		"total_markets_tracked": len(c.marketHistory),
	})

	c.logger.Info().Msg("tracker: stopped")
	return nil
}

func (c *Connector) Health() error {
	c.mu.RLock()
	running := c.running
	c.mu.RUnlock()

	if !running {
		return fmt.Errorf("tracker not running")
	}

	if err := c.marketConnector.Health(); err != nil {
		return fmt.Errorf("market connector unhealthy: %w", err)
	}

	return nil
}

func (c *Connector) getChainlinkPriceAtTimestamp(ctx context.Context, feedID string, timestamp uint64) (float64, error) {
	const maxAttempts = 4
	const retryDelay = 500 * time.Millisecond

	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(retryDelay):
			}
			c.logger.Warn().
				Err(lastErr).
				Int("attempt", attempt+1).
				Uint64("timestamp", timestamp).
				Str("feed_id", feedID).
				Msg("tracker: retrying chainlink report fetch")
		}

		rep, err := c.chainlinkClient.GetReportAtTimestampForFeed(ctx, feedID, timestamp)
		if err != nil {
			lastErr = err
			continue
		}
		if rep == nil {
			lastErr = fmt.Errorf("nil report returned")
			continue
		}

		price := c.extractPriceFromReport(rep)
		c.logger.Debug().
			Uint64("timestamp", timestamp).
			Float64("price", price).
			Str("feed_id", feedID).
			Int("attempt", attempt+1).
			Msg("tracker: fetched chainlink price")
		return price, nil
	}

	return 0, fmt.Errorf("get chainlink report after %d attempts: %w", maxAttempts, lastErr)
}

func (c *Connector) extractPriceFromReport(reportResp *streams.ReportResponse) float64 {
	decoded, err := report.Decode[v3.Data](reportResp.FullReport)
	if err != nil {
		c.logger.Error().Err(err).Msg("tracker: failed to decode chainlink report")
		return 0.0
	}
	if decoded.Data.BenchmarkPrice == nil {
		return 0.0
	}
	f, _ := decoded.Data.BenchmarkPrice.Float64()
	return f / 1e18
}

func (c *Connector) logEvent(eventType persistence.EventType, message string, payload map[string]interface{}) {
	if c.persistence == nil {
		return
	}

	event := persistence.NewEvent("polymarket_tracker", eventType, message)
	if payload != nil {
		event.WithPayloads(payload)
	}
	c.persistence.Write(event)
}

// SetExecutionClient injects the execution client and balance tracker for live trading.
func (c *Connector) SetExecutionClient(client *execution.Client, balance *execution.BalanceTracker) {
	c.execClient = client
	c.balance = balance
}

func (c *Connector) GetStats() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.marketHistory) == 0 {
		return map[string]interface{}{
			"active_markets": len(c.activeMarkets),
			"total_markets":  0,
		}
	}

	totalPnL := 0.0
	wins := 0
	losses := 0
	trades := 0

	for _, m := range c.marketHistory {
		totalPnL += m.PnL
		if m.PnL > 0 {
			wins++
		} else if m.PnL < 0 {
			losses++
		}
		if m.DidWeTrade {
			trades++
		}
	}

	winRate := 0.0
	if wins+losses > 0 {
		winRate = float64(wins) / float64(wins+losses) * 100
	}

	return map[string]interface{}{
		"active_markets": len(c.activeMarkets),
		"total_markets":  len(c.marketHistory),
		"trades_made":    trades,
		"wins":           wins,
		"losses":         losses,
		"win_rate":       winRate,
		"total_pnl":      totalPnL,
		"avg_pnl":        totalPnL / float64(len(c.marketHistory)),
	}
}

func (c *Connector) GetActiveWindowSummaries() []ActiveWindowSummary {
	c.mu.RLock()
	defer c.mu.RUnlock()

	pmState := c.state.GetPolymarket()
	summaries := make([]ActiveWindowSummary, 0, len(c.activeMarkets))

	for _, am := range c.activeMarkets {
		remaining := int64(time.Until(am.EndTime).Seconds())

		var yesMid, yesBid, yesAsk float64
		var noMid, noBid, noAsk float64

		if ob := pmState.OrderBooks[am.YesAssetID]; ob != nil {
			yesMid = ob.MidPrice
			yesBid = ob.BestBid
			yesAsk = ob.BestAsk
		}
		if ob := pmState.OrderBooks[am.NoAssetID]; ob != nil {
			noMid = ob.MidPrice
			noBid = ob.BestBid
			noAsk = ob.BestAsk
		}

		summaries = append(summaries, ActiveWindowSummary{
			Slug:           am.Slug,
			Symbol:         am.Symbol,
			WindowMin:      am.WindowType,
			StartPrice:     am.StartPrice,
			EndTime:        am.EndTime,
			TimeRemainingS: remaining,
			YesMid:         yesMid,
			YesBid:         yesBid,
			YesAsk:         yesAsk,
			NoMid:          noMid,
			NoBid:          noBid,
			NoAsk:          noAsk,
			YesAssetID:     am.YesAssetID,
			NoAssetID:      am.NoAssetID,
			HasPosition:    am.MyPosition != nil,
		})
	}

	return summaries
}

func (c *Connector) GetCurrentActiveMarkets() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	slugs := make([]string, 0, len(c.activeMarkets))
	for slug := range c.activeMarkets {
		slugs = append(slugs, slug)
	}
	return slugs
}

func (c *Connector) GetActiveMarket(slug string) *ActiveMarket {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if market, exists := c.activeMarkets[slug]; exists {
		return market
	}
	return nil
}

// RecoverPositions queries the execution engine for existing trades and restores
// MyPosition on any active market that already has a confirmed trade. This prevents
// duplicate orders after a restart.
func (c *Connector) RecoverPositions(ctx context.Context) error {
	if c.execClient == nil {
		return nil
	}

	trades, err := c.execClient.GetTrades(ctx)
	if err != nil {
		return fmt.Errorf("get trades: %w", err)
	}

	// Build map: assetID → trade (only CONFIRMED/MATCHED)
	tradeByAsset := make(map[string]execution.Trade, len(trades))
	for _, t := range trades {
		if t.Status == "CONFIRMED" || t.Status == "MATCHED" {
			tradeByAsset[t.AssetID] = t
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	recovered := 0
	for slug, am := range c.activeMarkets {
		if am.MyPosition != nil {
			continue
		}

		var matched *execution.Trade
		if t, ok := tradeByAsset[am.YesAssetID]; ok {
			matched = &t
		} else if t, ok := tradeByAsset[am.NoAssetID]; ok {
			matched = &t
		}
		if matched == nil {
			continue
		}

		side := "no"
		if matched.AssetID == am.YesAssetID {
			side = "yes"
		}

		size := 0.0
		cost := 0.0
		fmt.Sscanf(matched.Size, "%f", &size)
		fmt.Sscanf(matched.Price, "%f", &cost)

		am.MyPosition = &Position{
			AssetID:    matched.AssetID,
			Side:       side,
			EntryPrice: cost,
			Size:       size,
			Cost:       cost * size,
			IsLive:     true,
			OpenedAt:   time.Now(),
		}
		recovered++

		c.logger.Info().
			Str("slug", slug).
			Str("asset_id", matched.AssetID).
			Str("side", side).
			Float64("size", size).
			Float64("price", cost).
			Msg("tracker: recovered position from execution engine")
	}

	c.logger.Info().
		Int("trades_fetched", len(trades)).
		Int("positions_recovered", recovered).
		Msg("tracker: position recovery complete")

	return nil
}
