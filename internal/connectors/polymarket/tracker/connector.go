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

	activeMarkets   map[string]*ActiveMarket
	scheduledTimers map[string]*time.Timer
	marketHistory   []*CompletedMarket

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
	report, err := c.chainlinkClient.GetReportAtTimestampForFeed(ctx, feedID, timestamp)
	if err != nil {
		return 0, fmt.Errorf("get chainlink report: %w", err)
	}

	if report == nil {
		return 0, fmt.Errorf("nil report returned")
	}

	price := c.extractPriceFromReport(report)

	c.logger.Debug().
		Uint64("timestamp", timestamp).
		Float64("price", price).
		Str("feed_id", feedID).
		Msg("tracker: fetched chainlink price")

	return price, nil
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
