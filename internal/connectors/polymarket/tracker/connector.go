package tracker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
	streams "github.com/smartcontractkit/data-streams-sdk/go"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/connectors"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/connectors/chainlink"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/connectors/polymarket/gamma"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/connectors/polymarket/market"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/logging"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/persistence"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/state"
)

type Connector struct {
	name      string
	config    *TrackerConfig
	btcFeedID string

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
	btcFeedID string,
	stateStore *state.StateStore,
	persistWriter *persistence.Writer,
	baseLogger *zerolog.Logger,
) connectors.Connector {
	logger := logging.Component(baseLogger, "polymarket_tracker")

	return &Connector{
		name:            "polymarket_tracker",
		config:          cfg,
		btcFeedID:       btcFeedID,
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

	c.logger.Info().
		Strs("prefixes", c.config.MarketPrefixes).
		Ints("windows", c.config.Windows).
		Int("pre_schedule_windows", c.config.PreScheduleWindows).
		Msg("tracker: starting")

	c.logEvent(persistence.EventSystem, "connector_starting", map[string]interface{}{
		"market_prefixes":      c.config.MarketPrefixes,
		"windows":              c.config.Windows,
		"pre_schedule_windows": c.config.PreScheduleWindows,
	})

	ctx, cancel := context.WithCancel(ctx)
	c.ctx = ctx
	c.cancel = cancel

	for _, prefix := range c.config.MarketPrefixes {
		for _, window := range c.config.Windows {
			currentSlug := gamma.GetCurrentSlug(prefix, window)

			c.logger.Info().
				Str("prefix", prefix).
				Int("window", window).
				Str("slug", currentSlug).
				Msg("tracker: subscribing to current market window")

			if err := c.fetchAndSubscribe(currentSlug, prefix, window); err != nil {
				c.logger.Error().
					Err(err).
					Str("slug", currentSlug).
					Msg("tracker: failed to subscribe to current market")
			}
		}
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

func (c *Connector) getChainlinkPriceAtTimestamp(ctx context.Context, timestamp uint64) (float64, error) {
	report, err := c.chainlinkClient.GetReportAtTimestampForFeed(ctx, c.btcFeedID, timestamp)
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
		Str("feed_id", c.btcFeedID).
		Msg("tracker: fetched chainlink price")

	return price, nil
}

func (c *Connector) extractPriceFromReport(report *streams.ReportResponse) float64 {
	return 0.0
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
