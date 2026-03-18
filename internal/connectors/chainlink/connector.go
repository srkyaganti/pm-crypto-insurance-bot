package chainlink

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/rs/zerolog"
	streams "github.com/smartcontractkit/data-streams-sdk/go"
	"github.com/smartcontractkit/data-streams-sdk/go/report"
	v3 "github.com/smartcontractkit/data-streams-sdk/go/report/v3"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/logging"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/persistence"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/state"
)

type Connector struct {
	name        string
	config      *Config
	client      *Client
	state       *state.StateStore
	persistence *persistence.Writer
	logger      *zerolog.Logger

	running bool
	mu      sync.Mutex
	wg      sync.WaitGroup
	cancel  context.CancelFunc

	health     HealthStatus
	lastUpdate time.Time
}

func NewConnector(
	cfg *Config,
	stateStore *state.StateStore,
	persistWriter *persistence.Writer,
	baseLogger *zerolog.Logger,
) *Connector {
	logger := logging.Component(baseLogger, "chainlink")

	perSymbol := make(map[string]*SymbolHealth, len(cfg.FeedIDs))
	for symbol := range cfg.FeedIDs {
		perSymbol[symbol] = &SymbolHealth{}
	}

	return &Connector{
		name:        "chainlink",
		config:      cfg,
		state:       stateStore,
		persistence: persistWriter,
		logger:      logger,
		health: HealthStatus{
			IsConnected: false,
			IsStale:     true,
			PerSymbol:   perSymbol,
		},
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

	client, err := NewClient(c.config, c.logger)
	if err != nil {
		c.running = false
		return fmt.Errorf("create client: %w", err)
	}
	c.client = client

	if err := c.client.Initialize(); err != nil {
		c.running = false
		return fmt.Errorf("initialize client: %w", err)
	}

	for symbol := range c.config.FeedIDs {
		c.state.UpdateChainlink(symbol, func(s *state.ChainlinkState) {
			s.IsStale = true
		})
	}

	c.logEvent(persistence.EventSystem, "connector_starting", map[string]interface{}{
		"feed_count":      len(c.config.FeedIDs),
		"stale_threshold": c.config.StaleThreshold.String(),
		"ha_mode":         c.config.WsHA,
	})

	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer cancel()
		if err := c.runStreamLoop(ctx); err != nil && err != context.Canceled {
			c.logger.Error().Err(err).Msg("chainlink: stream loop exited with error")
		}
	}()

	return nil
}

func (c *Connector) runStreamLoop(ctx context.Context) error {
	statusCallback := func(isConnected bool) {
		c.mu.Lock()
		c.health.IsConnected = isConnected
		c.mu.Unlock()

		if isConnected {
			c.logEvent(persistence.EventConnector, "connected", map[string]interface{}{
				"feed_count": len(c.config.FeedIDs),
			})
		} else {
			c.logEvent(persistence.EventConnector, "disconnected", nil)
		}
	}

	if err := c.client.Stream(ctx, statusCallback); err != nil {
		return fmt.Errorf("start stream: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			c.logger.Info().Msg("chainlink: context cancelled, stopping stream")
			return ctx.Err()
		default:
		}

		reportResp, err := c.client.Read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.logger.Error().Err(err).Msg("chainlink: read error")
			c.health.Errors++
			continue
		}

		c.health.ReportsCount++
		c.health.LastUpdate = time.Now()
		c.health.IsStale = false

		if err := c.handleReport(reportResp); err != nil {
			c.logger.Error().Err(err).Msg("chainlink: handle report error")
		}
	}
}

func (c *Connector) handleReport(reportResp *streams.ReportResponse) error {
	decoded, err := report.Decode[v3.Data](reportResp.FullReport)
	if err != nil {
		return fmt.Errorf("decode report: %w", err)
	}

	feedIDStr := decoded.Data.FeedID.String()
	symbol := c.config.FeedIDToSymbol[feedIDStr]
	if symbol == "" {
		return fmt.Errorf("unknown feed ID: %s", feedIDStr)
	}

	price := bigIntToFloat(decoded.Data.BenchmarkPrice)
	now := time.Now()

	c.state.UpdateChainlink(symbol, func(s *state.ChainlinkState) {
		s.LatestPrice = price
		s.LastUpdate = now
		s.IsStale = false

		tick := state.PriceTick{
			Timestamp: now,
			Price:     price,
		}
		s.Ticks.Add(tick)
	})

	c.state.UpdateSymbolFreshness(symbol, func(sf *state.SymbolFreshness) {
		sf.ChainlinkFresh = true
		sf.LastUpdate = now
	})

	c.mu.Lock()
	if sh := c.health.PerSymbol[symbol]; sh != nil {
		sh.LastUpdate = now
		sh.ReportsCount++
		sh.IsStale = false
	}
	c.mu.Unlock()

	c.lastUpdate = now

	c.logger.Info().
		Str("symbol", symbol).
		Float64("price", price).
		Uint32("obs_ts", decoded.Data.ObservationsTimestamp).
		Int64("now_ts", now.Unix()).
		Msg("chainlink: price update")

	return nil
}

func bigIntToFloat(bi *big.Int) float64 {
	if bi == nil {
		return 0
	}
	f, _ := bi.Float64()
	return f / 1e8
}

func (c *Connector) Stop() error {
	c.mu.Lock()
	if !c.running {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	c.logger.Info().Msg("chainlink: stopping connector")

	if c.cancel != nil {
		c.cancel()
	}

	if c.client != nil {
		c.client.Close()
	}

	c.wg.Wait()

	for symbol := range c.config.FeedIDs {
		c.state.UpdateChainlink(symbol, func(s *state.ChainlinkState) {
			s.IsStale = true
		})
		c.state.UpdateSymbolFreshness(symbol, func(sf *state.SymbolFreshness) {
			sf.ChainlinkFresh = false
		})
	}

	c.logEvent(persistence.EventSystem, "connector_stopped", nil)

	c.mu.Lock()
	c.running = false
	c.mu.Unlock()

	c.logger.Info().Msg("chainlink: connector stopped")
	return nil
}

func (c *Connector) Health() error {
	c.mu.Lock()
	running := c.running
	c.mu.Unlock()

	if !running {
		return fmt.Errorf("connector not running")
	}

	if c.client == nil {
		return fmt.Errorf("client not initialized")
	}

	if !c.health.IsConnected {
		return fmt.Errorf("not connected to data stream")
	}

	if c.isStale() {
		return fmt.Errorf("feed is stale")
	}

	return nil
}

func (c *Connector) isStale() bool {
	if !c.health.IsConnected {
		return true
	}
	if c.config.StaleThreshold <= 0 {
		return false
	}
	return time.Since(c.lastUpdate) > c.config.StaleThreshold
}

func (c *Connector) checkStaleness() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.health.IsStale = c.isStale()

	if c.health.IsStale {
		for symbol := range c.config.FeedIDs {
			c.state.UpdateChainlink(symbol, func(s *state.ChainlinkState) {
				s.IsStale = true
			})
			c.state.UpdateSymbolFreshness(symbol, func(sf *state.SymbolFreshness) {
				sf.ChainlinkFresh = false
			})
		}

		c.state.UpdateRisk(func(s *state.RiskState) {
			s.FeedsHealthy = false
		})

		c.logger.Warn().
			Bool("connected", c.health.IsConnected).
			Time("last_update", c.lastUpdate).
			Msg("chainlink: feed marked as stale")
	}
}

func (c *Connector) logEvent(eventType persistence.EventType, message string, payload map[string]interface{}) {
	if c.persistence == nil {
		return
	}

	event := persistence.NewEvent("chainlink", eventType, message)
	if payload != nil {
		event.WithPayloads(payload)
	}
	c.persistence.Write(event)
}

func (c *Connector) IsRunning() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.running
}

func (c *Connector) GetStats() (int64, int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.health.ReportsCount, c.health.Errors
}

func (c *Connector) decodeReport(reportResp *streams.ReportResponse) (*DecodedReport, error) {
	decoded, err := report.Decode[v3.Data](reportResp.FullReport)
	if err != nil {
		return nil, fmt.Errorf("decode report: %w", err)
	}

	return &DecodedReport{
		FeedID:                decoded.Data.FeedID.String(),
		BenchmarkPrice:        bigIntToFloat(decoded.Data.BenchmarkPrice),
		ObservationsTimestamp: decoded.Data.ObservationsTimestamp,
		ValidFromTimestamp:    decoded.Data.ValidFromTimestamp,
	}, nil
}

func (c *Connector) GetReportAtTimestamp(ctx context.Context, feedIDHex string, timestamp uint64) (*DecodedReport, error) {
	reportResp, err := c.client.GetReportAtTimestampForFeed(ctx, feedIDHex, timestamp)
	if err != nil {
		return nil, err
	}

	decoded, err := c.decodeReport(reportResp)
	if err != nil {
		return nil, err
	}

	c.logger.Debug().
		Str("feed_id", decoded.FeedID).
		Uint64("timestamp", timestamp).
		Float64("benchmark_price", decoded.BenchmarkPrice).
		Msg("chainlink: fetched historical report")

	return decoded, nil
}

func (c *Connector) GetReportAtTimestampForFeed(ctx context.Context, feedID string, timestamp uint64) (*DecodedReport, error) {
	reportResp, err := c.client.GetReportAtTimestampForFeed(ctx, feedID, timestamp)
	if err != nil {
		return nil, err
	}

	decoded, err := c.decodeReport(reportResp)
	if err != nil {
		return nil, err
	}

	c.logger.Debug().
		Str("feed_id", decoded.FeedID).
		Uint64("timestamp", timestamp).
		Float64("benchmark_price", decoded.BenchmarkPrice).
		Msg("chainlink: fetched historical report")

	return decoded, nil
}
