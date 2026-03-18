package perps

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/connectors"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/logging"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/persistence"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/state"
)

type Connector struct {
	name        string
	config      *Config
	wsClient    *WSClient
	state       *state.StateStore
	persistence *persistence.Writer
	logger      *zerolog.Logger

	running bool
	mu      sync.Mutex
	wg      sync.WaitGroup
	cancel  context.CancelFunc

	health HealthStatus

	// per-symbol liquidation tracking
	liquidations     map[string][]state.LiquidationEvent
	liquidationsMu   sync.Mutex
}

func NewConnector(
	cfg *Config,
	stateStore *state.StateStore,
	persistWriter *persistence.Writer,
	baseLogger *zerolog.Logger,
) connectors.Connector {
	logger := logging.Component(baseLogger, "binance_perps")

	if cfg.MaxLiquidations == 0 {
		cfg.MaxLiquidations = 100
	}
	if cfg.StaleThreshold == 0 {
		cfg.StaleThreshold = 2 * time.Second
	}
	if cfg.LiquidationWindowSecs == 0 {
		cfg.LiquidationWindowSecs = 60
	}

	liquidations := make(map[string][]state.LiquidationEvent, len(cfg.SymbolToKey))
	for _, canonical := range cfg.SymbolToKey {
		liquidations[canonical] = make([]state.LiquidationEvent, 0, cfg.MaxLiquidations)
	}

	return &Connector{
		name:         "binance_perps",
		config:       cfg,
		state:        stateStore,
		persistence:  persistWriter,
		logger:       logger,
		liquidations: liquidations,
		health: HealthStatus{
			IsConnected: false,
			IsStale:     true,
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

	wsConfig := &Config{
		Symbols:         c.config.Symbols,
		SymbolToKey:     c.config.SymbolToKey,
		WsURL:           c.config.WsURL,
		StaleThreshold:  c.config.StaleThreshold,
		MaxLiquidations: c.config.MaxLiquidations,
	}

	c.wsClient = NewWSClient(wsConfig, c.logger)

	c.wsClient.OnMarkPrice(c.handleMarkPrice)
	c.wsClient.OnLiquidation(c.handleLiquidation)
	c.wsClient.OnConnect(func() {
		c.mu.Lock()
		c.health.IsConnected = true
		c.health.Reconnects++
		c.mu.Unlock()

		c.logEvent(persistence.EventConnector, "connected", map[string]interface{}{
			"symbols": c.config.Symbols,
		})
	})
	c.wsClient.OnDisconnect(func(err error) {
		c.mu.Lock()
		c.health.IsConnected = false
		c.mu.Unlock()

		c.logEvent(persistence.EventConnector, "disconnected", map[string]interface{}{
			"error": err.Error(),
		})
	})

	for _, canonical := range c.config.SymbolToKey {
		c.state.UpdateBinancePerps(canonical, func(s *state.BinancePerpsState) {
			s.IsStale = true
		})
	}

	c.logEvent(persistence.EventSystem, "connector_starting", map[string]interface{}{
		"symbols":          c.config.Symbols,
		"stale_threshold":  c.config.StaleThreshold.String(),
		"max_liquidations": c.config.MaxLiquidations,
	})

	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer cancel()
		if err := c.wsClient.Run(ctx); err != nil && err != context.Canceled {
			c.logger.Error().Err(err).Msg("binance perps: websocket client exited with error")
		}
	}()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.checkStaleness()
				c.pruneLiquidations()
			}
		}
	}()

	return nil
}

func (c *Connector) handleMarkPrice(symbol string, data *MarkPriceData) {
	now := time.Now()

	c.mu.Lock()
	c.health.MarkUpdates++
	c.health.LastMarkUpdate = now
	c.health.IsStale = false
	c.mu.Unlock()

	c.state.UpdateBinancePerps(symbol, func(s *state.BinancePerpsState) {
		s.LastMarkPrice = data.MarkPrice
		s.LastMarkUpdate = now
		s.IsStale = false
	})

	c.state.UpdateSymbolFreshness(symbol, func(sf *state.SymbolFreshness) {
		sf.BinanceFresh = true
		sf.LastUpdate = now
	})

	c.logger.Debug().
		Str("symbol", symbol).
		Float64("mark_price", data.MarkPrice).
		Float64("index_price", data.IndexPrice).
		Msg("binance perps: mark price update")
}

func (c *Connector) handleLiquidation(symbol string, data *LiquidationData) {
	now := time.Now()

	c.mu.Lock()
	c.health.LiquidationEvents++
	c.health.LastLiquidation = now
	c.mu.Unlock()

	event := state.LiquidationEvent{
		Timestamp: now,
		Side:      data.Side,
		Price:     data.Price,
		Quantity:  data.Quantity,
	}

	c.liquidationsMu.Lock()
	liqList := c.liquidations[symbol]
	liqList = append(liqList, event)
	if len(liqList) > c.config.MaxLiquidations {
		liqList = liqList[1:]
	}
	c.liquidations[symbol] = liqList
	recentCount := c.countRecentLiquidations(symbol, now)
	c.liquidationsMu.Unlock()

	liquidationFlag := recentCount >= 3

	c.state.UpdateBinancePerps(symbol, func(s *state.BinancePerpsState) {
		s.RecentLiquidations = c.getLiquidationsCopy(symbol)
		s.LiquidationFlag = liquidationFlag
	})

	c.logEvent(persistence.EventConnector, "liquidation_event", map[string]interface{}{
		"symbol":       symbol,
		"side":         data.Side,
		"price":        data.Price,
		"quantity":     data.Quantity,
		"recent_count": recentCount,
		"cluster_flag": liquidationFlag,
	})

	c.logger.Warn().
		Str("symbol", symbol).
		Str("side", data.Side).
		Float64("price", data.Price).
		Float64("quantity", data.Quantity).
		Int("recent_count", recentCount).
		Bool("cluster_flag", liquidationFlag).
		Msg("binance perps: liquidation event")
}

func (c *Connector) getLiquidationsCopy(symbol string) []state.LiquidationEvent {
	c.liquidationsMu.Lock()
	defer c.liquidationsMu.Unlock()

	src := c.liquidations[symbol]
	cp := make([]state.LiquidationEvent, len(src))
	copy(cp, src)
	return cp
}

// countRecentLiquidations returns the number of liquidation events within the
// configured time window. Caller must hold liquidationsMu.
func (c *Connector) countRecentLiquidations(symbol string, now time.Time) int {
	cutoff := now.Add(-time.Duration(c.config.LiquidationWindowSecs) * time.Second)
	count := 0
	for _, e := range c.liquidations[symbol] {
		if e.Timestamp.After(cutoff) {
			count++
		}
	}
	return count
}

// pruneLiquidations removes expired events and updates the flag for all symbols.
func (c *Connector) pruneLiquidations() {
	now := time.Now()
	cutoff := now.Add(-time.Duration(c.config.LiquidationWindowSecs) * time.Second)

	c.liquidationsMu.Lock()
	for symbol, events := range c.liquidations {
		pruned := events[:0]
		for _, e := range events {
			if e.Timestamp.After(cutoff) {
				pruned = append(pruned, e)
			}
		}
		c.liquidations[symbol] = pruned

		recentCount := len(pruned)
		c.state.UpdateBinancePerps(symbol, func(s *state.BinancePerpsState) {
			s.LiquidationFlag = recentCount >= 3
		})
	}
	c.liquidationsMu.Unlock()
}

func (c *Connector) checkStaleness() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.health.IsConnected {
		c.health.IsStale = true
		c.markAllStale()
		return
	}

	if c.config.StaleThreshold > 0 {
		timeSinceMark := time.Since(c.health.LastMarkUpdate)

		if timeSinceMark > c.config.StaleThreshold {
			c.health.IsStale = true
			c.markAllStale()

			c.logger.Warn().
				Dur("mark_age", timeSinceMark).
				Msg("binance perps: feed marked as stale")
		}
	}
}

func (c *Connector) markAllStale() {
	for _, canonical := range c.config.SymbolToKey {
		c.state.UpdateBinancePerps(canonical, func(s *state.BinancePerpsState) {
			s.IsStale = true
		})
		c.state.UpdateSymbolFreshness(canonical, func(sf *state.SymbolFreshness) {
			sf.BinanceFresh = false
		})
	}

	c.state.UpdateRisk(func(s *state.RiskState) {
		s.FeedsHealthy = false
	})
}

func (c *Connector) Stop() error {
	c.mu.Lock()
	if !c.running {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	c.logger.Info().Msg("binance perps: stopping connector")

	if c.cancel != nil {
		c.cancel()
	}

	if c.wsClient != nil {
		c.wsClient.Stop()
	}

	c.wg.Wait()

	for _, canonical := range c.config.SymbolToKey {
		c.state.UpdateBinancePerps(canonical, func(s *state.BinancePerpsState) {
			s.IsStale = true
		})
		c.state.UpdateSymbolFreshness(canonical, func(sf *state.SymbolFreshness) {
			sf.BinanceFresh = false
		})
	}

	c.logEvent(persistence.EventSystem, "connector_stopped", nil)

	c.mu.Lock()
	c.running = false
	c.mu.Unlock()

	c.logger.Info().Msg("binance perps: connector stopped")
	return nil
}

func (c *Connector) Health() error {
	c.mu.Lock()
	running := c.running
	c.mu.Unlock()

	if !running {
		return fmt.Errorf("connector not running")
	}

	if c.wsClient == nil {
		return fmt.Errorf("websocket client not initialized")
	}

	if !c.health.IsConnected {
		return fmt.Errorf("not connected to data stream")
	}

	if c.health.IsStale {
		return fmt.Errorf("feed is stale")
	}

	return nil
}

func (c *Connector) logEvent(eventType persistence.EventType, message string, payload map[string]interface{}) {
	if c.persistence == nil {
		return
	}

	event := persistence.NewEvent("binance_perps", eventType, message)
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

func (c *Connector) GetStats() (int64, int64, int, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	totalLiquidations := 0
	c.liquidationsMu.Lock()
	for _, liq := range c.liquidations {
		totalLiquidations += len(liq)
	}
	c.liquidationsMu.Unlock()
	return c.health.MarkUpdates, c.health.LiquidationEvents, totalLiquidations, c.health.IsStale
}
