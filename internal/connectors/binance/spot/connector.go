package spot

import (
	"context"
	"fmt"
	"strings"
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
	snapshot    *SnapshotClient
	orderBook   *OrderBook
	state       *state.StateStore
	persistence *persistence.Writer
	logger      *zerolog.Logger

	running bool
	mu      sync.Mutex
	wg      sync.WaitGroup
	cancel  context.CancelFunc

	health       HealthStatus
	lastUpdateID int64
	synced       bool
}

func NewConnector(
	cfg *Config,
	stateStore *state.StateStore,
	persistWriter *persistence.Writer,
	baseLogger *zerolog.Logger,
) connectors.Connector {
	logger := logging.Component(baseLogger, "binance_spot")

	if cfg.MaxDepthLevels == 0 {
		cfg.MaxDepthLevels = 20
	}
	if cfg.StaleThreshold == 0 {
		cfg.StaleThreshold = 2 * time.Second
	}
	if cfg.DepthSpeed == "" {
		cfg.DepthSpeed = "100ms"
	}

	return &Connector{
		name:        "binance_spot",
		config:      cfg,
		state:       stateStore,
		persistence: persistWriter,
		logger:      logger,
		orderBook:   NewOrderBook(),
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

	wsURL := c.config.WsURL
	if wsURL == "" {
		wsURL = "wss://stream.binance.com:9443"
	}

	restURL := c.config.RestURL
	if restURL == "" {
		restURL = "https://api.binance.com"
	}

	wsConfig := &Config{
		Symbol:         strings.ToLower(c.config.Symbol),
		WsURL:          wsURL,
		DepthSpeed:     c.config.DepthSpeed,
		MaxDepthLevels: c.config.MaxDepthLevels,
		StaleThreshold: c.config.StaleThreshold,
	}

	c.wsClient = NewWSClient(wsConfig, c.logger)
	c.snapshot = NewSnapshotClient(&Config{
		Symbol:  c.config.Symbol,
		RestURL: restURL,
	})

	c.wsClient.OnDepthUpdate(c.handleDepthUpdate)
	c.wsClient.OnBookTicker(c.handleBookTicker)
	c.wsClient.OnConnect(func() {
		c.health.IsConnected = true
		c.logEvent(persistence.EventConnector, "connected", map[string]interface{}{
			"symbol": c.config.Symbol,
		})

		if err := c.syncOrderBook(ctx); err != nil {
			c.logger.Error().Err(err).Msg("binance spot: orderbook sync failed")
		}
	})
	c.wsClient.OnDisconnect(func(err error) {
		c.health.IsConnected = false
		c.health.SnapshotSynced = false
		c.synced = false
		c.logEvent(persistence.EventConnector, "disconnected", map[string]interface{}{
			"error": err.Error(),
		})
	})

	c.state.UpdateBinanceSpot(func(s *state.BinanceSpotState) {
		s.Symbol = c.config.Symbol
		s.IsStale = true
	})

	c.logEvent(persistence.EventSystem, "connector_starting", map[string]interface{}{
		"symbol":          c.config.Symbol,
		"depth_speed":     c.config.DepthSpeed,
		"max_levels":      c.config.MaxDepthLevels,
		"stale_threshold": c.config.StaleThreshold.String(),
	})

	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer cancel()
		if err := c.wsClient.Run(ctx); err != nil && err != context.Canceled {
			c.logger.Error().Err(err).Msg("binance spot: websocket client exited with error")
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
			}
		}
	}()

	return nil
}

func (c *Connector) syncOrderBook(ctx context.Context) error {
	c.logger.Info().Msg("binance spot: starting orderbook sync")

	snapshot, err := c.snapshot.FetchDepthSnapshot(ctx)
	if err != nil {
		c.health.Errors++
		return fmt.Errorf("fetch snapshot: %w", err)
	}

	c.logger.Info().
		Int64("last_update_id", snapshot.LastUpdateID).
		Int("bids", len(snapshot.Bids)).
		Int("asks", len(snapshot.Asks)).
		Msg("binance spot: received depth snapshot")

	bids, asks := ParsePriceLevelsFromSnapshot(snapshot, c.config.MaxDepthLevels)
	c.orderBook.ApplySnapshot(bids, asks, c.config.MaxDepthLevels)
	c.lastUpdateID = snapshot.LastUpdateID
	c.synced = true
	c.health.SnapshotSynced = true

	c.logger.Info().Msg("binance spot: orderbook sync complete")

	c.updateState()

	return nil
}

func (c *Connector) handleDepthUpdate(data *DepthData) {
	if !c.synced {
		return
	}

	if data.FirstUpdateID > c.lastUpdateID+1 {
		c.logger.Warn().
			Int64("expected", c.lastUpdateID+1).
			Int64("got", data.FirstUpdateID).
			Msg("binance spot: depth update gap detected, resyncing")

		c.synced = false
		c.health.SnapshotSynced = false
		go func() {
			if err := c.syncOrderBook(context.Background()); err != nil {
				c.logger.Error().Err(err).Msg("binance spot: resync failed")
			}
		}()
		return
	}

	if data.FinalUpdateID < c.lastUpdateID {
		return
	}

	c.orderBook.ApplyUpdate(data.Bids, data.Asks)
	c.lastUpdateID = data.FinalUpdateID
	c.health.DepthUpdates++
	c.health.LastDepthUpdate = time.Now()
	c.health.IsStale = false

	c.updateState()
}

func (c *Connector) handleBookTicker(data *BookTickerData) {
	c.health.BookTickerUpdates++
	c.health.LastBookTicker = time.Now()

	c.state.UpdateBinanceSpot(func(s *state.BinanceSpotState) {
		s.OrderBook.BestBid = data.BidPrice
		s.OrderBook.BestAsk = data.AskPrice
		s.OrderBook.MidPrice = (data.BidPrice + data.AskPrice) / 2
		s.LastBookTicker = time.Now()
		s.IsStale = false
	})

	c.state.UpdateRisk(func(s *state.RiskState) {
		s.BinanceFresh = true
		s.LastRiskUpdate = time.Now()
	})
}

func (c *Connector) updateState() {
	bids, asks := c.orderBook.GetTopLevels(c.config.MaxDepthLevels)
	bestBid, bestAsk, midPrice := c.orderBook.GetBestBidAsk()

	c.state.UpdateBinanceSpot(func(s *state.BinanceSpotState) {
		s.Symbol = c.config.Symbol
		s.OrderBook.Bids = make([]state.OrderLevel, 0, len(bids))
		for _, bid := range bids {
			s.OrderBook.Bids = append(s.OrderBook.Bids, state.OrderLevel{
				Price: bid.Price,
				Size:  bid.Size,
			})
		}
		s.OrderBook.Asks = make([]state.OrderLevel, 0, len(asks))
		for _, ask := range asks {
			s.OrderBook.Asks = append(s.OrderBook.Asks, state.OrderLevel{
				Price: ask.Price,
				Size:  ask.Size,
			})
		}
		s.OrderBook.BestBid = bestBid
		s.OrderBook.BestAsk = bestAsk
		s.OrderBook.MidPrice = midPrice
		s.LastDepthUpdate = time.Now()
		s.IsStale = false
	})

	c.state.UpdateRisk(func(s *state.RiskState) {
		s.BinanceFresh = true
		s.LastRiskUpdate = time.Now()
	})
}

func (c *Connector) checkStaleness() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.health.IsConnected {
		c.health.IsStale = true
		c.markStale()
		return
	}

	if c.config.StaleThreshold > 0 {
		timeSinceDepth := time.Since(c.health.LastDepthUpdate)
		timeSinceTicker := time.Since(c.health.LastBookTicker)

		if timeSinceDepth > c.config.StaleThreshold || timeSinceTicker > c.config.StaleThreshold {
			c.health.IsStale = true
			c.markStale()

			c.logger.Warn().
				Dur("depth_age", timeSinceDepth).
				Dur("ticker_age", timeSinceTicker).
				Msg("binance spot: feed marked as stale")
		}
	}
}

func (c *Connector) markStale() {
	c.state.UpdateBinanceSpot(func(s *state.BinanceSpotState) {
		s.IsStale = true
	})

	c.state.UpdateRisk(func(s *state.RiskState) {
		s.BinanceFresh = false
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

	c.logger.Info().Msg("binance spot: stopping connector")

	if c.cancel != nil {
		c.cancel()
	}

	if c.wsClient != nil {
		c.wsClient.Stop()
	}

	c.wg.Wait()

	c.state.UpdateBinanceSpot(func(s *state.BinanceSpotState) {
		s.IsStale = true
	})

	c.state.UpdateRisk(func(s *state.RiskState) {
		s.BinanceFresh = false
	})

	c.logEvent(persistence.EventSystem, "connector_stopped", nil)

	c.mu.Lock()
	c.running = false
	c.mu.Unlock()

	c.logger.Info().Msg("binance spot: connector stopped")
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

	if !c.health.SnapshotSynced {
		return fmt.Errorf("orderbook not synced")
	}

	return nil
}

func (c *Connector) logEvent(eventType persistence.EventType, message string, payload map[string]interface{}) {
	if c.persistence == nil {
		return
	}

	event := persistence.NewEvent("binance_spot", eventType, message)
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

func (c *Connector) GetStats() (int64, int64, int64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.health.DepthUpdates, c.health.BookTickerUpdates, c.health.Errors, c.health.SnapshotSynced
}
