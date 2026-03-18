package spot

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

	wsConfig := &Config{
		Symbols:        c.config.Symbols,
		SymbolToKey:    c.config.SymbolToKey,
		WsURL:          wsURL,
		DepthSpeed:     c.config.DepthSpeed,
		MaxDepthLevels: c.config.MaxDepthLevels,
		StaleThreshold: c.config.StaleThreshold,
	}

	c.wsClient = NewWSClient(wsConfig, c.logger)

	c.wsClient.OnDepthUpdate(c.handleDepthSnapshot)
	c.wsClient.OnAggTrade(c.handleAggTrade)
	c.wsClient.OnConnect(func() {
		c.health.IsConnected = true
		c.logEvent(persistence.EventConnector, "connected", map[string]interface{}{
			"symbols": c.config.Symbols,
			"streams": len(c.config.Symbols) * 2,
		})
	})
	c.wsClient.OnDisconnect(func(err error) {
		c.health.IsConnected = false
		c.logEvent(persistence.EventConnector, "disconnected", map[string]interface{}{
			"error": err.Error(),
		})
	})

	for symbol := range c.config.SymbolToKey {
		canonical := c.config.SymbolToKey[symbol]
		c.state.UpdateBinanceSpot(canonical, func(s *state.BinanceSpotState) {
			s.Symbol = symbol
			s.IsStale = true
		})
	}

	c.logEvent(persistence.EventSystem, "connector_starting", map[string]interface{}{
		"symbols":         c.config.Symbols,
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

func (c *Connector) handleDepthSnapshot(symbol string, data *DepthData) {
	c.health.DepthUpdates++
	c.health.LastDepthUpdate = time.Now()
	c.health.IsStale = false

	c.state.UpdateBinanceSpot(symbol, func(s *state.BinanceSpotState) {
		s.OrderBook.Bids = make([]state.OrderLevel, 0, len(data.Bids))
		cumBid := 0.0
		for _, bid := range data.Bids {
			s.OrderBook.Bids = append(s.OrderBook.Bids, state.OrderLevel{
				Price: bid.Price,
				Size:  bid.Size,
			})
			cumBid += bid.Size
		}

		s.OrderBook.Asks = make([]state.OrderLevel, 0, len(data.Asks))
		cumAsk := 0.0
		for _, ask := range data.Asks {
			s.OrderBook.Asks = append(s.OrderBook.Asks, state.OrderLevel{
				Price: ask.Price,
				Size:  ask.Size,
			})
			cumAsk += ask.Size
		}

		if len(s.OrderBook.Bids) > 0 {
			s.OrderBook.BestBid = s.OrderBook.Bids[0].Price
		}
		if len(s.OrderBook.Asks) > 0 {
			s.OrderBook.BestAsk = s.OrderBook.Asks[0].Price
		}
		if s.OrderBook.BestBid > 0 && s.OrderBook.BestAsk > 0 {
			s.OrderBook.MidPrice = (s.OrderBook.BestBid + s.OrderBook.BestAsk) / 2
		}

		s.OrderBook.CumBidLiquidity = cumBid
		s.OrderBook.CumAskLiquidity = cumAsk
		s.OrderBook.LastUpdate = time.Now()
		s.OrderBook.UpdateCount++

		s.LastDepthUpdate = time.Now()
		s.IsStale = false
	})

	c.state.UpdateSymbolFreshness(symbol, func(sf *state.SymbolFreshness) {
		sf.BinanceFresh = true
		sf.LastUpdate = time.Now()
	})
}

func (c *Connector) handleAggTrade(symbol string, data *AggTradeData) {
	c.health.AggTradeUpdates++
	c.health.LastAggTrade = time.Now()

	c.state.UpdateBinanceSpot(symbol, func(s *state.BinanceSpotState) {
		c.updateFlowBucket(s, data)
	})
}

func (c *Connector) updateFlowBucket(s *state.BinanceSpotState, trade *AggTradeData) {
	now := time.Now()
	nowMs := now.UnixNano() / 1e6
	currentBucketTime := (nowMs / 100) * 100

	lastBucketTime := s.TradeFlow.Buckets[s.TradeFlow.BucketIdx].Timestamp
	if lastBucketTime == 0 {
		s.TradeFlow.Buckets[0] = state.FlowBucket{Timestamp: currentBucketTime}
		s.TradeFlow.BucketIdx = 0
	} else if currentBucketTime > lastBucketTime {
		bucketsPassed := int((currentBucketTime - lastBucketTime) / 100)
		if bucketsPassed > 300 {
			bucketsPassed = 300
		}

		for i := 0; i < bucketsPassed; i++ {
			s.TradeFlow.BucketIdx = (s.TradeFlow.BucketIdx + 1) % 300
			s.TradeFlow.Buckets[s.TradeFlow.BucketIdx] = state.FlowBucket{}
		}
		s.TradeFlow.Buckets[s.TradeFlow.BucketIdx].Timestamp = currentBucketTime
	}

	bucket := &s.TradeFlow.Buckets[s.TradeFlow.BucketIdx]
	if trade.IsBuy {
		bucket.BuyNotional += trade.Notional
		s.TradeFlow.TotalBuyNotional += trade.Notional
	} else {
		bucket.SellNotional += trade.Notional
		s.TradeFlow.TotalSellNotional += trade.Notional
	}
	bucket.TradeCount++
	s.TradeFlow.TotalTrades++
	s.TradeFlow.LastTrade = now

	c.calculateFlowWindows(s, nowMs)
}

func (c *Connector) calculateFlowWindows(s *state.BinanceSpotState, nowMs int64) {
	var win10s, win30s state.FlowWindow

	for i := 0; i < 300; i++ {
		bucket := s.TradeFlow.Buckets[i]
		if bucket.Timestamp == 0 {
			continue
		}

		age := nowMs - bucket.Timestamp

		if age < 10000 {
			win10s.BuyNotional += bucket.BuyNotional
			win10s.SellNotional += bucket.SellNotional
			win10s.TradeCount += bucket.TradeCount
		}

		if age < 30000 {
			win30s.BuyNotional += bucket.BuyNotional
			win30s.SellNotional += bucket.SellNotional
			win30s.TradeCount += bucket.TradeCount
		}
	}

	total10 := win10s.BuyNotional + win10s.SellNotional
	if total10 > 0 {
		win10s.Imbalance = (win10s.BuyNotional - win10s.SellNotional) / total10
	}

	total30 := win30s.BuyNotional + win30s.SellNotional
	if total30 > 0 {
		win30s.Imbalance = (win30s.BuyNotional - win30s.SellNotional) / total30
	}

	s.TradeFlow.Window10s = win10s
	s.TradeFlow.Window30s = win30s
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
		timeSinceDepth := time.Since(c.health.LastDepthUpdate)

		if timeSinceDepth > c.config.StaleThreshold {
			c.health.IsStale = true
			c.markAllStale()

			c.logger.Warn().
				Dur("depth_age", timeSinceDepth).
				Msg("binance spot: feed marked as stale")
		}
	}
}

func (c *Connector) markAllStale() {
	for _, canonical := range c.config.SymbolToKey {
		c.state.UpdateBinanceSpot(canonical, func(s *state.BinanceSpotState) {
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

	c.logger.Info().Msg("binance spot: stopping connector")

	if c.cancel != nil {
		c.cancel()
	}

	if c.wsClient != nil {
		c.wsClient.Stop()
	}

	c.wg.Wait()

	for _, canonical := range c.config.SymbolToKey {
		c.state.UpdateBinanceSpot(canonical, func(s *state.BinanceSpotState) {
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

func (c *Connector) GetStats() (int64, int64, int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.health.DepthUpdates, c.health.AggTradeUpdates, c.health.Errors
}
