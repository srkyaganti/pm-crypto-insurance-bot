package market

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/connectors"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/logging"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/state"
)

type Connector struct {
	name     string
	config   *Config
	wsClient *WSClient
	state    *state.StateStore
	logger   *zerolog.Logger

	running bool
	mu      sync.Mutex
	wg      sync.WaitGroup
	cancel  context.CancelFunc

	health HealthStatus
}

type HealthStatus struct {
	IsConnected     bool
	IsStale         bool
	LastBookUpdate  map[string]time.Time
	LastPriceChange map[string]time.Time
	LastTrade       map[string]time.Time
	BookUpdates     map[string]int64
	PriceChanges    map[string]int64
	Trades          map[string]int64
	Errors          int64
}

func NewConnector(
	cfg *Config,
	stateStore *state.StateStore,
	baseLogger *zerolog.Logger,
) connectors.Connector {
	logger := logging.Component(baseLogger, "polymarket_market")

	return &Connector{
		name:   "polymarket_market",
		config: cfg,
		state:  stateStore,
		logger: logger,
		health: HealthStatus{
			IsConnected:     false,
			IsStale:         true,
			LastBookUpdate:  make(map[string]time.Time),
			LastPriceChange: make(map[string]time.Time),
			LastTrade:       make(map[string]time.Time),
			BookUpdates:     make(map[string]int64),
			PriceChanges:    make(map[string]int64),
			Trades:          make(map[string]int64),
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

	c.state.UpdatePolymarket(func(s *state.PolymarketState) {
		s.OrderBooks = make(map[string]*state.PMOrderBook)
		s.MarketAssets = make(map[string]*state.MarketAssetMeta)
		s.ActiveOrders = make(map[string]*state.OrderStatus)
		s.SubscribedAssets = make([]string, 0)
		s.IsStale = true
	})

	c.wsClient = NewWSClient(c.config, c.logger)

	c.wsClient.OnBook(c.handleBook)
	c.wsClient.OnPriceChange(c.handlePriceChange)
	c.wsClient.OnTickSizeChange(c.handleTickSizeChange)
	c.wsClient.OnLastTradePrice(c.handleLastTradePrice)
	c.wsClient.OnBestBidAsk(c.handleBestBidAsk)
	c.wsClient.OnNewMarket(c.handleNewMarket)
	c.wsClient.OnMarketResolved(c.handleMarketResolved)

	c.wsClient.OnConnect(func() {
		c.health.IsConnected = true
		c.logger.Info().Msg("polymarket market: connected to websocket")

		if len(c.config.InitialAssetIDs) > 0 {
			c.wsClient.SubscribeMany(c.config.InitialAssetIDs)
			c.state.UpdatePolymarket(func(s *state.PolymarketState) {
				s.SubscribedAssets = append(s.SubscribedAssets, c.config.InitialAssetIDs...)
			})
		}
	})

	c.wsClient.OnDisconnect(func(err error) {
		c.health.IsConnected = false
		c.logger.Error().Err(err).Msg("polymarket market: disconnected from websocket")
	})

	c.logger.Info().
		Int("initial_assets", len(c.config.InitialAssetIDs)).
		Bool("custom_features", c.config.CustomFeatureEnabled).
		Msg("polymarket market: starting connector")

	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer cancel()
		if err := c.wsClient.Run(ctx); err != nil && err != context.Canceled {
			c.logger.Error().Err(err).Msg("polymarket market: websocket client exited with error")
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

func (c *Connector) handleBook(event *BookEvent) {
	assetID := event.AssetID

	c.mu.Lock()
	c.health.BookUpdates[assetID]++
	c.health.LastBookUpdate[assetID] = time.Now()
	c.mu.Unlock()

	c.state.UpdatePolymarket(func(s *state.PolymarketState) {
		if s.OrderBooks[assetID] == nil {
			s.OrderBooks[assetID] = &state.PMOrderBook{
				Bids: make([]state.PMOrderLevel, 0),
				Asks: make([]state.PMOrderLevel, 0),
			}
		}

		book := s.OrderBooks[assetID]

		book.Bids = make([]state.PMOrderLevel, 0, len(event.Bids))
		for _, bid := range event.Bids {
			price, _ := strconv.ParseFloat(bid.Price, 64)
			size, _ := strconv.ParseFloat(bid.Size, 64)
			book.Bids = append(book.Bids, state.PMOrderLevel{
				Price: price,
				Size:  size,
			})
		}

		book.Asks = make([]state.PMOrderLevel, 0, len(event.Asks))
		for _, ask := range event.Asks {
			price, _ := strconv.ParseFloat(ask.Price, 64)
			size, _ := strconv.ParseFloat(ask.Size, 64)
			book.Asks = append(book.Asks, state.PMOrderLevel{
				Price: price,
				Size:  size,
			})
		}

		if len(book.Bids) > 0 {
			book.BestBid = book.Bids[0].Price
		}
		if len(book.Asks) > 0 {
			book.BestAsk = book.Asks[0].Price
		}
		if book.BestBid > 0 && book.BestAsk > 0 {
			book.MidPrice = (book.BestBid + book.BestAsk) / 2
		}

		book.LastUpdate = time.Now()

		if s.MarketAssets[assetID] == nil {
			s.MarketAssets[assetID] = &state.MarketAssetMeta{
				AssetID:     assetID,
				ConditionID: event.Market,
			}
		}

		s.LastWSUpdate = time.Now()
		s.IsStale = false
	})

	c.state.UpdateRisk(func(s *state.RiskState) {
		s.PolymarketFresh = true
		s.LastRiskUpdate = time.Now()
	})

	c.logger.Debug().
		Str("asset_id", assetID).
		Int("bids", len(event.Bids)).
		Int("asks", len(event.Asks)).
		Msg("polymarket market: orderbook update")
}

func (c *Connector) handlePriceChange(event *PriceChangeEvent) {
	for _, pc := range event.PriceChanges {
		assetID := pc.AssetID

		c.mu.Lock()
		c.health.PriceChanges[assetID]++
		c.health.LastPriceChange[assetID] = time.Now()
		c.mu.Unlock()

		c.logger.Debug().
			Str("asset_id", assetID).
			Str("price", pc.Price).
			Str("size", pc.Size).
			Str("side", pc.Side).
			Msg("polymarket market: price change")
	}

	c.state.UpdatePolymarket(func(s *state.PolymarketState) {
		s.LastWSUpdate = time.Now()
		s.IsStale = false
	})
}

func (c *Connector) handleTickSizeChange(event *TickSizeChangeEvent) {
	c.logger.Info().
		Str("asset_id", event.AssetID).
		Str("old_tick", event.OldTickSize).
		Str("new_tick", event.NewTickSize).
		Msg("polymarket market: tick size change")

	c.state.UpdatePolymarket(func(s *state.PolymarketState) {
		s.LastWSUpdate = time.Now()
	})
}

func (c *Connector) handleLastTradePrice(event *LastTradePriceEvent) {
	assetID := event.AssetID

	c.mu.Lock()
	c.health.Trades[assetID]++
	c.health.LastTrade[assetID] = time.Now()
	c.mu.Unlock()

	c.logger.Debug().
		Str("asset_id", assetID).
		Str("price", event.Price).
		Str("size", event.Size).
		Str("side", event.Side).
		Msg("polymarket market: trade")

	c.state.UpdatePolymarket(func(s *state.PolymarketState) {
		s.LastWSUpdate = time.Now()
		s.IsStale = false
	})
}

func (c *Connector) handleBestBidAsk(event *BestBidAskEvent) {
	assetID := event.AssetID

	c.logger.Debug().
		Str("asset_id", assetID).
		Str("best_bid", event.BestBid).
		Str("best_ask", event.BestAsk).
		Str("spread", event.Spread).
		Msg("polymarket market: best bid/ask")

	c.state.UpdatePolymarket(func(s *state.PolymarketState) {
		if s.OrderBooks[assetID] != nil {
			bestBid, _ := strconv.ParseFloat(event.BestBid, 64)
			bestAsk, _ := strconv.ParseFloat(event.BestAsk, 64)
			s.OrderBooks[assetID].BestBid = bestBid
			s.OrderBooks[assetID].BestAsk = bestAsk
			if bestBid > 0 && bestAsk > 0 {
				s.OrderBooks[assetID].MidPrice = (bestBid + bestAsk) / 2
			}
		}
		s.LastWSUpdate = time.Now()
	})
}

func (c *Connector) handleNewMarket(event *NewMarketEvent) {
	c.logger.Info().
		Str("market", event.Market).
		Str("question", event.Question).
		Strs("asset_ids", event.AssetsIDs).
		Msg("polymarket market: new market")

	c.state.UpdatePolymarket(func(s *state.PolymarketState) {
		for i, assetID := range event.AssetsIDs {
			if s.MarketAssets[assetID] == nil {
				outcome := ""
				if i < len(event.Outcomes) {
					outcome = event.Outcomes[i]
				}
				s.MarketAssets[assetID] = &state.MarketAssetMeta{
					AssetID:     assetID,
					ConditionID: event.Market,
					Question:    event.Question,
					Outcome:     outcome,
				}
			}
		}
		s.LastWSUpdate = time.Now()
	})
}

func (c *Connector) handleMarketResolved(event *MarketResolvedEvent) {
	c.logger.Info().
		Str("market", event.Market).
		Str("question", event.Question).
		Str("winning_asset_id", event.WinningAssetID).
		Str("winning_outcome", event.WinningOutcome).
		Msg("polymarket market: market resolved")

	c.state.UpdatePolymarket(func(s *state.PolymarketState) {
		for _, assetID := range event.AssetsIDs {
			if s.OrderBooks[assetID] != nil {
				delete(s.OrderBooks, assetID)
			}
			if s.MarketAssets[assetID] != nil {
				delete(s.MarketAssets, assetID)
			}
		}
		s.LastWSUpdate = time.Now()
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

	allStale := true
	for assetID := range c.health.LastBookUpdate {
		if time.Since(c.health.LastBookUpdate[assetID]) < 5*time.Second {
			allStale = false
			break
		}
	}

	if allStale && len(c.health.LastBookUpdate) > 0 {
		c.health.IsStale = true
		c.markStale()

		c.logger.Warn().Msg("polymarket market: feed marked as stale")
	}
}

func (c *Connector) markStale() {
	c.state.UpdatePolymarket(func(s *state.PolymarketState) {
		s.IsStale = true
	})

	c.state.UpdateRisk(func(s *state.RiskState) {
		s.PolymarketFresh = false
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

	c.logger.Info().Msg("polymarket market: stopping connector")

	if c.cancel != nil {
		c.cancel()
	}

	if c.wsClient != nil {
		c.wsClient.Stop()
	}

	c.wg.Wait()

	c.state.UpdatePolymarket(func(s *state.PolymarketState) {
		s.IsStale = true
	})

	c.state.UpdateRisk(func(s *state.RiskState) {
		s.PolymarketFresh = false
	})

	c.mu.Lock()
	c.running = false
	c.mu.Unlock()

	c.logger.Info().Msg("polymarket market: connector stopped")
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

func (c *Connector) IsRunning() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.running
}

func (c *Connector) Subscribe(assetID string) error {
	if err := c.wsClient.Subscribe(assetID); err != nil {
		return err
	}

	c.state.UpdatePolymarket(func(s *state.PolymarketState) {
		found := false
		for _, id := range s.SubscribedAssets {
			if id == assetID {
				found = true
				break
			}
		}
		if !found {
			s.SubscribedAssets = append(s.SubscribedAssets, assetID)
		}
	})

	return nil
}

func (c *Connector) Unsubscribe(assetID string) error {
	if err := c.wsClient.Unsubscribe(assetID); err != nil {
		return err
	}

	c.state.UpdatePolymarket(func(s *state.PolymarketState) {
		newList := make([]string, 0, len(s.SubscribedAssets))
		for _, id := range s.SubscribedAssets {
			if id != assetID {
				newList = append(newList, id)
			}
		}
		s.SubscribedAssets = newList

		delete(s.OrderBooks, assetID)
		delete(s.MarketAssets, assetID)
	})

	return nil
}

func (c *Connector) SubscribeMany(assetIDs []string) error {
	if err := c.wsClient.SubscribeMany(assetIDs); err != nil {
		return err
	}

	c.state.UpdatePolymarket(func(s *state.PolymarketState) {
		existingMap := make(map[string]bool)
		for _, id := range s.SubscribedAssets {
			existingMap[id] = true
		}

		for _, assetID := range assetIDs {
			if !existingMap[assetID] {
				s.SubscribedAssets = append(s.SubscribedAssets, assetID)
			}
		}
	})

	return nil
}

func (c *Connector) UnsubscribeMany(assetIDs []string) error {
	if err := c.wsClient.UnsubscribeMany(assetIDs); err != nil {
		return err
	}

	c.state.UpdatePolymarket(func(s *state.PolymarketState) {
		removeMap := make(map[string]bool)
		for _, id := range assetIDs {
			removeMap[id] = true
		}

		newList := make([]string, 0, len(s.SubscribedAssets))
		for _, id := range s.SubscribedAssets {
			if !removeMap[id] {
				newList = append(newList, id)
			}
		}
		s.SubscribedAssets = newList

		for _, assetID := range assetIDs {
			delete(s.OrderBooks, assetID)
			delete(s.MarketAssets, assetID)
		}
	})

	return nil
}

func (c *Connector) GetCurrentSubscriptions() []string {
	return c.wsClient.GetCurrentSubscriptions()
}

// Reconnect schedules a debounced WS reconnection so new subscriptions take effect.
func (c *Connector) Reconnect() {
	c.wsClient.ScheduleReconnect()
}
