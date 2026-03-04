package state

import (
	"sync"
	"time"
)

type AppState struct {
	Meta         MetaState
	Chainlink    ChainlinkState
	BinanceSpot  BinanceSpotState
	BinancePerps BinancePerpsState
	Polymarket   PolymarketState
	Risk         RiskState
}

type MetaState struct {
	StartedAt        time.Time
	LastDecisionTime time.Time
	UptimeSeconds    int64
	IsHealthy        bool
}

type PriceTick struct {
	Timestamp time.Time
	Price     float64
}

type RingBuffer struct {
	Buffer   []PriceTick
	Capacity int
	Index    int
	Full     bool
}

type ChainlinkState struct {
	LatestPrice float64
	LastUpdate  time.Time
	IsStale     bool
	Ticks       RingBuffer
}

type OrderLevel struct {
	Price float64
	Size  float64
}

type OrderBookSnapshot struct {
	Bids            []OrderLevel
	Asks            []OrderLevel
	BestBid         float64
	BestAsk         float64
	MidPrice        float64
	CumBidLiquidity float64
	CumAskLiquidity float64
	LastUpdate      time.Time
	UpdateCount     int64
}

type FlowBucket struct {
	Timestamp    int64
	BuyNotional  float64
	SellNotional float64
	TradeCount   int64
}

type FlowWindow struct {
	BuyNotional  float64
	SellNotional float64
	TradeCount   int64
	Imbalance    float64
}

type TradeFlowState struct {
	Buckets           [300]FlowBucket
	BucketIdx         int
	LastRotate        time.Time
	Window10s         FlowWindow
	Window30s         FlowWindow
	TotalTrades       int64
	TotalBuyNotional  float64
	TotalSellNotional float64
	LastTrade         time.Time
}

type BinanceSpotState struct {
	Symbol          string
	OrderBook       OrderBookSnapshot
	TradeFlow       TradeFlowState
	WallPersistence int
	LastDepthUpdate time.Time
	LastAggTrade    time.Time
	IsStale         bool
}

type LiquidationEvent struct {
	Timestamp time.Time
	Side      string
	Price     float64
	Quantity  float64
}

type BinancePerpsState struct {
	Symbol             string
	LastMarkPrice      float64
	LastMarkUpdate     time.Time
	RecentLiquidations []LiquidationEvent
	LiquidationFlag    bool
	IsStale            bool
}

type PMOrderLevel struct {
	Price float64
	Size  float64
}

type PMOrderBook struct {
	Bids       []PMOrderLevel
	Asks       []PMOrderLevel
	BestBid    float64
	BestAsk    float64
	MidPrice   float64
	LastUpdate time.Time
}

type MarketMeta struct {
	ConditionID string
	YesTokenID  string
	NoTokenID   string
	ExpiryTime  time.Time
}

type OrderStatus struct {
	OrderID      string
	Status       string
	FilledSize   float64
	AvgFillPrice float64
	LastUpdate   time.Time
}

type PolymarketState struct {
	Market       MarketMeta
	OrderBook    PMOrderBook
	LastWSUpdate time.Time
	ActiveOrder  *OrderStatus
	IsStale      bool
}

type RiskState struct {
	FeedsHealthy    bool
	ChainlinkFresh  bool
	BinanceFresh    bool
	PolymarketFresh bool
	MaxMove30sPct   float64
	MaxMove60sPct   float64
	DailyLossCount  int
	TradingDisabled bool
	LastRiskUpdate  time.Time
}

type StateVersion struct {
	SchemaVersion string
}

const CurrentSchemaVersion = "v1.0.0"

type StateStore struct {
	mu    sync.RWMutex
	state AppState
}

func NewStateStore() *StateStore {
	now := time.Now()
	return &StateStore{
		state: AppState{
			Meta: MetaState{
				StartedAt: now,
				IsHealthy: true,
			},
			Chainlink: ChainlinkState{
				Ticks: RingBuffer{
					Buffer:   make([]PriceTick, 0, 180),
					Capacity: 180,
					Index:    0,
					Full:     false,
				},
			},
			BinanceSpot: BinanceSpotState{
				OrderBook: OrderBookSnapshot{
					Bids: make([]OrderLevel, 0, 20),
					Asks: make([]OrderLevel, 0, 20),
				},
				TradeFlow: TradeFlowState{
					LastRotate: time.Now(),
				},
			},
			BinancePerps: BinancePerpsState{
				RecentLiquidations: make([]LiquidationEvent, 0, 100),
			},
			Polymarket: PolymarketState{
				OrderBook: PMOrderBook{
					Bids: make([]PMOrderLevel, 0, 20),
					Asks: make([]PMOrderLevel, 0, 20),
				},
			},
			Risk: RiskState{
				FeedsHealthy:    false,
				ChainlinkFresh:  false,
				BinanceFresh:    false,
				PolymarketFresh: false,
				TradingDisabled: true,
				LastRiskUpdate:  now,
			},
		},
	}
}

func (s *StateStore) Snapshot() AppState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state
}

func (s *StateStore) GetMeta() MetaState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state.Meta
}

func (s *StateStore) GetChainlink() ChainlinkState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state.Chainlink
}

func (s *StateStore) GetBinanceSpot() BinanceSpotState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state.BinanceSpot
}

func (s *StateStore) GetBinancePerps() BinancePerpsState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state.BinancePerps
}

func (s *StateStore) GetPolymarket() PolymarketState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state.Polymarket
}

func (s *StateStore) GetRisk() RiskState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state.Risk
}

func (s *StateStore) UpdateMeta(fn func(*MetaState)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(&s.state.Meta)
}

func (s *StateStore) UpdateChainlink(fn func(*ChainlinkState)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(&s.state.Chainlink)
}

func (s *StateStore) UpdateBinanceSpot(fn func(*BinanceSpotState)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(&s.state.BinanceSpot)
}

func (s *StateStore) UpdateBinancePerps(fn func(*BinancePerpsState)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(&s.state.BinancePerps)
}

func (s *StateStore) UpdatePolymarket(fn func(*PolymarketState)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(&s.state.Polymarket)
}

func (s *StateStore) UpdateRisk(fn func(*RiskState)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(&s.state.Risk)
}
