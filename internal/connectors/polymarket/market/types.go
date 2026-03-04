package market

import "time"

type PriceLevel struct {
	Price string `json:"price"`
	Size  string `json:"size"`
}

type BookEvent struct {
	EventType string       `json:"event_type"`
	AssetID   string       `json:"asset_id"`
	Market    string       `json:"market"`
	Bids      []PriceLevel `json:"bids"`
	Asks      []PriceLevel `json:"asks"`
	Timestamp string       `json:"timestamp"`
	Hash      string       `json:"hash"`
}

type PriceChange struct {
	AssetID string `json:"asset_id"`
	Price   string `json:"price"`
	Size    string `json:"size"`
	Side    string `json:"side"`
	Hash    string `json:"hash"`
	BestBid string `json:"best_bid"`
	BestAsk string `json:"best_ask"`
}

type PriceChangeEvent struct {
	EventType    string        `json:"event_type"`
	Market       string        `json:"market"`
	PriceChanges []PriceChange `json:"price_changes"`
	Timestamp    string        `json:"timestamp"`
}

type TickSizeChangeEvent struct {
	EventType   string `json:"event_type"`
	AssetID     string `json:"asset_id"`
	Market      string `json:"market"`
	OldTickSize string `json:"old_tick_size"`
	NewTickSize string `json:"new_tick_size"`
	Timestamp   string `json:"timestamp"`
}

type LastTradePriceEvent struct {
	EventType  string `json:"event_type"`
	AssetID    string `json:"asset_id"`
	Market     string `json:"market"`
	Price      string `json:"price"`
	Side       string `json:"side"`
	Size       string `json:"size"`
	FeeRateBps string `json:"fee_rate_bps"`
	Timestamp  string `json:"timestamp"`
}

type BestBidAskEvent struct {
	EventType string `json:"event_type"`
	Market    string `json:"market"`
	AssetID   string `json:"asset_id"`
	BestBid   string `json:"best_bid"`
	BestAsk   string `json:"best_ask"`
	Spread    string `json:"spread"`
	Timestamp string `json:"timestamp"`
}

type EventMessage struct {
	ID          string `json:"id"`
	Ticker      string `json:"ticker"`
	Slug        string `json:"slug"`
	Title       string `json:"title"`
	Description string `json:"description"`
}

type NewMarketEvent struct {
	EventType    string        `json:"event_type"`
	ID           string        `json:"id"`
	Question     string        `json:"question"`
	Market       string        `json:"market"`
	Slug         string        `json:"slug"`
	Description  string        `json:"description"`
	AssetsIDs    []string      `json:"assets_ids"`
	Outcomes     []string      `json:"outcomes"`
	EventMessage *EventMessage `json:"event_message"`
	Timestamp    string        `json:"timestamp"`
}

type MarketResolvedEvent struct {
	EventType      string        `json:"event_type"`
	ID             string        `json:"id"`
	Question       string        `json:"question"`
	Market         string        `json:"market"`
	Slug           string        `json:"slug"`
	Description    string        `json:"description"`
	AssetsIDs      []string      `json:"assets_ids"`
	Outcomes       []string      `json:"outcomes"`
	WinningAssetID string        `json:"winning_asset_id"`
	WinningOutcome string        `json:"winning_outcome"`
	EventMessage   *EventMessage `json:"event_message"`
	Timestamp      string        `json:"timestamp"`
}

type Config struct {
	WsURL                string
	CustomFeatureEnabled bool
	ReconnectMinWait     time.Duration
	ReconnectMaxWait     time.Duration
	PingPeriod           time.Duration
	PongWait             time.Duration
	WriteWait            time.Duration
	InitialAssetIDs      []string
}

const (
	DefaultWsURL                = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
	DefaultReconnectMinWait     = 1 * time.Second
	DefaultReconnectMaxWait     = 30 * time.Second
	DefaultPingPeriod           = 20 * time.Second
	DefaultPongWait             = 30 * time.Second
	DefaultWriteWait            = 10 * time.Second
	DefaultCustomFeatureEnabled = true
)
