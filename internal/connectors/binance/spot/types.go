package spot

import (
	"encoding/json"
	"time"
)

type Config struct {
	Symbols        []string
	SymbolToKey    map[string]string // lowercase binance symbol -> canonical (e.g. "btcusdt" -> "BTC")
	WsURL          string
	RestURL        string
	DepthSpeed     string
	MaxDepthLevels int
	StaleThreshold time.Duration
}

type PartialDepthEvent struct {
	LastUpdateID int64      `json:"lastUpdateId"`
	Bids         [][]string `json:"bids"`
	Asks         [][]string `json:"asks"`
}

type AggTradeEvent struct {
	EventType    string `json:"e"`
	EventTime    int64  `json:"E"`
	Symbol       string `json:"s"`
	TradeID      int64  `json:"a"`
	Price        string `json:"p"`
	Quantity     string `json:"q"`
	TradeTime    int64  `json:"T"`
	IsBuyerMaker bool   `json:"m"`
	Ignore       bool   `json:"M"` // prevents "M" from case-insensitively overwriting "m"
}

type AggTradeData struct {
	Symbol    string
	TradeID   int64
	Price     float64
	Quantity  float64
	Notional  float64
	IsBuy     bool
	Timestamp time.Time
}

type CombinedStreamMessage struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

type PriceLevel struct {
	Price float64
	Size  float64
}

type DepthData struct {
	LastUpdateID int64
	Bids         []PriceLevel
	Asks         []PriceLevel
}

type HealthStatus struct {
	IsConnected     bool
	IsStale         bool
	LastDepthUpdate time.Time
	LastAggTrade    time.Time
	Reconnects      int
	Errors          int64
	DepthUpdates    int64
	AggTradeUpdates int64
}
