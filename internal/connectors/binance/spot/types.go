package spot

import (
	"encoding/json"
	"time"
)

type Config struct {
	Symbol         string
	WsURL          string
	RestURL        string
	DepthSpeed     string
	MaxDepthLevels int
	StaleThreshold time.Duration
}

type PartialDepthEvent struct {
	EventType string     `json:"e"`
	EventTime int64      `json:"E"`
	Symbol    string     `json:"s"`
	Bids      [][]string `json:"b"`
	Asks      [][]string `json:"a"`
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
	Symbol    string
	EventTime int64
	Bids      []PriceLevel
	Asks      []PriceLevel
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
