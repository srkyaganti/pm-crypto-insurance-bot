package perps

import (
	"encoding/json"
	"time"
)

type Config struct {
	Symbols         []string
	SymbolToKey     map[string]string // uppercase binance symbol -> canonical (e.g. "BTCUSDT" -> "BTC")
	WsURL           string
	StaleThreshold  time.Duration
	MaxLiquidations int
}

type HealthStatus struct {
	IsConnected       bool
	IsStale           bool
	LastMarkUpdate    time.Time
	LastLiquidation   time.Time
	Reconnects        int
	Errors            int64
	MarkUpdates       int64
	LiquidationEvents int64
}

type MarkPriceUpdate struct {
	EventType            string `json:"e"`
	EventTime            int64  `json:"E"`
	Symbol               string `json:"s"`
	MarkPrice            string `json:"p"`
	IndexPrice           string `json:"i"`
	EstimatedSettlePrice string `json:"P"`
	FundingRate          string `json:"r"`
	NextFundingTime      int64  `json:"T"`
}

type ForceOrderEvent struct {
	EventType string     `json:"e"`
	EventTime int64      `json:"E"`
	Order     ForceOrder `json:"o"`
}

type ForceOrder struct {
	Symbol                    string `json:"s"`
	Side                      string `json:"S"`
	OrderType                 string `json:"o"`
	TimeInForce               string `json:"f"`
	OriginalQuantity          string `json:"q"`
	Price                     string `json:"p"`
	AveragePrice              string `json:"ap"`
	OrderStatus               string `json:"X"`
	LastFilledQuantity        string `json:"l"`
	FilledAccumulatedQuantity string `json:"z"`
	TradeTime                 int64  `json:"T"`
}

type CombinedStreamMessage struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

type MarkPriceData struct {
	Symbol     string
	MarkPrice  float64
	IndexPrice float64
	EventTime  int64
}

type LiquidationData struct {
	Symbol    string
	Side      string
	Price     float64
	Quantity  float64
	EventTime int64
}
