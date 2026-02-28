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

type DepthUpdate struct {
	EventType     string     `json:"e"`
	EventTime     int64      `json:"E"`
	Symbol        string     `json:"s"`
	FirstUpdateID int64      `json:"U"`
	FinalUpdateID int64      `json:"u"`
	Bids          [][]string `json:"b"`
	Asks          [][]string `json:"a"`
}

type BookTicker struct {
	UpdateID int64  `json:"u"`
	Symbol   string `json:"s"`
	BidPrice string `json:"b"`
	BidQty   string `json:"B"`
	AskPrice string `json:"a"`
	AskQty   string `json:"A"`
}

type DepthSnapshot struct {
	LastUpdateID int64      `json:"lastUpdateId"`
	Bids         [][]string `json:"bids"`
	Asks         [][]string `json:"asks"`
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
	Symbol        string
	FirstUpdateID int64
	FinalUpdateID int64
	EventTime     int64
	Bids          []PriceLevel
	Asks          []PriceLevel
}

type BookTickerData struct {
	Symbol   string
	UpdateID int64
	BidPrice float64
	BidQty   float64
	AskPrice float64
	AskQty   float64
}

type HealthStatus struct {
	IsConnected       bool
	IsStale           bool
	LastDepthUpdate   time.Time
	LastBookTicker    time.Time
	SnapshotSynced    bool
	Reconnects        int
	Errors            int64
	DepthUpdates      int64
	BookTickerUpdates int64
}
