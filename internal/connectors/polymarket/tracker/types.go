package tracker

import (
	"time"
)

type ActiveMarket struct {
	Slug        string
	YesAssetID  string
	NoAssetID   string
	ConditionID string
	WindowType  int
	FeedID      string
	Symbol      string
	Prefix      string

	StartTime    time.Time
	EndTime      time.Time
	SubscribedAt time.Time

	StartPrice float64
	EndPrice   float64

	PaperTrades []PaperTrade
	MyPosition  *Position

	EndTimer *time.Timer

	LastUpdate time.Time
}

type PaperTrade struct {
	Timestamp time.Time
	AssetID   string
	Side      string
	Price     float64
	Size      float64
	Reason    string
}

type Position struct {
	AssetID    string
	Side       string
	EntryPrice float64
	Size       float64
	OpenedAt   time.Time
}

type CompletedMarket struct {
	Slug        string
	WindowType  int
	FeedID      string
	Symbol      string
	Prefix      string
	StartTime   time.Time
	EndTime     time.Time
	YesAssetID  string
	NoAssetID   string
	ConditionID string

	StartPrice     float64
	EndPrice       float64
	PriceChange    float64
	PriceChangePct float64

	WinningOutcome string
	MarketResult   string

	PaperTrades []PaperTrade
	Position    *Position
	PnL         float64
	DidWeTrade  bool

	YesFinalPrice float64
	NoFinalPrice  float64
	Volume        float64
	Timestamp     time.Time
}

type TrackerConfig struct {
	PreScheduleWindows int
	CustomFeatures     bool
}

type ActiveWindowSummary struct {
	Slug           string
	Symbol         string
	WindowMin      int
	StartPrice     float64
	EndTime        time.Time
	TimeRemainingS int64
	YesMid, YesBid, YesAsk float64
	NoMid, NoBid, NoAsk    float64
}
