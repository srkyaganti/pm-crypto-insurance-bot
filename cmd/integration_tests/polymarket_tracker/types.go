package main

import (
	"time"
)

type ActiveMarket struct {
	Slug        string
	MarketData  interface{}
	YesAssetID  string
	NoAssetID   string
	ConditionID string
	WindowType  int

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

type Config struct {
	GammaURL           string
	MarketWsURL        string
	ChainlinkRestURL   string
	ChainlinkWsURL     string
	ChainlinkApiKey    string
	ChainlinkApiSecret string
	BTCFeedID          string

	MarketPrefixes     []string
	Windows            []int
	PreScheduleWindows int
	DataDir            string
	LogInterval        time.Duration
	LogLevel           string
	CustomFeatures     bool
}
