package chainlink

import (
	"time"
)

type Config struct {
	ApiKey         string
	ApiSecret      string
	RestURL        string
	WsURL          string
	FeedIDs        map[string]string // symbol -> feedID hex
	FeedIDToSymbol map[string]string // feedID hex -> symbol (reverse lookup)
	WsHA           bool
	WsMaxReconnect int
	StaleThreshold time.Duration
}

type HealthStatus struct {
	IsConnected  bool
	IsStale      bool
	LastUpdate   time.Time
	ReportsCount int64
	Errors       int64
	PerSymbol    map[string]*SymbolHealth
}

type SymbolHealth struct {
	LastUpdate   time.Time
	ReportsCount int64
	IsStale      bool
}

type DecodedReport struct {
	FeedID                string
	BenchmarkPrice        float64
	ObservationsTimestamp uint32
	ValidFromTimestamp    uint32
}
