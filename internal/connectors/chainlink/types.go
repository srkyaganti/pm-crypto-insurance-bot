package chainlink

import (
	"time"
)

type Config struct {
	ApiKey         string
	ApiSecret      string
	RestURL        string
	WsURL          string
	FeedID         string
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
}

type DecodedReport struct {
	FeedID                string
	BenchmarkPrice        float64
	ObservationsTimestamp uint32
	ValidFromTimestamp    uint32
}
