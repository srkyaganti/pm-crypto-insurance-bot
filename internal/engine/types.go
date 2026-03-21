package engine

import "time"

// confEntry records a single PM confidence observation for sustained-conf tracking.
type confEntry struct {
	Timestamp time.Time
	Conf      float64
}

// MarketSignals holds all computed signals for a single active market window.
type MarketSignals struct {
	Slug              string  `json:"slug"`
	Symbol            string  `json:"symbol"`
	WinningSide       string  `json:"winning_side"`
	CLPrice           float64 `json:"cl_price"`
	StartPrice        float64 `json:"start_price"`
	Gap               float64 `json:"gap"`
	PMConfidence      float64 `json:"pm_confidence"`
	SustainedPMConf   float64 `json:"sustained_pm_conf"`
	BinanceMid        float64 `json:"binance_mid"`
	DivergencePct     float64 `json:"divergence_pct"`
	CLBinanceAgree    bool    `json:"cl_binance_agree"`
	LiquidityToFlip   float64 `json:"liquidity_to_flip"`
	BookImbalance     float64 `json:"book_imbalance"`
	ExpectedMove      float64 `json:"expected_move"`
	GapTrend          float64 `json:"gap_trend"`
	FlowMomentum      float64 `json:"flow_momentum"`
	FlowIntensity30s        float64 `json:"flow_intensity_30s"`
	VolumePerSec            float64 `json:"volume_per_sec"`
	LiquidityToFlipUSD      float64 `json:"liquidity_to_flip_usd"`
	ExpectedVolumeRemaining float64 `json:"expected_volume_remaining"`
	DepthRatio              float64 `json:"depth_ratio"`
	LiquidationActive bool    `json:"liquidation_active"`
	TimeRemainingS    int64   `json:"time_remaining_s"`
}

// TradeDecision is the output of the gate evaluation for one market.
type TradeDecision struct {
	ShouldTrade  bool          `json:"should_trade"`
	Side         string        `json:"side"`
	RejectReason string        `json:"reject_reason"`
	Signals      MarketSignals `json:"signals"`
}
