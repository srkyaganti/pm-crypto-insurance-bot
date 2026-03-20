package engine

// MarketSignals holds all computed signals for a single active market window.
type MarketSignals struct {
	Slug              string  `json:"slug"`
	Symbol            string  `json:"symbol"`
	WinningSide       string  `json:"winning_side"`
	CLPrice           float64 `json:"cl_price"`
	StartPrice        float64 `json:"start_price"`
	Gap               float64 `json:"gap"`
	PMConfidence      float64 `json:"pm_confidence"`
	BinanceMid        float64 `json:"binance_mid"`
	DivergencePct     float64 `json:"divergence_pct"`
	CLBinanceAgree    bool    `json:"cl_binance_agree"`
	LiquidityToFlip   float64 `json:"liquidity_to_flip"`
	ExpectedMove      float64 `json:"expected_move"`
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
