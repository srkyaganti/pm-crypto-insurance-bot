package engine

// MarketSignals holds all computed signals for a single active market window.
type MarketSignals struct {
	Slug              string
	Symbol            string
	WinningSide       string  // "YES" (price >= start) or "NO" (price < start)
	CLPrice           float64
	StartPrice        float64
	Gap               float64  // CLPrice - StartPrice
	PMConfidence      float64  // winning side's best bid on Polymarket
	BinanceMid        float64
	DivergencePct     float64  // |binance_mid - cl_price| / cl_price * 100
	CLBinanceAgree    bool     // both CL and Binance mid on same side of start_price
	LiquidityToFlip   float64  // BTC notional in order book between mid and start_price
	ExpectedMove      float64  // stddev-based expected BTC move (notional) over time_remaining
	LiquidationActive bool     // perps liquidation flag
	TimeRemainingS    int64
}

// TradeDecision is the output of the gate evaluation for one market.
type TradeDecision struct {
	ShouldTrade  bool
	Side         string // "YES" or "NO"
	RejectReason string // which gate failed; empty if ShouldTrade is true
	Signals      MarketSignals
}
