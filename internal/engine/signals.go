package engine

import (
	"math"

	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/connectors/polymarket/tracker"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/state"
)

// ComputeSignals builds a MarketSignals snapshot for one active market window.
func ComputeSignals(
	w tracker.ActiveWindowSummary,
	clState state.ChainlinkState,
	spotState state.BinanceSpotState,
	perpsState state.BinancePerpsState,
) MarketSignals {
	clPrice := clState.LatestPrice
	startPrice := w.StartPrice
	gap := clPrice - startPrice

	// Winning side: YES = price >= start (market resolved "Up"), NO = price < start
	var winningSide string
	var pmConfidence float64
	if clPrice >= startPrice {
		winningSide = "YES"
		pmConfidence = w.YesAsk
	} else {
		winningSide = "NO"
		pmConfidence = w.NoAsk
	}

	binanceMid := spotState.OrderBook.MidPrice
	divergencePct := ComputeDivergencePct(binanceMid, clPrice)

	clBinanceAgree := false
	if clPrice > 0 && binanceMid > 0 && startPrice > 0 {
		clAbove := clPrice >= startPrice
		binanceAbove := binanceMid >= startPrice
		clBinanceAgree = clAbove == binanceAbove
	}

	liquidityToFlip := spotState.LiquidityToPrice(startPrice)
	bookImbalance := spotState.BookImbalanceNearPrice(startPrice, 0.1)
	expectedMove := EstimateExpectedMove(clState.Ticks, w.TimeRemainingS, clPrice)

	return MarketSignals{
		Slug:              w.Slug,
		Symbol:            w.Symbol,
		WinningSide:       winningSide,
		CLPrice:           clPrice,
		StartPrice:        startPrice,
		Gap:               gap,
		PMConfidence:      pmConfidence,
		BinanceMid:        binanceMid,
		DivergencePct:     divergencePct,
		CLBinanceAgree:    clBinanceAgree,
		LiquidityToFlip:   liquidityToFlip,
		BookImbalance:     bookImbalance,
		ExpectedMove:      expectedMove,
		LiquidationActive: perpsState.LiquidationFlag,
		TimeRemainingS:    w.TimeRemainingS,
	}
}

// EstimateExpectedMove returns the estimated BTC price move (notional) over timeSecs seconds,
// using the stddev of per-tick returns from the Chainlink ring buffer scaled by sqrt(timeSecs).
func EstimateExpectedMove(ticks state.RingBuffer, timeSecs int64, currentPrice float64) float64 {
	if currentPrice == 0 || timeSecs <= 0 {
		return 0
	}

	all := ticks.All()
	if len(all) < 2 {
		return 0
	}

	returns := make([]float64, 0, len(all)-1)
	for i := 1; i < len(all); i++ {
		prev := all[i-1].Price
		if prev > 0 {
			returns = append(returns, (all[i].Price-prev)/prev)
		}
	}
	if len(returns) == 0 {
		return 0
	}

	var sum float64
	for _, r := range returns {
		sum += r
	}
	mean := sum / float64(len(returns))

	var varSum float64
	for _, r := range returns {
		d := r - mean
		varSum += d * d
	}
	stddev := math.Sqrt(varSum / float64(len(returns)))

	return stddev * math.Sqrt(float64(timeSecs)) * currentPrice
}

// ComputeDivergencePct returns |binanceMid - clPrice| / clPrice * 100.
func ComputeDivergencePct(binanceMid, clPrice float64) float64 {
	if clPrice == 0 {
		return 0
	}
	return math.Abs(binanceMid-clPrice) / clPrice * 100
}
