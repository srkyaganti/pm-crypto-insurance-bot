package engine

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/rs/zerolog"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/config"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/connectors/polymarket/tracker"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/execution"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/persistence"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/state"
)

// Engine evaluates active Polymarket markets on a fixed tick and executes trades.
type Engine struct {
	state       *state.StateStore
	tracker     *tracker.Connector
	cfg         *config.StrategyConfig
	execClient  *execution.Client
	balance     *execution.BalanceTracker
	persistence *persistence.Writer
	logger      zerolog.Logger
}

func NewEngine(
	store *state.StateStore,
	trackerConn *tracker.Connector,
	cfg *config.StrategyConfig,
	execClient *execution.Client,
	balance *execution.BalanceTracker,
	pw *persistence.Writer,
	baseLogger *zerolog.Logger,
) *Engine {
	logger := baseLogger.With().Str("component", "engine").Logger()
	return &Engine{
		state:       store,
		tracker:     trackerConn,
		cfg:         cfg,
		execClient:  execClient,
		balance:     balance,
		persistence: pw,
		logger:      logger,
	}
}

// symbolFloat looks up a per-symbol value from a map. Returns 0 if symbol not found.
func (e *Engine) symbolFloat(m map[string]float64, symbol string) float64 {
	if v, ok := m[symbol]; ok {
		return v
	}
	return 0
}

// Run starts the 500ms evaluation loop and blocks until ctx is cancelled.
func (e *Engine) Run(ctx context.Context) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	e.logger.Info().Msg("engine: paper trading loop started")

	for {
		select {
		case <-ctx.Done():
			e.logger.Info().Msg("engine: stopped")
			return nil
		case <-ticker.C:
			e.tick()
		}
	}
}

func (e *Engine) tick() {
	if e.tracker == nil {
		return
	}
	for _, w := range e.tracker.GetActiveWindowSummaries() {
		e.evaluateMarket(w)
	}
}

func (e *Engine) evaluateMarket(w tracker.ActiveWindowSummary) {
	clState := e.state.GetChainlink(w.Symbol)
	spotState := e.state.GetBinanceSpot(w.Symbol)
	perpsState := e.state.GetBinancePerps(w.Symbol)

	signals := ComputeSignals(w, clState, spotState, perpsState)
	decision := e.runGates(w, signals)

	logEvent := e.logger.Info().
		Str("slug", w.Slug).
		Str("symbol", w.Symbol).
		Int64("time_remaining_s", signals.TimeRemainingS).
		Str("winning_side", signals.WinningSide).
		Float64("cl_price", signals.CLPrice).
		Float64("start_price", signals.StartPrice).
		Float64("gap", signals.Gap).
		Float64("pm_confidence", signals.PMConfidence).
		Float64("divergence_pct", signals.DivergencePct).
		Bool("cl_binance_agree", signals.CLBinanceAgree).
		Float64("liquidity_to_flip", signals.LiquidityToFlip).
		Float64("book_imbalance", signals.BookImbalance).
		Float64("expected_move", signals.ExpectedMove).
		Bool("liquidation_active", signals.LiquidationActive)

	if decision.ShouldTrade {
		if e.cfg.Mode == "live" && e.execClient != nil {
			logEvent.Msg("engine: LIVE_TRADE")
			e.executeLiveTrade(w, decision)
		} else {
			logEvent.Msg("engine: PAPER_TRADE")
			e.recordPaperTrade(w, decision)
		}
	} else {
		logEvent.Str("reject_reason", decision.RejectReason).Msg("engine: SKIP")
	}
}

// runGates evaluates the 6 entry gates in order and returns a TradeDecision.
func (e *Engine) runGates(w tracker.ActiveWindowSummary, sig MarketSignals) TradeDecision {
	// Gate 1: Time window — entry only in the final 60-120 seconds
	if sig.TimeRemainingS < int64(e.cfg.EntryWindowMinSecs) || sig.TimeRemainingS > int64(e.cfg.EntryWindowMaxSecs) {
		return TradeDecision{RejectReason: "gate1_time_window", Signals: sig}
	}

	// Gate 2: No existing position in this market
	if w.HasPosition {
		return TradeDecision{RejectReason: "gate2_existing_position", Signals: sig}
	}

	// Gate 3: Polymarket is already pricing the winning side at high confidence
	if sig.PMConfidence < e.symbolFloat(e.cfg.MinPMConfidence, sig.Symbol) {
		return TradeDecision{RejectReason: "gate3_pm_confidence", Signals: sig}
	}

	// Gate 4: CL and Binance agree on which side of start_price, and divergence is small
	if !sig.CLBinanceAgree || sig.DivergencePct > e.symbolFloat(e.cfg.MaxCLBinanceDivergencePct, sig.Symbol) {
		return TradeDecision{RejectReason: "gate4_cl_binance_agreement", Signals: sig}
	}

	// Gate 5: Price gap must exceed expected move, and order book must have minimum depth.
	// Block if ring buffer hasn't filled yet (no reliable stddev estimate).
	if sig.ExpectedMove == 0 {
		return TradeDecision{RejectReason: "gate5_expected_move_unavailable", Signals: sig}
	}
	gapAbs := sig.Gap
	if gapAbs < 0 {
		gapAbs = -gapAbs
	}
	if gapAbs < e.symbolFloat(e.cfg.DepthSafetyMultiplier, sig.Symbol)*sig.ExpectedMove {
		return TradeDecision{RejectReason: "gate5_gap_too_small", Signals: sig}
	}
	if sig.LiquidityToFlip < e.symbolFloat(e.cfg.MinDepth, sig.Symbol) {
		return TradeDecision{RejectReason: "gate5_depth_too_thin", Signals: sig}
	}

	// Gate 6: No active liquidation cascade on perps
	if sig.LiquidationActive {
		return TradeDecision{RejectReason: "gate6_liquidation_cascade", Signals: sig}
	}

	return TradeDecision{
		ShouldTrade: true,
		Side:        sig.WinningSide,
		Signals:     sig,
	}
}

func (e *Engine) executeLiveTrade(w tracker.ActiveWindowSummary, decision TradeDecision) {
	sig := decision.Signals
	tradeSize := e.cfg.TradeSizeUSD

	if !e.balance.Debit(tradeSize) {
		e.logger.Warn().
			Str("slug", w.Slug).
			Float64("trade_size", tradeSize).
			Msg("engine: insufficient balance for live trade")
		return
	}

	requestID := fmt.Sprintf("trade-%s-%d", w.Slug, time.Now().UnixMilli())

	assetID := w.NoAssetID
	if decision.Side == "YES" {
		assetID = w.YesAssetID
	}

	priceCeiling := sig.PMConfidence // use actual ask price from the PM book

	orderReq := execution.MarketOrderRequest{
		TokenID:   assetID,
		Side:      "BUY",
		Amount:    tradeSize,
		TickSize:  "0.01",
		NegRisk:   false,
		OrderType: "FOK",
		Price:     priceCeiling,
	}

	resp, err := e.execClient.PlaceMarketOrder(context.Background(), requestID, orderReq)
	if err != nil {
		e.balance.Credit(tradeSize)
		e.logger.Error().Err(err).Str("slug", w.Slug).Str("request_id", requestID).Msg("engine: live order request failed")
		return
	}

	if !resp.Success {
		e.balance.Credit(tradeSize)
		e.logger.Warn().
			Str("slug", w.Slug).
			Str("request_id", requestID).
			Str("error", resp.ErrorMsg).
			Msg("engine: live order rejected")
		return
	}

	shares, _ := strconv.ParseFloat(resp.TakingAmount, 64)
	cost, _ := strconv.ParseFloat(resp.MakingAmount, 64)

	if err := e.tracker.SetLivePosition(w.Slug, assetID, decision.Side, shares, cost, resp.OrderID, requestID); err != nil {
		e.logger.Error().Err(err).Str("slug", w.Slug).Msg("engine: failed to record live position")
	}

	e.logger.Info().
		Str("slug", w.Slug).
		Str("request_id", requestID).
		Str("order_id", resp.OrderID).
		Str("side", decision.Side).
		Float64("shares", shares).
		Float64("cost", cost).
		Float64("price_ceiling", priceCeiling).
		Msg("engine: live trade filled")

	if e.persistence != nil {
		event := persistence.NewEvent("engine", persistence.EventExecution, "live_trade")
		event.WithPayloads(map[string]interface{}{
			"slug":          w.Slug,
			"symbol":        sig.Symbol,
			"side":          decision.Side,
			"asset_id":      assetID,
			"request_id":    requestID,
			"order_id":      resp.OrderID,
			"shares":        shares,
			"cost":          cost,
			"price_ceiling": priceCeiling,
			"trade_size":    tradeSize,
		})
		e.persistence.Write(event)
	}
}

func (e *Engine) recordPaperTrade(w tracker.ActiveWindowSummary, decision TradeDecision) {
	sig := decision.Signals

	assetID := w.NoAssetID
	if decision.Side == "YES" {
		assetID = w.YesAssetID
	}

	size := float64(e.cfg.MaxSharesPerTrade)
	if err := e.tracker.ExecutePaperTrade(w.Slug, assetID, decision.Side, "all_gates_passed", size); err != nil {
		e.logger.Error().Err(err).Str("slug", w.Slug).Msg("engine: failed to record paper trade")
		return
	}

	if e.persistence == nil {
		return
	}

	event := persistence.NewEvent("engine", persistence.EventDecision, "paper_trade")
	event.WithPayloads(map[string]interface{}{
		"slug":              w.Slug,
		"symbol":            sig.Symbol,
		"side":              decision.Side,
		"asset_id":          assetID,
		"size":              size,
		"cl_price":          sig.CLPrice,
		"start_price":       sig.StartPrice,
		"gap":               sig.Gap,
		"pm_confidence":     sig.PMConfidence,
		"binance_mid":       sig.BinanceMid,
		"divergence_pct":    sig.DivergencePct,
		"liquidity_to_flip": sig.LiquidityToFlip,
		"expected_move":     sig.ExpectedMove,
		"time_remaining_s":  sig.TimeRemainingS,
	})
	e.persistence.Write(event)
}
