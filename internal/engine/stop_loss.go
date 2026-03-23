package engine

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/connectors/polymarket/tracker"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/execution"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/persistence"
)

type stopLossState struct {
	BelowStopTicks  int
	EmptyBookSince  time.Time
	SellPending     bool
	RemainingShares float64
	TotalProceeds   float64
	TotalSharesSold float64
	SellAttempts    int
	OriginalCost    float64
	OriginalSize    float64
	EntryPrice      float64
}

func (e *Engine) monitorStopLoss(w tracker.ActiveWindowSummary) {
	sl := &e.cfg.StopLoss
	if !sl.Enabled {
		return
	}

	// If position was fully cleared, nothing to monitor
	if !w.HasPosition && e.stopLossStates[w.Slug] == nil {
		return
	}

	// Route paper vs live
	if e.cfg.Mode != "live" || e.execClient == nil {
		e.monitorPaperStopLoss(w)
		return
	}

	// Initialize state on first encounter
	slState, exists := e.stopLossStates[w.Slug]
	if !exists {
		if !w.HasPosition {
			return
		}
		slState = &stopLossState{
			RemainingShares: w.PositionSize,
			OriginalCost:    w.PositionSize * w.PositionEntryPrice,
			OriginalSize:    w.PositionSize,
			EntryPrice:      w.PositionEntryPrice,
		}
		e.stopLossStates[w.Slug] = slState
	}

	if slState.SellPending || slState.RemainingShares <= 0.001 {
		return
	}

	bid, spread := getBidAndSpread(w)
	stopPrice := e.symbolFloat(sl.StopPrice, w.Symbol)
	if stopPrice == 0 {
		return
	}

	// Detect empty book
	isEmpty := spread > sl.EmptyBookSpreadThresh || bid <= 0.01

	if isEmpty {
		if slState.EmptyBookSince.IsZero() {
			slState.EmptyBookSince = time.Now()
		}
		return
	}

	// Book returned after being empty
	if !slState.EmptyBookSince.IsZero() {
		emptyDuration := time.Since(slState.EmptyBookSince)
		slState.EmptyBookSince = time.Time{} // reset

		// Trigger 1: Empty book recovery — sell only if book was empty long enough AND bid is below stop
		if emptyDuration >= time.Duration(sl.EmptyBookTimeoutSecs)*time.Second {
			if bid < stopPrice && bid > sl.MinSellPrice {
				if w.TimeRemainingS >= int64(sl.MinTimeRemainingSecs) {
					e.executeStopLossSell(w, slState, bid, "empty_book_recovery")
					return
				}
			}
		}
	}

	// Guard: don't sell in final seconds
	if w.TimeRemainingS < int64(sl.MinTimeRemainingSecs) {
		return
	}

	// Trigger 2: Bid below stop price with confirmation
	if bid < stopPrice && bid > sl.MinSellPrice {
		slState.BelowStopTicks++
		if slState.BelowStopTicks >= sl.ConfirmTicks {
			e.executeStopLossSell(w, slState, bid, "bid_below_stop")
		}
	} else {
		slState.BelowStopTicks = 0
	}
}

func (e *Engine) executeStopLossSell(w tracker.ActiveWindowSummary, slState *stopLossState, bidPrice float64, reason string) {
	slState.SellPending = true
	defer func() { slState.SellPending = false }()
	slState.SellAttempts++

	requestID := fmt.Sprintf("stop-loss-%s-%d", w.Slug, time.Now().UnixMilli())

	orderReq := execution.MarketOrderRequest{
		TokenID:   w.PositionAssetID,
		Side:      "SELL",
		Amount:    slState.RemainingShares,
		TickSize:  "0.01",
		OrderType: "FAK",
		// Price omitted (zero value + omitempty) — FAK SELL sweeps available bids
	}

	e.logger.Info().
		Str("slug", w.Slug).
		Str("reason", reason).
		Float64("bid", bidPrice).
		Float64("shares", slState.RemainingShares).
		Str("request_id", requestID).
		Msg("engine: STOP_LOSS_SELL sending order")

	resp, err := e.execClient.PlaceMarketOrder(context.Background(), requestID, orderReq)
	if err != nil {
		e.logger.Error().Err(err).Str("slug", w.Slug).Str("request_id", requestID).Msg("engine: stop-loss order failed")
		return
	}

	if !resp.Success {
		e.logger.Warn().
			Str("slug", w.Slug).
			Str("request_id", requestID).
			Str("error", resp.ErrorMsg).
			Msg("engine: stop-loss order rejected")
		return
	}

	// TakingAmount = USDC received, MakingAmount = shares sold
	proceedsUSDC, _ := strconv.ParseFloat(resp.TakingAmount, 64)
	sharesSold, _ := strconv.ParseFloat(resp.MakingAmount, 64)

	slState.TotalProceeds += proceedsUSDC
	slState.TotalSharesSold += sharesSold
	slState.RemainingShares -= sharesSold

	e.balance.Credit(proceedsUSDC)

	e.logger.Info().
		Str("slug", w.Slug).
		Str("reason", reason).
		Str("request_id", requestID).
		Float64("proceeds_usdc", proceedsUSDC).
		Float64("shares_sold", sharesSold).
		Float64("remaining_shares", slState.RemainingShares).
		Msg("engine: STOP_LOSS_SELL filled")

	if e.persistence != nil {
		event := persistence.NewEvent("engine", persistence.EventExecution, "stop_loss_sell")
		event.WithPayloads(map[string]interface{}{
			"slug":             w.Slug,
			"symbol":           w.Symbol,
			"reason":           reason,
			"request_id":       requestID,
			"order_id":         resp.OrderID,
			"bid_price":        bidPrice,
			"proceeds_usdc":    proceedsUSDC,
			"shares_sold":      sharesSold,
			"remaining_shares": slState.RemainingShares,
			"total_proceeds":   slState.TotalProceeds,
			"total_sold":       slState.TotalSharesSold,
			"original_cost":    slState.OriginalCost,
			"attempt":          slState.SellAttempts,
		})
		e.persistence.Write(event)
	}

	if slState.RemainingShares <= 0.001 {
		// Full exit
		pnl := slState.TotalProceeds - slState.OriginalCost
		if err := e.tracker.RecordStopLossExit(w.Slug, slState.TotalProceeds, pnl); err != nil {
			e.logger.Error().Err(err).Str("slug", w.Slug).Msg("engine: failed to record stop-loss exit")
		}
		if err := e.tracker.ClearPosition(w.Slug); err != nil {
			e.logger.Error().Err(err).Str("slug", w.Slug).Msg("engine: failed to clear position after stop-loss")
		}

		e.logger.Info().
			Str("slug", w.Slug).
			Float64("total_proceeds", slState.TotalProceeds).
			Float64("original_cost", slState.OriginalCost).
			Float64("pnl", pnl).
			Msg("engine: STOP_LOSS_EXIT complete")

		// Async balance reconcile
		go func() {
			actual, err := e.execClient.GetCollateralBalance(context.Background())
			if err == nil {
				e.balance.Reconcile(actual)
			}
		}()

		delete(e.stopLossStates, w.Slug)
	} else {
		// Partial fill — update tracker position, retry next tick
		if err := e.tracker.UpdatePositionAfterPartialSell(w.Slug, sharesSold, proceedsUSDC); err != nil {
			e.logger.Error().Err(err).Str("slug", w.Slug).Msg("engine: failed to update position after partial stop-loss")
		}
	}
}

func (e *Engine) monitorPaperStopLoss(w tracker.ActiveWindowSummary) {
	sl := &e.cfg.StopLoss
	if !sl.Enabled || !w.HasPosition {
		return
	}

	slState, exists := e.stopLossStates[w.Slug]
	if !exists {
		slState = &stopLossState{
			RemainingShares: w.PositionSize,
			OriginalCost:    w.PositionSize * w.PositionEntryPrice,
			OriginalSize:    w.PositionSize,
			EntryPrice:      w.PositionEntryPrice,
		}
		e.stopLossStates[w.Slug] = slState
	}

	bid, spread := getBidAndSpread(w)
	stopPrice := e.symbolFloat(sl.StopPrice, w.Symbol)
	if stopPrice == 0 {
		return
	}

	isEmpty := spread > sl.EmptyBookSpreadThresh || bid <= 0.01

	if isEmpty {
		if slState.EmptyBookSince.IsZero() {
			slState.EmptyBookSince = time.Now()
		}
		return
	}

	// Book returned after being empty
	if !slState.EmptyBookSince.IsZero() {
		emptyDuration := time.Since(slState.EmptyBookSince)
		slState.EmptyBookSince = time.Time{}

		if emptyDuration >= time.Duration(sl.EmptyBookTimeoutSecs)*time.Second {
			if bid < stopPrice && bid > sl.MinSellPrice {
				if w.TimeRemainingS >= int64(sl.MinTimeRemainingSecs) {
					e.executePaperStopLoss(w, slState, bid, "empty_book_recovery")
					return
				}
			}
		}
	}

	if w.TimeRemainingS < int64(sl.MinTimeRemainingSecs) {
		return
	}

	if bid < stopPrice && bid > sl.MinSellPrice {
		slState.BelowStopTicks++
		if slState.BelowStopTicks >= sl.ConfirmTicks {
			e.executePaperStopLoss(w, slState, bid, "bid_below_stop")
		}
	} else {
		slState.BelowStopTicks = 0
	}
}

func (e *Engine) executePaperStopLoss(w tracker.ActiveWindowSummary, slState *stopLossState, bid float64, reason string) {
	proceeds := slState.RemainingShares * bid
	pnl := proceeds - slState.OriginalCost

	e.logger.Info().
		Str("slug", w.Slug).
		Str("reason", reason).
		Float64("bid", bid).
		Float64("shares", slState.RemainingShares).
		Float64("proceeds", proceeds).
		Float64("pnl", pnl).
		Msg("engine: PAPER_STOP_LOSS")

	if err := e.tracker.RecordStopLossExit(w.Slug, proceeds, pnl); err != nil {
		e.logger.Error().Err(err).Str("slug", w.Slug).Msg("engine: failed to record paper stop-loss exit")
	}
	if err := e.tracker.ClearPosition(w.Slug); err != nil {
		e.logger.Error().Err(err).Str("slug", w.Slug).Msg("engine: failed to clear position after paper stop-loss")
	}

	if e.persistence != nil {
		event := persistence.NewEvent("engine", persistence.EventDecision, "paper_stop_loss")
		event.WithPayloads(map[string]interface{}{
			"slug":          w.Slug,
			"symbol":        w.Symbol,
			"reason":        reason,
			"bid":           bid,
			"shares":        slState.RemainingShares,
			"proceeds":      proceeds,
			"original_cost": slState.OriginalCost,
			"pnl":           pnl,
		})
		e.persistence.Write(event)
	}

	delete(e.stopLossStates, w.Slug)
}

// getBidAndSpread returns the best bid and bid-ask spread for the position's side.
func getBidAndSpread(w tracker.ActiveWindowSummary) (bid, spread float64) {
	if w.PositionSide == "yes" {
		bid = w.YesBid
		if w.YesAsk > 0 && w.YesBid > 0 {
			spread = w.YesAsk - w.YesBid
		} else {
			spread = 1.0
		}
	} else {
		bid = w.NoBid
		if w.NoAsk > 0 && w.NoBid > 0 {
			spread = w.NoAsk - w.NoBid
		} else {
			spread = 1.0
		}
	}
	return
}
