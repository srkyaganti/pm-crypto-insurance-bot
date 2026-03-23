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

// monitorStopLoss checks whether a position should be exited via stop-loss.
// Called from tick() for every active market summary.
func (e *Engine) monitorStopLoss(w tracker.ActiveWindowSummary) {
	if !w.HasPosition {
		delete(e.stopLossStates, w.Slug)
		return
	}
	if !e.cfg.StopLoss.Enabled {
		return
	}

	if e.cfg.Mode != "live" {
		e.monitorPaperStopLoss(w)
		return
	}

	slState := e.getOrCreateStopLossState(w)
	if slState.SellPending {
		return
	}

	ourBid, spread := getBidAndSpread(w)
	stopPrice := e.symbolFloat(e.cfg.StopLoss.StopPrice, w.Symbol)
	isEmptyBook := spread > e.cfg.StopLoss.EmptyBookSpreadThresh || ourBid <= 0.01
	now := time.Now()

	// --- TRIGGER 1: Empty book timeout ---
	if isEmptyBook {
		if slState.EmptyBookSince.IsZero() {
			slState.EmptyBookSince = now
		}
		// Nothing to sell into on an empty book — wait for liquidity to return
		return
	}

	// Book is real. Check if it was previously empty beyond timeout.
	if !slState.EmptyBookSince.IsZero() {
		elapsed := now.Sub(slState.EmptyBookSince)
		if elapsed >= time.Duration(e.cfg.StopLoss.EmptyBookTimeoutS)*time.Second {
			// Book just came back after extended empty period — only sell if bid is below stop
			if ourBid < stopPrice && ourBid > e.cfg.StopLoss.MinSellPrice {
				e.executeStopLossSell(w, slState, ourBid, "empty_book_recovery")
			}
		}
		slState.EmptyBookSince = time.Time{} // reset
		return
	}

	// --- GUARD: Time remaining ---
	if w.TimeRemainingS < int64(e.cfg.StopLoss.MinTimeRemainingS) {
		return
	}

	// --- TRIGGER 2: Bid below stop price on real book ---
	if ourBid < stopPrice && ourBid > e.cfg.StopLoss.MinSellPrice {
		slState.BelowStopTicks++
		if slState.BelowStopTicks >= e.cfg.StopLoss.ConfirmTicks {
			e.executeStopLossSell(w, slState, ourBid, "stop_triggered")
		}
	} else {
		slState.BelowStopTicks = 0
	}
}

// executeStopLossSell sends a FAK SELL order and processes the result.
func (e *Engine) executeStopLossSell(w tracker.ActiveWindowSummary, slState *stopLossState, bidPrice float64, reason string) {
	slState.SellPending = true
	defer func() { slState.SellPending = false }()

	requestID := fmt.Sprintf("stoploss-%s-%d", w.Slug, time.Now().UnixMilli())

	orderReq := execution.MarketOrderRequest{
		TokenID:   w.PositionAssetID,
		Side:      "SELL",
		Amount:    slState.RemainingShares,
		TickSize:  "0.01",
		NegRisk:   false,
		OrderType: "FAK",
		Price:     bidPrice,
	}

	e.logger.Warn().
		Str("slug", w.Slug).
		Str("symbol", w.Symbol).
		Str("reason", reason).
		Float64("bid_price", bidPrice).
		Float64("shares", slState.RemainingShares).
		Str("request_id", requestID).
		Msg("engine: STOP_LOSS_ATTEMPT")

	if e.persistence != nil {
		event := persistence.NewEvent("engine", persistence.EventExecution, "stop_loss_attempt")
		event.WithPayloads(map[string]interface{}{
			"slug":       w.Slug,
			"symbol":     w.Symbol,
			"reason":     reason,
			"bid_price":  bidPrice,
			"shares":     slState.RemainingShares,
			"request_id": requestID,
			"asset_id":   w.PositionAssetID,
		})
		e.persistence.Write(event)
	}

	resp, err := e.execClient.PlaceMarketOrder(context.Background(), requestID, orderReq)
	if err != nil {
		slState.SellAttempts++
		e.logger.Error().Err(err).
			Str("slug", w.Slug).
			Str("request_id", requestID).
			Int("attempts", slState.SellAttempts).
			Msg("engine: STOP_LOSS_ERROR")
		return
	}

	if !resp.Success {
		slState.SellAttempts++
		e.logger.Warn().
			Str("slug", w.Slug).
			Str("request_id", requestID).
			Str("error", resp.ErrorMsg).
			Int("attempts", slState.SellAttempts).
			Msg("engine: STOP_LOSS_REJECTED")
		return
	}

	// SELL response: TakingAmount=USDC received, MakingAmount=shares sold
	usdcReceived, _ := strconv.ParseFloat(resp.TakingAmount, 64)
	sharesSold, _ := strconv.ParseFloat(resp.MakingAmount, 64)

	if sharesSold <= 0 {
		e.logger.Warn().
			Str("slug", w.Slug).
			Str("request_id", requestID).
			Msg("engine: STOP_LOSS_NO_FILL")
		return
	}

	avgPrice := usdcReceived / sharesSold
	slState.RemainingShares -= sharesSold
	slState.TotalProceeds += usdcReceived
	slState.TotalSharesSold += sharesSold

	e.balance.Credit(usdcReceived)

	isFullExit := slState.RemainingShares <= 0.001

	e.logger.Warn().
		Str("slug", w.Slug).
		Str("symbol", w.Symbol).
		Str("reason", reason).
		Float64("shares_sold", sharesSold).
		Float64("usdc_received", usdcReceived).
		Float64("avg_price", avgPrice).
		Float64("remaining_shares", slState.RemainingShares).
		Bool("full_exit", isFullExit).
		Str("request_id", requestID).
		Msg("engine: STOP_LOSS_FILL")

	if e.persistence != nil {
		event := persistence.NewEvent("engine", persistence.EventExecution, "stop_loss_fill")
		event.WithPayloads(map[string]interface{}{
			"slug":             w.Slug,
			"symbol":           w.Symbol,
			"reason":           reason,
			"shares_sold":      sharesSold,
			"usdc_received":    usdcReceived,
			"avg_price":        avgPrice,
			"remaining_shares": slState.RemainingShares,
			"full_exit":        isFullExit,
			"request_id":       requestID,
			"total_proceeds":   slState.TotalProceeds,
			"total_sold":       slState.TotalSharesSold,
		})
		e.persistence.Write(event)
	}

	if isFullExit {
		totalPnL := slState.TotalProceeds - (slState.TotalSharesSold * w.PositionEntryPrice)
		e.tracker.RecordStopLossExit(w.Slug, slState.TotalProceeds, totalPnL)
		e.tracker.ClearPosition(w.Slug)
		delete(e.stopLossStates, w.Slug)

		e.logger.Info().
			Str("slug", w.Slug).
			Float64("total_pnl", totalPnL).
			Float64("total_proceeds", slState.TotalProceeds).
			Msg("engine: STOP_LOSS_COMPLETE")

		// Async balance reconciliation
		go func() {
			actual, err := e.execClient.GetCollateralBalance(context.Background())
			if err == nil {
				e.balance.Reconcile(actual)
			}
		}()
	} else {
		e.tracker.UpdatePositionAfterPartialSell(w.Slug, sharesSold, usdcReceived)
	}
}

// monitorPaperStopLoss simulates stop-loss monitoring in paper mode.
func (e *Engine) monitorPaperStopLoss(w tracker.ActiveWindowSummary) {
	slState := e.getOrCreateStopLossState(w)

	ourBid, spread := getBidAndSpread(w)
	stopPrice := e.symbolFloat(e.cfg.StopLoss.StopPrice, w.Symbol)
	isEmptyBook := spread > e.cfg.StopLoss.EmptyBookSpreadThresh || ourBid <= 0.01

	if isEmptyBook {
		return
	}

	if w.TimeRemainingS < int64(e.cfg.StopLoss.MinTimeRemainingS) {
		return
	}

	if ourBid < stopPrice && ourBid > e.cfg.StopLoss.MinSellPrice {
		slState.BelowStopTicks++
		if slState.BelowStopTicks >= e.cfg.StopLoss.ConfirmTicks {
			pnl := (ourBid - w.PositionEntryPrice) * w.PositionSize

			e.logger.Warn().
				Str("slug", w.Slug).
				Str("symbol", w.Symbol).
				Float64("bid_price", ourBid).
				Float64("stop_price", stopPrice).
				Float64("position_size", w.PositionSize).
				Float64("paper_pnl", pnl).
				Msg("engine: PAPER_STOP_LOSS_TRIGGER")

			if e.persistence != nil {
				event := persistence.NewEvent("engine", persistence.EventDecision, "paper_stop_loss")
				event.WithPayloads(map[string]interface{}{
					"slug":          w.Slug,
					"symbol":        w.Symbol,
					"bid_price":     ourBid,
					"stop_price":    stopPrice,
					"position_size": w.PositionSize,
					"entry_price":   w.PositionEntryPrice,
					"paper_pnl":     pnl,
				})
				e.persistence.Write(event)
			}

			proceeds := ourBid * w.PositionSize
			e.tracker.RecordStopLossExit(w.Slug, proceeds, pnl)
			e.tracker.ClearPosition(w.Slug)
			delete(e.stopLossStates, w.Slug)
		}
	} else {
		slState.BelowStopTicks = 0
	}
}

// getOrCreateStopLossState returns the existing state or initializes a new one.
func (e *Engine) getOrCreateStopLossState(w tracker.ActiveWindowSummary) *stopLossState {
	if sl, ok := e.stopLossStates[w.Slug]; ok {
		return sl
	}
	sl := &stopLossState{
		RemainingShares: w.PositionSize,
	}
	e.stopLossStates[w.Slug] = sl
	return sl
}

// getBidAndSpread returns the bid and spread for the position's side.
func getBidAndSpread(w tracker.ActiveWindowSummary) (bid, spread float64) {
	if w.PositionSide == "yes" {
		return w.YesBid, w.YesAsk - w.YesBid
	}
	return w.NoBid, w.NoAsk - w.NoBid
}
