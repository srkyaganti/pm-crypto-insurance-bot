package tracker

import (
	"fmt"
	"time"

	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/persistence"
)

func (c *Connector) ExecutePaperTrade(slug, assetID, side, reason string, size float64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	market, exists := c.activeMarkets[slug]
	if !exists {
		return fmt.Errorf("market not found: %s", slug)
	}

	pmState := c.state.GetPolymarket()
	ob := pmState.OrderBooks[assetID]
	if ob == nil {
		return fmt.Errorf("no orderbook for asset: %s", assetID)
	}

	execPrice := ob.MidPrice

	trade := PaperTrade{
		Timestamp: time.Now(),
		AssetID:   assetID,
		Side:      side,
		Price:     execPrice,
		Size:      size,
		Reason:    reason,
	}

	market.PaperTrades = append(market.PaperTrades, trade)

	if market.MyPosition == nil {
		outcome := "no"
		if assetID == market.YesAssetID {
			outcome = "yes"
		}

		market.MyPosition = &Position{
			AssetID:    assetID,
			Side:       outcome,
			EntryPrice: execPrice,
			Size:       size,
			OpenedAt:   time.Now(),
		}
	}

	c.logEvent(persistence.EventDecision, "paper_trade_executed", map[string]interface{}{
		"slug":   slug,
		"asset":  assetID,
		"side":   side,
		"price":  execPrice,
		"size":   size,
		"reason": reason,
	})

	c.logger.Info().
		Str("slug", slug).
		Str("asset_id", assetID).
		Str("side", side).
		Float64("price", execPrice).
		Float64("size", size).
		Str("reason", reason).
		Msg("tracker: executed paper trade")

	return nil
}

// SetLivePosition records a real trade position on an active market.
func (c *Connector) SetLivePosition(slug, assetID, side string, shares, cost float64, orderID, requestID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	market, exists := c.activeMarkets[slug]
	if !exists {
		return fmt.Errorf("market not found: %s", slug)
	}

	outcome := "no"
	if assetID == market.YesAssetID {
		outcome = "yes"
	}

	market.MyPosition = &Position{
		AssetID:    assetID,
		Side:       outcome,
		EntryPrice: cost / shares,
		Size:       shares,
		Cost:       cost,
		OrderID:    orderID,
		RequestID:  requestID,
		IsLive:     true,
		OpenedAt:   time.Now(),
	}

	c.logEvent(persistence.EventExecution, "live_trade_recorded", map[string]interface{}{
		"slug":       slug,
		"asset_id":   assetID,
		"side":       side,
		"shares":     shares,
		"cost":       cost,
		"order_id":   orderID,
		"request_id": requestID,
	})

	c.logger.Info().
		Str("slug", slug).
		Str("asset_id", assetID).
		Str("side", side).
		Float64("shares", shares).
		Float64("cost", cost).
		Str("order_id", orderID).
		Msg("tracker: recorded live position")

	return nil
}

func (c *Connector) GetMarketPosition(slug string) *Position {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if market, exists := c.activeMarkets[slug]; exists {
		return market.MyPosition
	}
	return nil
}

// ClearPosition removes the position from an active market after a full stop-loss exit.
func (c *Connector) ClearPosition(slug string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if market, exists := c.activeMarkets[slug]; exists {
		market.MyPosition = nil
	}
}

// UpdatePositionAfterPartialSell reduces position size and cost after a partial stop-loss fill.
// If remaining size <= 0.001 (float tolerance), clears the position entirely.
func (c *Connector) UpdatePositionAfterPartialSell(slug string, sharesSold, proceedsUSDC float64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	market, exists := c.activeMarkets[slug]
	if !exists || market.MyPosition == nil {
		return
	}

	market.MyPosition.Size -= sharesSold
	market.MyPosition.Cost -= proceedsUSDC
	if market.MyPosition.Cost < 0 {
		market.MyPosition.Cost = 0
	}

	if market.MyPosition.Size <= 0.001 {
		market.MyPosition = nil
	}
}

// RecordStopLossExit marks a market as having been exited via stop-loss,
// so handleMarketEnd knows the position was already closed.
func (c *Connector) RecordStopLossExit(slug string, proceeds, pnl float64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if market, exists := c.activeMarkets[slug]; exists {
		market.StopLossExited = true
		market.StopLossPnL = pnl
		market.StopLossProceeds = proceeds
	}
}
