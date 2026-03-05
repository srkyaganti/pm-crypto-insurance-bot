package main

import (
	"fmt"
	"time"
)

func (mt *MarketTracker) ExecutePaperTrade(slug, assetID, side, reason string, size float64) error {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	market, exists := mt.activeMarkets[slug]
	if !exists {
		return fmt.Errorf("market not found: %s", slug)
	}

	pmState := mt.stateStore.GetPolymarket()
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

	mt.logger.Info().
		Str("slug", slug).
		Str("asset_id", assetID).
		Str("side", side).
		Float64("price", execPrice).
		Float64("size", size).
		Str("reason", reason).
		Msg("Executed paper trade")

	return nil
}

func (mt *MarketTracker) GetMarketPosition(slug string) *Position {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	if market, exists := mt.activeMarkets[slug]; exists {
		return market.MyPosition
	}
	return nil
}

func (mt *MarketTracker) GetActiveMarket(slug string) *ActiveMarket {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	if market, exists := mt.activeMarkets[slug]; exists {
		return market
	}
	return nil
}

func (mt *MarketTracker) GetMarketHistory() *MarketHistory {
	return mt.marketHistory
}
