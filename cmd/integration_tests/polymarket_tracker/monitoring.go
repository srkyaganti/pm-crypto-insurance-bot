package main

import (
	"time"
)

func (mt *MarketTracker) monitoringLoop() {
	ticker := time.NewTicker(mt.config.LogInterval)
	defer ticker.Stop()

	for {
		select {
		case <-mt.ctx.Done():
			return
		case <-ticker.C:
			mt.logCurrentState()
		}
	}
}

func (mt *MarketTracker) logCurrentState() {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	pmState := mt.stateStore.GetPolymarket()

	mt.logger.Info().
		Int("active_markets", len(mt.activeMarkets)).
		Int("total_subscriptions", len(pmState.SubscribedAssets)).
		Bool("is_stale", pmState.IsStale).
		Int("scheduled_timers", len(mt.scheduledTimers)).
		Int("history_count", mt.marketHistory.Count()).
		Msg("Market tracker state")

	for slug, market := range mt.activeMarkets {
		timeRemaining := time.Until(market.EndTime)

		yesOB := pmState.OrderBooks[market.YesAssetID]
		noOB := pmState.OrderBooks[market.NoAssetID]

		event := mt.logger.Info().
			Str("slug", slug).
			Int("window", market.WindowType).
			Dur("time_remaining", timeRemaining).
			Float64("chainlink_start_price", market.StartPrice).
			Int("paper_trades", len(market.PaperTrades))

		if yesOB != nil {
			event = event.
				Float64("yes_bid", yesOB.BestBid).
				Float64("yes_ask", yesOB.BestAsk).
				Float64("yes_mid", yesOB.MidPrice).
				Int("yes_bid_levels", len(yesOB.Bids)).
				Int("yes_ask_levels", len(yesOB.Asks))
		}

		if noOB != nil {
			event = event.
				Float64("no_bid", noOB.BestBid).
				Float64("no_ask", noOB.BestAsk).
				Float64("no_mid", noOB.MidPrice).
				Int("no_bid_levels", len(noOB.Bids)).
				Int("no_ask_levels", len(noOB.Asks))
		}

		if market.MyPosition != nil {
			event = event.
				Str("position_side", market.MyPosition.Side).
				Float64("position_entry", market.MyPosition.EntryPrice).
				Float64("position_size", market.MyPosition.Size)
		}

		event.Msg("Active market status")
	}
}

func (mt *MarketTracker) strategyAnalysisLoop() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-mt.ctx.Done():
			return
		case <-ticker.C:
			mt.analyzeHistoricalPerformance()
		}
	}
}

func (mt *MarketTracker) analyzeHistoricalPerformance() {
	recentMarkets := mt.marketHistory.GetRecent(20)

	if len(recentMarkets) == 0 {
		return
	}

	stats := mt.marketHistory.GetStats()

	mt.logger.Info().
		Interface("stats", stats).
		Msg("Historical performance analysis")
}
