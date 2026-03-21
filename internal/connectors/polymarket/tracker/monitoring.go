package tracker

import (
	"context"
	"time"
)

func (c *Connector) monitoringLoop(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.logCurrentState()
		}
	}
}

func (c *Connector) logCurrentState() {
	c.mu.RLock()
	defer c.mu.RUnlock()

	pmState := c.state.GetPolymarket()

	c.logger.Info().
		Int("active_markets", len(c.activeMarkets)).
		Int("total_subscriptions", len(pmState.SubscribedAssets)).
		Bool("is_stale", pmState.IsStale).
		Int("scheduled_timers", len(c.scheduledTimers)).
		Int("history_count", len(c.marketHistory)).
		Int("pending_redeems", len(c.pendingRedeems)).
		Msg("tracker: state")

	for slug, market := range c.activeMarkets {
		timeRemaining := time.Until(market.EndTime)

		yesOB := pmState.OrderBooks[market.YesAssetID]
		noOB := pmState.OrderBooks[market.NoAssetID]

		event := c.logger.Info().
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

		event.Msg("tracker: active market status")
	}
}

func (c *Connector) strategyAnalysisLoop(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.analyzeHistoricalPerformance()
		}
	}
}

func (c *Connector) analyzeHistoricalPerformance() {
	c.mu.RLock()
	recentCount := 20
	if len(c.marketHistory) < recentCount {
		recentCount = len(c.marketHistory)
	}
	recentMarkets := c.marketHistory[len(c.marketHistory)-recentCount:]
	c.mu.RUnlock()

	if len(recentMarkets) == 0 {
		return
	}

	stats := c.GetStats()

	c.logger.Info().
		Interface("stats", stats).
		Msg("tracker: historical performance analysis")
}
