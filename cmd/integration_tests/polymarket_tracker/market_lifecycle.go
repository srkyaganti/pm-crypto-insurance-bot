package main

import (
	"fmt"
	"time"

	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/connectors/polymarket/gamma"
)

func (mt *MarketTracker) fetchAndSubscribe(slug, prefix string, window int) error {
	mt.logger.Info().
		Str("slug", slug).
		Str("prefix", prefix).
		Int("window", window).
		Msg("Fetching and subscribing to market")

	feedID := mt.prefixToFeedID[prefix]

	marketData, err := mt.gammaClient.GetMarketBySlug(mt.ctx, slug)
	if err != nil {
		return fmt.Errorf("fetch market from gamma: %w", err)
	}

	startTime := marketData.WindowStart
	duration := time.Duration(window) * time.Minute
	endTime := startTime.Add(duration)

	startTimestamp := uint64(startTime.Unix())
	startPrice, err := mt.getChainlinkPriceAtTimestamp(mt.ctx, feedID, startTimestamp)
	if err != nil {
		mt.logger.Error().
			Err(err).
			Uint64("timestamp", startTimestamp).
			Msg("Failed to get Chainlink start price")
		startPrice = 0
	}

	activeMarket := &ActiveMarket{
		Slug:        slug,
		YesAssetID:  marketData.Tokens.UpTokenID,
		NoAssetID:   marketData.Tokens.DownTokenID,
		ConditionID: marketData.ConditionID,
		WindowType:  window,
		FeedID:      feedID,
		Prefix:      prefix,
		StartTime:   startTime,
		EndTime:     endTime,
		StartPrice:  startPrice,
		PaperTrades: []PaperTrade{},
	}

	if err := mt.marketConnector.SubscribeMany([]string{
		activeMarket.YesAssetID,
		activeMarket.NoAssetID,
	}); err != nil {
		return fmt.Errorf("subscribe to market: %w", err)
	}

	timeUntilEnd := time.Until(endTime)

	activeMarket.EndTimer = time.AfterFunc(timeUntilEnd, func() {
		mt.handleMarketEnd(slug)
	})

	mt.mu.Lock()
	mt.activeMarkets[slug] = activeMarket
	mt.mu.Unlock()

	mt.logger.Info().
		Str("slug", slug).
		Int("window", window).
		Time("start_time", startTime).
		Time("end_time", endTime).
		Dur("duration_remaining", timeUntilEnd).
		Float64("chainlink_start_price", startPrice).
		Str("yes_asset", activeMarket.YesAssetID).
		Str("no_asset", activeMarket.NoAssetID).
		Msg("Successfully subscribed to market with end timer set")

	go mt.preScheduleFutureWindows(prefix, window, startTime)

	return nil
}

func (mt *MarketTracker) preScheduleFutureWindows(prefix string, window int, currentStart time.Time) {
	windowDuration := time.Duration(window) * time.Minute

	for i := 1; i <= mt.config.PreScheduleWindows; i++ {
		futureStart := currentStart.Add(time.Duration(i) * windowDuration)
		futureSlug := gamma.BuildSlug(prefix, fmt.Sprintf("%dm", window), futureStart.Unix())

		mt.mu.RLock()
		_, alreadyScheduled := mt.scheduledTimers[futureSlug]
		mt.mu.RUnlock()

		if alreadyScheduled {
			continue
		}

		subscribeTime := futureStart.Add(-10 * time.Second)
		timeUntilSubscribe := time.Until(subscribeTime)

		if timeUntilSubscribe < 0 {
			continue
		}

		mt.logger.Info().
			Str("future_slug", futureSlug).
			Time("future_start", futureStart).
			Dur("subscribes_in", timeUntilSubscribe).
			Msg("Pre-scheduling future market window")

		timer := time.AfterFunc(timeUntilSubscribe, func() {
			mt.logger.Info().
				Str("slug", futureSlug).
				Msg("Timer fired for pre-scheduled market")

			if err := mt.fetchAndSubscribe(futureSlug, prefix, window); err != nil {
				mt.logger.Error().
					Err(err).
					Str("slug", futureSlug).
					Msg("Failed to subscribe to pre-scheduled market")
			}

			mt.mu.Lock()
			delete(mt.scheduledTimers, futureSlug)
			mt.mu.Unlock()
		})

		mt.mu.Lock()
		mt.scheduledTimers[futureSlug] = timer
		mt.mu.Unlock()
	}
}

func (mt *MarketTracker) handleMarketEnd(slug string) {
	mt.logger.Info().
		Str("slug", slug).
		Msg("Market window ending - collecting final data")

	mt.mu.Lock()
	activeMarket, exists := mt.activeMarkets[slug]
	if !exists {
		mt.mu.Unlock()
		mt.logger.Warn().Str("slug", slug).Msg("Market not found in active markets")
		return
	}

	if activeMarket.EndTimer != nil {
		activeMarket.EndTimer.Stop()
	}

	delete(mt.activeMarkets, slug)
	mt.mu.Unlock()

	endTimestamp := uint64(activeMarket.EndTime.Unix())
	endPrice, err := mt.getChainlinkPriceAtTimestamp(mt.ctx, activeMarket.FeedID, endTimestamp)
	if err != nil {
		mt.logger.Error().
			Err(err).
			Uint64("timestamp", endTimestamp).
			Msg("Failed to get Chainlink end price - using 0")
		endPrice = 0
	}

	priceChange := endPrice - activeMarket.StartPrice
	priceChangePct := 0.0
	if activeMarket.StartPrice > 0 {
		priceChangePct = (priceChange / activeMarket.StartPrice) * 100
	}

	winningOutcome := "no"
	if priceChange > 0 {
		winningOutcome = "yes"
	}

	pmState := mt.stateStore.GetPolymarket()
	yesFinalPrice := 0.0
	noFinalPrice := 0.0

	if ob := pmState.OrderBooks[activeMarket.YesAssetID]; ob != nil {
		yesFinalPrice = ob.MidPrice
	}
	if ob := pmState.OrderBooks[activeMarket.NoAssetID]; ob != nil {
		noFinalPrice = ob.MidPrice
	}

	pnl := 0.0
	marketResult := "no_position"

	if activeMarket.MyPosition != nil {
		marketResult = "loss"
		if activeMarket.MyPosition.Side == winningOutcome {
			marketResult = "win"
			pnl = activeMarket.MyPosition.Size * (1.0 - activeMarket.MyPosition.EntryPrice)
		} else {
			pnl = -activeMarket.MyPosition.Size * activeMarket.MyPosition.EntryPrice
		}
	}

	completedMarket := &CompletedMarket{
		Slug:           slug,
		WindowType:     activeMarket.WindowType,
		FeedID:         activeMarket.FeedID,
		Prefix:         activeMarket.Prefix,
		StartTime:      activeMarket.StartTime,
		EndTime:        activeMarket.EndTime,
		YesAssetID:     activeMarket.YesAssetID,
		NoAssetID:      activeMarket.NoAssetID,
		ConditionID:    activeMarket.ConditionID,
		StartPrice:     activeMarket.StartPrice,
		EndPrice:       endPrice,
		PriceChange:    priceChange,
		PriceChangePct: priceChangePct,
		WinningOutcome: winningOutcome,
		MarketResult:   marketResult,
		PaperTrades:    activeMarket.PaperTrades,
		Position:       activeMarket.MyPosition,
		PnL:            pnl,
		DidWeTrade:     activeMarket.MyPosition != nil,
		YesFinalPrice:  yesFinalPrice,
		NoFinalPrice:   noFinalPrice,
		Timestamp:      time.Now(),
	}

	if err := mt.marketHistory.Add(completedMarket); err != nil {
		mt.logger.Error().Err(err).Msg("Failed to save completed market to history")
	}

	if err := mt.marketConnector.UnsubscribeMany([]string{
		activeMarket.YesAssetID,
		activeMarket.NoAssetID,
	}); err != nil {
		mt.logger.Error().
			Err(err).
			Str("slug", slug).
			Msg("Failed to unsubscribe from market")
	}

	mt.logger.Info().
		Str("slug", slug).
		Int("window", activeMarket.WindowType).
		Float64("chainlink_start_price", completedMarket.StartPrice).
		Float64("chainlink_end_price", completedMarket.EndPrice).
		Float64("price_change", completedMarket.PriceChange).
		Float64("price_change_pct", completedMarket.PriceChangePct).
		Str("winning_outcome", completedMarket.WinningOutcome).
		Str("market_result", completedMarket.MarketResult).
		Bool("did_we_trade", completedMarket.DidWeTrade).
		Float64("paper_pnl", completedMarket.PnL).
		Int("num_paper_trades", len(completedMarket.PaperTrades)).
		Float64("yes_market_price", completedMarket.YesFinalPrice).
		Float64("no_market_price", completedMarket.NoFinalPrice).
		Msg("Market completed and archived")
}

func (mt *MarketTracker) GetCurrentActiveMarkets() []string {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	slugs := make([]string, 0, len(mt.activeMarkets))
	for slug := range mt.activeMarkets {
		slugs = append(slugs, slug)
	}
	return slugs
}
