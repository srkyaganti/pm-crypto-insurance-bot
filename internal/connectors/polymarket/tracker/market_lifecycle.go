package tracker

import (
	"fmt"
	"time"

	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/connectors/polymarket/gamma"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/persistence"
)

// fetchAndRegisterMarket fetches market data and registers it as active, returning asset IDs.
// Does NOT subscribe to the WS — caller is responsible for batching subscriptions.
func (c *Connector) fetchAndRegisterMarket(slug, prefix string, window int) ([]string, error) {
	feedID := c.prefixToFeedID[prefix]

	marketData, err := c.gammaClient.GetMarketBySlug(c.ctx, slug)
	if err != nil {
		return nil, fmt.Errorf("fetch market from gamma: %w", err)
	}

	startTime := marketData.WindowStart
	duration := time.Duration(window) * time.Minute
	endTime := startTime.Add(duration)

	startTimestamp := uint64(startTime.Unix())

	// If startTime is in the past (>5s ago), fetch immediately.
	// Otherwise defer: the pre-scheduler fires 10s before start, so the timestamp
	// would be rejected by Chainlink as "in the future".
	var startPrice float64
	fetchDeadline := startTime.Add(5 * time.Second)
	if time.Now().After(fetchDeadline) {
		var err error
		startPrice, err = c.getChainlinkPriceAtTimestamp(c.ctx, feedID, startTimestamp)
		if err != nil {
			c.logger.Error().
				Err(err).
				Uint64("timestamp", startTimestamp).
				Str("feed_id", feedID).
				Msg("tracker: failed to get chainlink start price")
		}
	}

	activeMarket := &ActiveMarket{
		Slug:        slug,
		YesAssetID:  marketData.Tokens.UpTokenID,
		NoAssetID:   marketData.Tokens.DownTokenID,
		ConditionID: marketData.ConditionID,
		WindowType:  window,
		FeedID:      feedID,
		Symbol:      c.prefixToSymbol[prefix],
		Prefix:      prefix,
		StartTime:   startTime,
		EndTime:     endTime,
		StartPrice:  startPrice,
		PaperTrades: []PaperTrade{},
	}

	timeUntilEnd := time.Until(endTime)
	activeMarket.EndTimer = time.AfterFunc(timeUntilEnd, func() {
		c.handleMarketEnd(slug)
	})

	c.mu.Lock()
	c.activeMarkets[slug] = activeMarket
	c.mu.Unlock()

	// If start price was deferred, fetch it in the background once the timestamp is valid.
	if startPrice == 0 && time.Now().Before(fetchDeadline) {
		go func() {
			delay := time.Until(fetchDeadline)
			select {
			case <-c.ctx.Done():
				return
			case <-time.After(delay):
			}
			price, err := c.getChainlinkPriceAtTimestamp(c.ctx, feedID, startTimestamp)
			if err != nil {
				c.logger.Error().Err(err).Str("slug", slug).Msg("tracker: failed to fetch deferred start price")
				return
			}
			c.mu.Lock()
			if am, exists := c.activeMarkets[slug]; exists {
				am.StartPrice = price
			}
			c.mu.Unlock()
			c.logger.Info().Str("slug", slug).Float64("start_price", price).Msg("tracker: updated deferred start price")
		}()
	}

	c.logger.Info().
		Str("slug", slug).
		Int("window", window).
		Str("feed_id", feedID).
		Time("start_time", startTime).
		Time("end_time", endTime).
		Dur("duration_remaining", timeUntilEnd).
		Float64("chainlink_start_price", startPrice).
		Str("yes_asset", activeMarket.YesAssetID).
		Str("no_asset", activeMarket.NoAssetID).
		Msg("tracker: registered market")

	go c.preScheduleFutureWindows(prefix, window, startTime)

	return []string{activeMarket.YesAssetID, activeMarket.NoAssetID}, nil
}

// fetchAndSubscribe fetches a market and subscribes by reconnecting the WS.
// Used for dynamically added markets (pre-scheduled future windows).
func (c *Connector) fetchAndSubscribe(slug, prefix string, window int) error {
	c.logger.Info().
		Str("slug", slug).
		Str("prefix", prefix).
		Int("window", window).
		Msg("tracker: fetching and subscribing to market")

	assetIDs, err := c.fetchAndRegisterMarket(slug, prefix, window)
	if err != nil {
		return err
	}

	// Track the new assets in the subscription manager
	if err := c.marketConnector.SubscribeMany(assetIDs); err != nil {
		return fmt.Errorf("subscribe to market: %w", err)
	}

	// Polymarket WS only accepts subscriptions on connect, so reconnect
	// to pick up the new assets via ResubscribeAll.
	c.marketConnector.Reconnect()

	return nil
}

func (c *Connector) preScheduleFutureWindows(prefix string, window int, currentStart time.Time) {
	windowDuration := time.Duration(window) * time.Minute

	for i := 1; i <= c.config.PreScheduleWindows; i++ {
		futureStart := currentStart.Add(time.Duration(i) * windowDuration)
		futureSlug := gamma.BuildSlug(prefix, fmt.Sprintf("%dm", window), futureStart.Unix())

		c.mu.RLock()
		_, alreadyScheduled := c.scheduledTimers[futureSlug]
		c.mu.RUnlock()

		if alreadyScheduled {
			continue
		}

		subscribeTime := futureStart.Add(-10 * time.Second)
		timeUntilSubscribe := time.Until(subscribeTime)

		if timeUntilSubscribe < 0 {
			continue
		}

		c.logger.Info().
			Str("future_slug", futureSlug).
			Time("future_start", futureStart).
			Dur("subscribes_in", timeUntilSubscribe).
			Msg("tracker: pre-scheduling future market window")

		timer := time.AfterFunc(timeUntilSubscribe, func() {
			c.logger.Info().
				Str("slug", futureSlug).
				Msg("tracker: timer fired for pre-scheduled market")

			if err := c.fetchAndSubscribe(futureSlug, prefix, window); err != nil {
				c.logger.Error().
					Err(err).
					Str("slug", futureSlug).
					Msg("tracker: failed to subscribe to pre-scheduled market")
			}

			c.mu.Lock()
			delete(c.scheduledTimers, futureSlug)
			c.mu.Unlock()
		})

		c.mu.Lock()
		c.scheduledTimers[futureSlug] = timer
		c.mu.Unlock()
	}
}

func (c *Connector) handleMarketEnd(slug string) {
	c.logger.Info().
		Str("slug", slug).
		Msg("tracker: market window ending - collecting final data")

	c.mu.Lock()
	activeMarket, exists := c.activeMarkets[slug]
	if !exists {
		c.mu.Unlock()
		c.logger.Warn().Str("slug", slug).Msg("tracker: market not found in active markets")
		return
	}

	if activeMarket.EndTimer != nil {
		activeMarket.EndTimer.Stop()
	}

	delete(c.activeMarkets, slug)
	c.mu.Unlock()

	endTimestamp := uint64(activeMarket.EndTime.Unix())
	endPrice, endPriceErr := c.getChainlinkPriceAtTimestamp(c.ctx, activeMarket.FeedID, endTimestamp)
	if endPriceErr != nil {
		c.logger.Error().
			Err(endPriceErr).
			Uint64("timestamp", endTimestamp).
			Msg("tracker: failed to get chainlink end price after retries")
		endPrice = 0
	}

	// Read PM prices before winner determination (used as fallback)
	pmState := c.state.GetPolymarket()
	yesFinalPrice := 0.0
	noFinalPrice := 0.0

	if ob := pmState.OrderBooks[activeMarket.YesAssetID]; ob != nil {
		yesFinalPrice = ob.MidPrice
	}
	if ob := pmState.OrderBooks[activeMarket.NoAssetID]; ob != nil {
		noFinalPrice = ob.MidPrice
	}

	priceChange := 0.0
	priceChangePct := 0.0
	winningOutcome := "unknown"
	if endPriceErr == nil {
		priceChange = endPrice - activeMarket.StartPrice
		if activeMarket.StartPrice > 0 {
			priceChangePct = (priceChange / activeMarket.StartPrice) * 100
		}
		if priceChange >= 0 {
			winningOutcome = "yes"
		} else {
			winningOutcome = "no"
		}
	} else {
		// Fallback: use PM book prices to infer winner
		if yesFinalPrice > 0.5 {
			winningOutcome = "yes"
			c.logger.Warn().Float64("yes_price", yesFinalPrice).Msg("tracker: CL end price failed, using PM yes price as fallback for winner")
		} else if noFinalPrice > 0.5 {
			winningOutcome = "no"
			c.logger.Warn().Float64("no_price", noFinalPrice).Msg("tracker: CL end price failed, using PM no price as fallback for winner")
		}
	}

	pnl := 0.0
	marketResult := "no_position"

	if activeMarket.StopLossExited && activeMarket.MyPosition == nil {
		// Fully exited via stop-loss before resolution
		marketResult = "stop_loss"
		pnl = activeMarket.StopLossPnL
	} else if activeMarket.MyPosition != nil {
		marketResult = "loss"
		if activeMarket.MyPosition.Side == winningOutcome {
			marketResult = "win"
			pnl = activeMarket.MyPosition.Size * (1.0 - activeMarket.MyPosition.EntryPrice)
		} else {
			pnl = -activeMarket.MyPosition.Size * activeMarket.MyPosition.EntryPrice
		}
		// Add any partial stop-loss proceeds
		pnl += activeMarket.StopLossPnL
	} else if activeMarket.StopLossProceeds > 0 {
		// Stop-loss proceeds exist but position was cleared some other way
		marketResult = "stop_loss"
		pnl = activeMarket.StopLossPnL
	}

	completedMarket := &CompletedMarket{
		Slug:           slug,
		WindowType:     activeMarket.WindowType,
		FeedID:         activeMarket.FeedID,
		Symbol:         activeMarket.Symbol,
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
		PnL:              pnl,
		DidWeTrade:       activeMarket.MyPosition != nil || activeMarket.StopLossExited,
		StopLossExited:   activeMarket.StopLossExited,
		StopLossPnL:      activeMarket.StopLossPnL,
		StopLossProceeds: activeMarket.StopLossProceeds,
		YesFinalPrice:    yesFinalPrice,
		NoFinalPrice:   noFinalPrice,
		Timestamp:      time.Now(),
	}

	// Live trading: if fully exited via stop-loss, just reconcile balance
	if activeMarket.StopLossExited && activeMarket.MyPosition == nil && c.execClient != nil {
		go func() {
			actual, err := c.execClient.GetCollateralBalance(c.ctx)
			if err == nil {
				c.balance.Reconcile(actual)
			}
		}()
	} else
	// Live trading: queue redeem for winning positions, reconcile on loss
	if activeMarket.MyPosition != nil && activeMarket.MyPosition.IsLive && c.execClient != nil {
		if activeMarket.MyPosition.Side == winningOutcome || winningOutcome == "unknown" {
			pr := &PendingRedeem{
				Slug:           slug,
				ConditionID:    activeMarket.ConditionID,
				WinningOutcome: winningOutcome,
				PositionSide:   activeMarket.MyPosition.Side,
				PositionSize:   activeMarket.MyPosition.Size,
				EntryPrice:     activeMarket.MyPosition.EntryPrice,
				MarketEndTime:  activeMarket.EndTime,
				QueuedAt:       time.Now(),
			}
			c.mu.Lock()
			c.pendingRedeems = append(c.pendingRedeems, pr)
			c.mu.Unlock()
			c.logger.Info().
				Str("slug", slug).
				Str("winning_outcome", winningOutcome).
				Msg("tracker: queued pending redeem")
		} else {
			// Known loss — just reconcile balance
			go func() {
				actual, err := c.execClient.GetCollateralBalance(c.ctx)
				if err == nil {
					c.balance.Reconcile(actual)
				}
			}()
		}
	}

	c.mu.Lock()
	c.marketHistory = append(c.marketHistory, completedMarket)
	c.mu.Unlock()

	c.logEvent(persistence.EventState, "market_completed", map[string]interface{}{
		"slug":             completedMarket.Slug,
		"window_type":      completedMarket.WindowType,
		"feed_id":          completedMarket.FeedID,
		"start_time":       completedMarket.StartTime,
		"end_time":         completedMarket.EndTime,
		"start_price":      completedMarket.StartPrice,
		"end_price":        completedMarket.EndPrice,
		"price_change":     completedMarket.PriceChange,
		"price_change_pct": completedMarket.PriceChangePct,
		"winning_outcome":  completedMarket.WinningOutcome,
		"market_result":    completedMarket.MarketResult,
		"did_we_trade":     completedMarket.DidWeTrade,
		"pnl":                completedMarket.PnL,
		"stop_loss_exited":   completedMarket.StopLossExited,
		"stop_loss_pnl":      completedMarket.StopLossPnL,
		"stop_loss_proceeds": completedMarket.StopLossProceeds,
		"num_paper_trades":   len(completedMarket.PaperTrades),
		"yes_final_price":    completedMarket.YesFinalPrice,
		"no_final_price":     completedMarket.NoFinalPrice,
	})

	if err := c.marketConnector.UnsubscribeMany([]string{
		activeMarket.YesAssetID,
		activeMarket.NoAssetID,
	}); err != nil {
		c.logger.Error().
			Err(err).
			Str("slug", slug).
			Msg("tracker: failed to unsubscribe from market")
	}

	c.logger.Info().
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
		Msg("tracker: market completed and archived")
}
