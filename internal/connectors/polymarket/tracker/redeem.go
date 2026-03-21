package tracker

import (
	"context"
	"fmt"
	"time"
)

func (c *Connector) redeemLoop(ctx context.Context) {
	intervalSecs := c.config.RedeemLoopIntervalSecs
	if intervalSecs <= 0 {
		intervalSecs = 15
	}

	ticker := time.NewTicker(time.Duration(intervalSecs) * time.Second)
	defer ticker.Stop()

	c.logger.Info().Int("interval_secs", intervalSecs).Msg("tracker: redeem loop started")

	for {
		select {
		case <-ctx.Done():
			c.logger.Info().Msg("tracker: redeem loop stopped")
			return
		case <-ticker.C:
			c.processRedeems(ctx)
		}
	}
}

func (c *Connector) processRedeems(ctx context.Context) {
	c.mu.RLock()
	if len(c.pendingRedeems) == 0 {
		c.mu.RUnlock()
		return
	}
	// Snapshot pending redeems
	pending := make([]*PendingRedeem, len(c.pendingRedeems))
	copy(pending, c.pendingRedeems)
	c.mu.RUnlock()

	delaySecs := c.config.RedeemDelaySecs
	if delaySecs <= 0 {
		delaySecs = 30
	}
	delay := time.Duration(delaySecs) * time.Second

	maxRetries := c.config.RedeemMaxRetries
	if maxRetries <= 0 {
		maxRetries = 5
	}

	var toRemove []int
	for i, pr := range pending {
		if time.Since(pr.QueuedAt) < delay {
			continue
		}

		success := c.attemptRedeem(ctx, pr)
		pr.Attempts++

		if success || pr.Attempts >= maxRetries {
			toRemove = append(toRemove, i)
			if !success {
				c.logger.Error().
					Str("slug", pr.Slug).
					Int("attempts", pr.Attempts).
					Msg("tracker: redeem exhausted retries")
			}
		}
	}

	if len(toRemove) > 0 {
		c.mu.Lock()
		c.pendingRedeems = removeIndices(c.pendingRedeems, toRemove)
		c.mu.Unlock()
	}
}

func (c *Connector) attemptRedeem(ctx context.Context, pr *PendingRedeem) bool {
	c.logger.Info().
		Str("slug", pr.Slug).
		Str("condition_id", pr.ConditionID).
		Str("winning_outcome", pr.WinningOutcome).
		Int("attempt", pr.Attempts+1).
		Msg("tracker: attempting redeem")

	// Verify we have a confirmed trade for this condition
	trades, err := c.execClient.GetTrades(ctx)
	if err != nil {
		c.logger.Warn().Err(err).Str("slug", pr.Slug).Msg("tracker: failed to fetch trades for redeem verification")
		// Proceed anyway — trade verification is best-effort
	} else {
		found := false
		for _, t := range trades {
			if t.Market == pr.ConditionID && t.Status == "CONFIRMED" {
				found = true
				break
			}
		}
		if !found {
			c.logger.Warn().Str("slug", pr.Slug).Msg("tracker: no confirmed trade found for condition, attempting redeem anyway")
		}
	}

	requestID := fmt.Sprintf("redeem-%s-%d", pr.Slug, time.Now().UnixMilli())
	if err := c.execClient.RedeemAndWait(ctx, requestID, pr.ConditionID); err != nil {
		c.logger.Error().Err(err).Str("slug", pr.Slug).Msg("tracker: redeem failed")
		return false
	}

	payout := pr.PositionSize
	c.balance.Credit(payout)
	c.logger.Info().
		Str("slug", pr.Slug).
		Float64("payout", payout).
		Msg("tracker: redeemed successfully")

	go func() {
		actual, err := c.execClient.GetCollateralBalance(ctx)
		if err != nil {
			c.logger.Error().Err(err).Msg("tracker: balance reconciliation failed after redeem")
			return
		}
		c.balance.Reconcile(actual)
	}()

	return true
}

// removeIndices removes elements at the given sorted indices from a slice.
func removeIndices(s []*PendingRedeem, indices []int) []*PendingRedeem {
	result := make([]*PendingRedeem, 0, len(s)-len(indices))
	idxSet := make(map[int]struct{}, len(indices))
	for _, idx := range indices {
		idxSet[idx] = struct{}{}
	}
	for i, v := range s {
		if _, skip := idxSet[i]; !skip {
			result = append(result, v)
		}
	}
	return result
}
