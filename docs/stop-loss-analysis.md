# Stop-Loss Feasibility Analysis

**Date:** 2026-03-23
**Data source:** All recorder + event data from S3 (March 21-22)
**Scope:** 8 losing trades from post-v2 session, full order book analysis

---

## Executive Summary

Stop-loss is **feasible but not guaranteed**. Across the 8 losses:
- **Every loss had at least 11 exit opportunities** where bid >= 0.50 with real depth
- Exit windows lasted **20-80 seconds** before the book collapsed
- But the book alternates between real and empty states (23-58% real), so the engine must act fast when a window appears
- FOK at scale will fail — partial/layered sells are essential

**Recommendation:** Implement stop-loss with IOC (Immediate or Cancel) partial fills at layered prices, not FOK. Monitor bid every 500ms, trigger at configurable threshold (0.80 default), sell in chunks.

---

## Per-Loss Analysis

### Loss 1: btc-updown-5m-1774152600 (NO side, BTC)

```
Entry: ~51s remaining, bought NO at 0.97
Outcome: Price reversed, NO resolved $0.00

Book evolution (our NO bids):
  tr=90  bid=0.74  spread=0.01  depth_at_80=0      ← real, but below 80
  tr=81  bid=0.70  spread=0.02  depth_at_80=0      ← fading
  tr=60  bid=0.79  spread=0.01  depth_at_80=0      ← brief recovery
  tr=51  bid=0.87  spread=0.01  depth_at_80=895    ← COULD EXIT HERE at 87c
  tr=45  bid=0.84  spread=0.01  depth_at_80=319    ← still tradeable
  tr=33  bid=0.84  spread=0.01  depth_at_80=249    ← last real chance
  tr=12  bid=0.39  spread=0.08  depth_at_80=0      ← too late, crashed
  tr=5   bid=0.24  spread=0.01  depth_at_80=0      ← gone

Exit windows: 23 snapshots where bid>=0.50 (tr=90s to tr=14s)
Best exit: 0.90 at tr=51s → loss would be $0.10 instead of $1.00
```

**Stop-loss at 0.80 would have saved:** If triggered at tr=51 (bid=0.87), sell at ~0.85.
Loss = $0.97 - $0.85 = **$0.12 instead of $1.00**

---

### Loss 2: btc-updown-5m-1774157100 (YES side, BTC)

```
Entry: ~46s remaining, bought YES at 0.97
Outcome: Price dropped below start, YES resolved $0.00

Book evolution (our YES bids):
  tr=90  bid=0.92  spread=0.01  depth_at_80=3702   ← excellent depth!
  tr=75  bid=0.93  spread=0.01  depth_at_80=3620   ← still great
  tr=63  bid=0.86  spread=0.01  depth_at_80=1335   ← dropping
  tr=57  bid=0.67  spread=0.01  depth_at_80=0      ← CRASHED through 80
  tr=51  bid=0.50  spread=0.04  depth_at_80=0      ← barely alive
  tr=42  bid=0.20  spread=0.01  depth_at_80=0      ← gone

Exit windows: 13 snapshots where bid>=0.50 (tr=90s to tr=51s)
Best exit: 0.93 at tr=75s
```

**CRITICAL FINDING:** Price crashed from 0.86 → 0.67 in **6 seconds** (tr=63 to tr=57).
A stop at 0.80 would have been **skipped over** — bid went from above 0.80 to below in one tick.

**Stop-loss at 0.80:** Would need to catch the tr=63 snapshot (bid=0.86) before the crash.
At 500ms tick rate, there's ~6 ticks in 3 seconds. Might catch it, might not.
**Worst case:** Sell at 0.67 = loss of $0.30.  **Best case:** Sell at 0.86 = loss of $0.11.
**Either way better than $1.00.**

---

### Loss 3: btc-updown-5m-1774159500 (NO side, BTC)

```
Book: 84% empty! Only 15 real-book snapshots out of 91.
Real-book avg bid: 0.737, depth at 80: 2261 shares (good when present)

  tr=72  bid=0.96  spread=0.01  ← highest point
  tr=39  bid=0.96  spread=0.02  ← still holding
  tr=12  bid=0.92  spread=0.03  ← still above 80!
  tr=9   bid=0.50  spread=0.05  ← sudden crash
  tr=6   bid=0.41  spread=0.004 ← done

Exit windows: 11 snapshots (tr=79s to tr=9s)
```

**This loss was VERY late — bid was 0.92 at tr=12 and crashed to 0.50 at tr=9.**
Stop at 0.80 would likely still be above threshold until the final 9 seconds.
**Result:** Probably save this one. Sell at ~0.90 = **loss $0.07 instead of $1.00.**

---

### Loss 4: btc-updown-5m-1774171200 (YES side, BTC)

```
  tr=87  bid=0.93  depth_at_80=5645  ← great depth early
  tr=69  bid=0.68  depth_at_80=0     ← CRASHED through 80
  tr=57  bid=0.85  depth_at_80=1342  ← bounced back!
  tr=51  bid=0.64  depth_at_80=0     ← crashed again
  tr=39  bid=0.66  ← never recovered above 0.80
  tr=15  bid=0.01  ← empty

Exit windows: 18 snapshots (tr=87s to tr=31s)
```

**Gap from 0.93 (tr=87) to 0.68 (tr=69) = 18 seconds.** Not instant. Multiple ticks to catch it.
Stop at 0.80 triggers around tr=72-69, sell at ~0.80.
**Loss = $0.17 instead of $1.00**

But there's a complication: bid bounced to 0.85 at tr=57 then crashed again. If stop triggered at 0.80 and didn't fill (empty book at that moment), the bounce might create a second chance.

---

### Loss 5: btc-updown-5m-1774176300 (YES side, BTC)

```
  tr=87  bid=0.83  spread=0.01  ← moderate
  tr=45  bid=0.80  spread=0.01  ← right at threshold
  tr=39  bid=0.79  spread=0.01  ← just below
  tr=36  bid=0.79  spread=0.02  ← holding
  tr=27  bid=0.79  spread=0.01  ← still close
  tr=12  bid=0.21  spread=0.01  ← collapsed
  tr=5   bid=0.09  ← gone

Exit windows: 18 snapshots (tr=87s to tr=20s)
```

**Slow bleed:** Price drifted from 0.83 → 0.79 over 60 seconds before collapsing.
Stop at 0.80 triggers around tr=39-36. Plenty of time.
**Loss = ~$0.17 instead of $1.00**

---

### Loss 6: sol-updown-15m-1774162800 (YES side, SOL)

```
  tr=90  bid=0.87  spread=0.02  depth_at_80=309
  tr=69  bid=0.90  spread=0.02  depth_at_80=276
  tr=63  bid=0.96  spread=0.01  depth_at_80=591  ← peak!
  tr=57-21: ALL EMPTY (bid=0.01, spread=0.98)   ← book vanished
  tr=4   bid=0.93  spread=0.06  ← brief flash
  tr=1   bid=0.06  ← resolved against us

Exit windows: 20 snapshots (tr=90s to tr=4s)
```

**Unique case:** Book was mostly empty (77%). When real, bids were ABOVE 0.80 until tr=63, then the book just vanished for 40+ seconds. Our position was fine (bid>0.80) when the book existed, but we couldn't sell during the empty window.

**Stop at 0.80:** Would NOT trigger during real-book ticks (bid was above 0.80). By the time bid dropped below 0.80, there was nothing to sell into. **This is the failure mode.**

**Mitigation:** "Sell if empty book persists for N seconds while we hold a position" — don't wait for price to drop, sell into ANY available bid if the book has been empty too long.

---

### Loss 7: sol-updown-5m-1774169400 (NO side, SOL)

```
  tr=81  bid=0.94  spread=0.01  depth_at_80=831
  tr=72  bid=0.58  spread=0.06  depth_at_80=0   ← crashed in 9 seconds
  tr=66  bid=0.47  ← below stop
  tr=45  bid=0.31  ← declining
  tr=21  bid=0.05  ← gone

Exit windows: 15 snapshots (tr=89s to tr=68s) — very narrow!
```

**Fast crash:** 0.94 → 0.58 in 9 seconds. Stop at 0.80 skipped over.
But at 500ms ticks, ~18 evaluations in 9 seconds. Some intermediate ticks likely had bid around 0.70-0.80.
**Worst case:** Sell at 0.58 = loss $0.39.  **Still better than $1.00.**

---

### Loss 8: sol-updown-5m-1774177200 (NO side, SOL)

```
  tr=90  bid=0.94  spread=0.02  depth_at_80=2004 ← great
  tr=78  bid=0.95  spread=0.01  depth_at_80=1585 ← peak
  tr=57  bid=0.92  spread=0.02  depth_at_80=544
  tr=48  bid=0.94  spread=0.01  depth_at_80=839  ← still strong
  tr=39  bid=0.68  spread=0.02  depth_at_80=0    ← dropped
  tr=33  bid=0.70  spread=0.10  depth_at_80=0    ← weakening
  tr=21  bid=0.46  ← below stop
  tr=9   bid=0.64  ← brief bounce
  tr=5   bid=0.34  ← done

Exit windows: 36 snapshots (tr=90s to tr=7s) — most of any loss!
```

**Gradual decline** from 0.94 to 0.68 over 30 seconds. Stop at 0.80 triggers around tr=42-39.
**Loss = ~$0.17 instead of $1.00**

---

## Summary Table

| Loss # | Slug | Side | Crash Speed | Stop@0.80 Would Save? | Est. Loss w/ Stop | Actual Loss |
|--------|------|------|-------------|----------------------|-------------------|-------------|
| 1 | btc-5m-152600 | NO | Slow (30s) | YES | ~$0.12 | $1.00 |
| 2 | btc-5m-157100 | YES | FAST (6s) | PARTIAL | ~$0.11-0.30 | $1.00 |
| 3 | btc-5m-159500 | NO | Late crash (3s) | YES | ~$0.07 | $1.00 |
| 4 | btc-5m-171200 | YES | Medium (18s) | YES | ~$0.17 | $1.00 |
| 5 | btc-5m-176300 | YES | Slow bleed (60s) | YES | ~$0.17 | $1.00 |
| 6 | sol-15m-162800 | YES | Book vanished | FAILED | ~$1.00 | $1.00 |
| 7 | sol-5m-169400 | NO | FAST (9s) | PARTIAL | ~$0.39 | $1.00 |
| 8 | sol-5m-177200 | NO | Medium (30s) | YES | ~$0.17 | $1.00 |

**Score: 5 full saves, 2 partial saves, 1 failure out of 8 losses.**

Estimated savings: $8.00 actual loss → ~$2.39 with stop-loss = **$5.61 saved**
Net P&L would flip from **-$4.06 to ~+$1.55**

---

## Failure Modes & Guardrails

### Failure Mode 1: Price Gaps Through Stop Level

**Problem:** Bid jumps from 0.86 to 0.67 in one tick — 0.80 stop never triggered at 0.80.

**Guardrail:** Don't use FOK. Use **IOC (Immediate or Cancel)** at a range:
```
Stop triggers at bid=0.67 (below 0.80 threshold)
  → Place IOC SELL at 0.67 (current bid)
  → Accepts any fill at 0.67 or better
  → Partial fills are OK — sell what you can
```

### Failure Mode 2: Empty Book (No Bids)

**Problem:** Book shows bid=0.01, spread=0.98. Nothing to sell into.

**Guardrails:**
1. **Empty book timer:** If book has been empty for N seconds while holding a position, sell into ANY available bid on the next real tick. Don't wait for price to drop.
2. **Time-based panic sell:** If `time_remaining < 10s` and still holding, sell at ANY price. Getting $0.30 is better than $0.00.

### Failure Mode 3: FOK Fails to Fill at Scale

**Problem:** With 100+ shares, FOK at 0.80 may not fill because depth at 0.80 is only 300-800 shares (varies). At $1/trade this is fine, but at scale it breaks.

**Guardrail: Layered IOC sells:**
```
Position: 100 shares of YES, stop triggered

Layer 1: IOC SELL 25 shares at 0.82  (above stop, try for better price)
Layer 2: IOC SELL 25 shares at 0.80  (at stop level)
Layer 3: IOC SELL 25 shares at 0.75  (sweeps deeper)
Layer 4: IOC SELL 25 shares at 0.60  (emergency floor)

If any layer partially fills, remaining goes to next layer.
Total: dump everything, accept slippage, don't let FOK kill the whole order.
```

Actual depth data supports this:

| Price Level | Avg Depth (real book) | Supports Shares |
|-------------|----------------------|-----------------|
| >= 0.80 | 300-3,000 shares | 1-100 easily |
| >= 0.70 | 1,000-7,000 shares | up to 500 |
| >= 0.50 | 2,000-15,000 shares | up to 1000 |
| Any bid | 8,000-50,000 shares | virtually unlimited |

At $1/trade (1 share), even thin books have enough. At $100/trade (100 shares), you need layered sells.

### Failure Mode 4: Stop-Loss Triggers on Noise, Misses Winning Trade

**Problem:** Bid briefly dips to 0.78, stop sells, price recovers and resolves at $1.00.

**Guardrails:**
1. **Only trigger on real books** (spread < 0.50). Don't trigger on empty book bid=0.01.
2. **Require N consecutive ticks below threshold** before triggering (e.g., 2 ticks = 1 second). Filters single-tick noise.
3. **Use a wider stop (0.70)** for markets with known oscillation (SOL).

### Failure Mode 5: Race Between Stop-Loss and Resolution

**Problem:** Stop triggers at tr=3s, order takes 1s to process, market resolves during execution.

**Guardrail:** Don't attempt stop-loss if `time_remaining < 5s`. At that point, the outcome is essentially decided. Trying to sell wastes gas and may fail anyway.

---

## Implementation Design

```
Engine tick loop (every 500ms):
│
├── Evaluate new trades (existing gates)
│
└── Monitor open positions:
    │
    ├── Get current bid for our side
    ├── Get spread (ask - bid)
    │
    ├── Is book empty? (spread > 0.50)
    │   ├── Yes → increment empty_book_counter[slug]
    │   │         if counter > empty_book_timeout_ticks:
    │   │           → PANIC: sell at any available bid on next real tick
    │   └── No  → reset empty_book_counter[slug]
    │
    ├── Is bid below stop_loss_price? AND spread < 0.50 (real book)?
    │   ├── Yes → increment stop_trigger_counter[slug]
    │   │         if counter >= min_stop_ticks (default 2):
    │   │           if time_remaining > min_stop_time_secs (default 5):
    │   │             → EXECUTE layered IOC sell
    │   └── No  → reset stop_trigger_counter[slug]
    │
    └── Is time_remaining < panic_sell_secs (default 5)?
        └── Yes → PANIC: sell at any available bid
```

## Config Fields Needed

```yaml
strategy:
  # Stop-loss
  stop_loss_price:
    BTC: 0.80
    ETH: 0.80
    SOL: 0.75         # SOL is noisier, wider stop
  stop_loss_confirm_ticks: 2          # require 2 consecutive ticks below threshold
  stop_loss_min_time_remaining: 5     # don't attempt if <5s left
  stop_loss_empty_book_timeout_s: 10  # panic sell if book empty for 10s while holding
  stop_loss_layers: 4                 # split sell into N layers
  stop_loss_layer_spread: 0.05        # each layer 5c apart (0.82, 0.77, 0.72, 0.67)
  stop_loss_panic_time_s: 5           # panic sell at any price if <5s remaining
```

## Projected Impact

With stop-loss applied to the 8 March 22 losses:

```
Without stop:  65 wins × $0.06 = $3.94,  8 losses × $1.00 = $8.00 → PnL = -$4.06
With stop:     65 wins × $0.06 = $3.94,  8 losses × $0.30 = $2.39 → PnL = +$1.55

(Using $0.30 avg loss estimate: 5 saves at ~$0.15, 2 partial at ~$0.35, 1 fail at $1.00)
```

**Net swing: +$5.61 improvement.**
