# Strategy V2: 5 Improvements for Higher Win Rate at 95c Entry

**Date:** 2026-03-22
**Branch:** `temp/pm-tracker-integration`
**Motivation:** Live trading on March 21 showed 3 direction losses and thin margins. Root causes: overpaying at 97-99c ask, PM confidence oscillating on thin books, momentum reversals, betting against Binance flow, and a static depth gate that didn't adapt to volume.

---

## Table of Contents

1. [Gate Sequence Overview](#gate-sequence-overview)
2. [Change 1: Max Entry Price Cap](#change-1-max-entry-price-cap)
3. [Change 2: Sustained PM Confidence](#change-2-sustained-pm-confidence)
4. [Change 3: Gap Trend Gate](#change-3-gap-trend-gate)
5. [Change 4: Flow Agreement Gate](#change-4-flow-agreement-gate)
6. [Change 5: Dynamic Depth](#change-5-dynamic-depth)
7. [New Signals Reference](#new-signals-reference)
8. [Config Reference](#config-reference)
9. [Files Modified](#files-modified)
10. [Verification Checklist](#verification-checklist)

---

## Gate Sequence Overview

The engine evaluates each active market on a 500ms tick. A trade must pass **every** gate in order. New gates are marked with `*`.

```
                          ┌─────────────────────────┐
                          │   Active Market Window   │
                          └────────────┬────────────┘
                                       │
                          ┌────────────▼────────────┐
                          │  gate1: time_window      │  30-90s before resolution
                          └────────────┬────────────┘
                                       │ PASS
                          ┌────────────▼────────────┐
                          │  gate2: no position      │  no existing position or
                          │  gate2b: no in-flight    │  pending order for this slug
                          └────────────┬────────────┘
                                       │ PASS
                       ┌───────────────▼───────────────┐
                       │  gate3: pm_confidence         │  * now uses min(conf) over
                       │  (sustained over 10s window)  │    the last 10 seconds
                       └───────────────┬───────────────┘
                                       │ PASS
                       ┌───────────────▼───────────────┐
                       │  gate4: cl_binance_agreement  │  CL & Binance on same side,
                       │                               │  divergence < threshold
                       └───────────────┬───────────────┘
                                       │ PASS
                       ┌───────────────▼───────────────┐
                       │  gate4b: gap_trend *          │  gap must not be shrinking
                       │  (reject fading momentum)     │  (ratio >= 0.8)
                       └───────────────┬───────────────┘
                                       │ PASS
                       ┌───────────────▼───────────────┐
                       │  gate4c: flow_agreement *     │  Binance trade flow must
                       │  (reject counter-flow bets)   │  not strongly oppose bet
                       └───────────────┬───────────────┘
                                       │ PASS
                       ┌───────────────▼───────────────┐
                       │  gate5a: min_gap_pct          │  gap as % of start price
                       │  gate5: gap > N x exp_move    │  gap vs expected move
                       │  gate5: depth (dynamic) *     │  depth ratio OR static floor
                       └───────────────┬───────────────┘
                                       │ PASS
                       ┌───────────────▼───────────────┐
                       │  gate6: liquidation_cascade   │  no active liquidations
                       └───────────────┬───────────────┘
                                       │ PASS
                               ┌───────▼───────┐
                               │  EXECUTE TRADE │───► price capped at 0.95 *
                               └───────────────┘
```

---

## Change 1: Max Entry Price Cap

### Why

The PM order book typically has **thousands of shares at 0.95** but the best ask sits at 0.97-0.99. Before this change, the engine set the FOK price ceiling to `PMConfidence` (the ask), meaning it paid 97-99c per share. On a winning trade that redeems at $1.00, that's only 1-3c profit per share. A single loss wipes out many wins.

By capping at 0.95, the FOK order only fills against the deep 95c liquidity. Profit per win jumps to 5c/share, giving much more room to absorb occasional losses.

### How It Works

```
Before:                              After:
┌──────────────────────┐             ┌──────────────────────┐
│  PM Order Book       │             │  PM Order Book       │
│                      │             │                      │
│  Ask  0.99  50 shr   │ ◄── old    │  Ask  0.99  50 shr   │    skipped
│  Ask  0.97  120 shr  │     fill    │  Ask  0.97  120 shr  │    skipped
│  Ask  0.95  3000 shr │             │  Ask  0.95  3000 shr │ ◄── new fill
│                      │             │         ▲            │
│  priceCeiling = 0.99 │             │  priceCeiling = 0.95 │
└──────────────────────┘             └──────────────────────┘

Win profit:  $1.00 - $0.99 = $0.01   Win profit:  $1.00 - $0.95 = $0.05
             (1% margin)                           (5.3% margin)
```

### Code Path

```
executeLiveTrade()                          engine.go:277-280
    │
    ├── priceCeiling = sig.PMConfidence     (was the only line)
    │
    └── if MaxEntryPrice[symbol] > 0 && MaxEntryPrice[symbol] < priceCeiling:
            priceCeiling = MaxEntryPrice[symbol]    ◄── NEW
```

### Config

```yaml
strategy:
  max_entry_price:
    BTC: 0.95
    ETH: 0.95
    SOL: 0.95
```

### Backwards Compatibility

If `max_entry_price` is absent or the symbol has no entry, `symbolFloat()` returns 0, the condition `maxEntry > 0` is false, and the cap is skipped. Existing behavior is preserved.

---

## Change 2: Sustained PM Confidence

### Why

The Polymarket order book is thin. A single large order appearing and disappearing causes the ask price (our confidence signal) to oscillate wildly:

```
Time (seconds)     PM Confidence (YES ask)
─────────────      ──────────────────────
  t+0              0.95   ✓ above threshold
  t+0.5            0.51   ✗ someone pulled liquidity
  t+1.0            0.97   ✓ liquidity returned
  t+1.5            0.48   ✗ again
  t+2.0            0.99   ✓ spike — old engine would trade HERE
```

A single-tick sample catches a fleeting spike and enters a trade when the market is actually uncertain. The sustained confidence window requires the **minimum** confidence over the last N seconds to exceed the threshold.

### How It Works

```
            10-second rolling window
    ◄──────────────────────────────────►
    ┌──┬──┬──┬──┬──┬──┬──┬──┬──┬──┬──┐
    │.95│.97│.51│.96│.99│.93│.95│.97│.98│.96│.95│  ◄── raw PM conf per tick
    └──┴──┴──┴──┴──┴──┴──┴──┴──┴──┴──┘
                 ▲
                 │
           min = 0.51  ──► SustainedPMConf = 0.51
                           Gate 3 rejects (< 0.95 threshold)

    ... later, after the dip clears ...

    ┌──┬──┬──┬──┬──┬──┬──┬──┬──┬──┬──┐
    │.96│.99│.93│.95│.97│.98│.96│.95│.97│.96│.95│
    └──┴──┴──┴──┴──┴──┴──┴──┴──┴──┴──┘
                 ▲
                 │
           min = 0.93  ──► still rejected

    ... confidence stabilizes ...

    ┌──┬──┬──┬──┬──┬──┬──┬──┬──┬──┬──┐
    │.95│.97│.96│.95│.97│.98│.96│.95│.97│.96│.95│
    └──┴──┴──┴──┴──┴──┴──┴──┴──┴──┴──┘
                 ▲
                 │
           min = 0.95  ──► SustainedPMConf = 0.95
                           Gate 3 passes ✓
```

### Code Path

**Engine state** (`engine.go:28, 50`):
```go
confHistory map[string][]confEntry   // slug → [{Timestamp, Conf}, ...]
```

**Recording + min calculation** (`engine.go:62-85`):
```
recordAndGetSustainedConf(slug, conf, now)
    │
    ├── if SustainedConfWindowSecs == 0 → return conf (disabled)
    │
    ├── append {now, conf} to confHistory[slug]
    ├── prune entries older than window
    └── return min(conf) over remaining entries
```

**Called from** (`engine.go:129`):
```
evaluateMarket()
    └── signals.SustainedPMConf = recordAndGetSustainedConf(slug, PMConfidence, now)
```

**Gate 3 modification** (`engine.go:183-190`):
```
confToCheck = SustainedPMConf
if confToCheck == 0:           ◄── fallback if sustained conf not yet computed
    confToCheck = PMConfidence
if confToCheck < MinPMConfidence[symbol]:
    reject "gate3_pm_confidence"
```

**Cleanup** (`engine.go:114-119`): After each tick, `confHistory` entries for slugs no longer in `GetActiveWindowSummaries()` are deleted to prevent memory leaks.

### Config

```yaml
strategy:
  sustained_conf_window_secs: 10    # 0 = disabled, use raw PMConfidence
```

---

## Change 3: Gap Trend Gate

### Why

The gap (current price - start price) was checked at a single point in time. But in the March 21 losses, the gap was **already shrinking** at the moment the engine entered. The price was reverting back toward start, and the trade lost because the gap closed entirely before resolution.

```
          BTC Price vs Start Price (67,200)

  67,260 ┤         ╭──╮
         │        ╭╯  ╰╮
  67,240 ┤      ╭─╯    ╰╮
         │     ╭╯       ╰──╮
  67,220 ┤   ╭─╯       ▲   ╰──╮
         │  ╭╯         │      ╰──╮
  67,200 ┤──╯  gap     │ gap     ╰──── start_price
         │  growing    shrinking
         │             │
         │        old engine          reversion completes,
         │        entered HERE        market resolves NO
         │
         ├────┬────┬────┬────┬────┬────┤
         t-60 t-50 t-40 t-30 t-20 t-10  t=0 (resolution)
```

The gap trend gate computes the ratio of current gap to the gap N seconds ago. A ratio < 1.0 means the gap is shrinking (fading momentum). The gate rejects trades where this ratio is below the configured minimum (0.8).

### How It Works

```
GapTrend = gap_now / gap_10s_ago

Example A (healthy trend):
    gap_10s_ago = +$30     gap_now = +$45
    GapTrend = 45/30 = 1.50   ──► PASS (> 0.8)

Example B (fading momentum):
    gap_10s_ago = +$50     gap_now = +$35
    GapTrend = 35/50 = 0.70   ──► REJECT gate4b_gap_fading (< 0.8)

Example C (gap flipped sign):
    gap_10s_ago = +$20     gap_now = -$5
    GapTrend = 0 (sign mismatch) ──► gate skipped (other gates handle)
```

### Code Path

**Signal computation** (`signals.go:49-61`):
```
pastTicks = clState.Ticks.GetTicksSince(now - lookback)
gapPrev   = pastTicks[0].Price - startPrice

if same sign(gap, gapPrev):
    GapTrend = gap / gapPrev
else:
    GapTrend = 0   (skip gate)
```

**Gate** (`engine.go:197-202`):
```
if MinGapTrend[symbol] > 0 AND GapTrend > 0:
    if GapTrend < MinGapTrend[symbol]:
        reject "gate4b_gap_fading"
```

### Config

```yaml
strategy:
  min_gap_trend:
    BTC: 0.8      # gap must be at least 80% of what it was 10s ago
    ETH: 0.8
    SOL: 0.8
  gap_trend_lookback_s: 10
```

---

## Change 4: Flow Agreement Gate

### Why

On March 21, the engine placed bets **against the Binance trade flow**. For example, it bet YES (price up) while net Binance flow was strongly selling. Binance is the most liquid BTC market — when its trade flow disagrees with our bet direction, the chance of reversal is high.

```
    Scenario: Engine bets YES (price will stay above start)

    Binance Trade Flow:
    ┌──────────────────────────────────────────┐
    │  sell ◄════════════════════╗              │
    │                            ║ net flow     │
    │                            ║ = -0.5       │
    │                 buy ═══════╝              │
    │                                          │
    │  FlowMomentum = -0.5  (strong selling)   │
    │  WinningSide  = YES                      │
    │  MaxFlowDisagreement = 0.3               │
    │                                          │
    │  -0.5 < -0.3  ──► REJECT gate4c         │
    └──────────────────────────────────────────┘

    Without this gate, the engine would have entered
    and likely lost as selling pressure pushed price
    back below start_price.
```

### How It Works

`FlowMomentum` is a normalized value from `-1.0` (all sells) to `+1.0` (all buys), computed by the Binance spot connector from recent aggregate trades.

| Bet Side | FlowMomentum | MaxFlowDisagreement | Result |
|----------|-------------|---------------------|--------|
| YES (up) | +0.4 (buying) | 0.3 | PASS (flow agrees) |
| YES (up) | -0.1 (light selling) | 0.3 | PASS (not strong enough) |
| YES (up) | -0.5 (heavy selling) | 0.3 | **REJECT** (-0.5 < -0.3) |
| NO (down) | -0.4 (selling) | 0.3 | PASS (flow agrees) |
| NO (down) | +0.5 (heavy buying) | 0.3 | **REJECT** (+0.5 > +0.3) |

### Code Path

**Signal** (`signals.go:64`):
```go
flowMomentum := spotState.FlowMomentum()
```

**Gate** (`engine.go:204-212`):
```
if MaxFlowDisagreement[symbol] > 0:
    if WinningSide == "YES" AND FlowMomentum < -maxDisagree → reject
    if WinningSide == "NO"  AND FlowMomentum > +maxDisagree → reject
```

### Config

```yaml
strategy:
  max_flow_disagreement:
    BTC: 0.3    # reject if |opposing flow| > 0.3
    ETH: 0.3
    SOL: 0.3
```

---

## Change 5: Dynamic Depth

### Why

The old static `min_depth` gate used a fixed BTC threshold (0.5 BTC). This didn't scale:
- During quiet periods, 0.5 BTC of depth is substantial and the gate was too loose.
- During volatile periods with high volume, 0.5 BTC is nothing and the gate was too strict.

The new dynamic depth gate asks: **"Is there enough order book depth to resist the expected trading volume for the remaining time?"**

### How It Works

```
                    Liquidity needed to flip
                    price back past start_price
                    (converted to USD)
                           │
  DepthRatio  =  ──────────────────────────────
                    Expected volume remaining
                    in this market window


  Expected volume remaining = volume_per_sec  x  time_remaining_s
  volume_per_sec            = FlowIntensity30s / 30


  Example:

  ┌─────────────────────────────────────────────────────────────┐
  │  LiquidityToFlip     = 0.8 BTC                             │
  │  CL Price            = $67,200                              │
  │  LiquidityToFlipUSD  = 0.8 x 67,200 = $53,760             │
  │                                                             │
  │  FlowIntensity30s    = $900,000 (volume in last 30s)       │
  │  VolumePerSec        = 900,000 / 30 = $30,000/s            │
  │  TimeRemainingS      = 45s                                  │
  │  ExpectedVolRemaining= 30,000 x 45 = $1,350,000            │
  │                                                             │
  │  DepthRatio          = 53,760 / 1,350,000 = 0.04           │
  │  MinDepthRatio       = 1.5                                  │
  │                                                             │
  │  0.04 < 1.5  ──► REJECT gate5_depth_ratio_too_low          │
  │                                                             │
  │  Interpretation: The volume expected in the remaining 45s   │
  │  is 25x the depth protecting the start price. High risk     │
  │  of price flipping back.                                    │
  └─────────────────────────────────────────────────────────────┘
```

```
  Depth Ratio Spectrum:

  0.0          0.5          1.0          1.5          2.0+
  ├────────────┼────────────┼────────────┼────────────┤
  │  DANGEROUS │   RISKY    │  MARGINAL  │    SAFE    │
  │            │            │            │            │
  │  volume >> │ volume >   │ roughly    │ depth >>   │
  │  depth     │ depth      │ balanced   │ volume     │
  │            │            │            │            │
  │ ◄──── REJECT ──────────────────────► │ ◄─ PASS ─►│
  │        (< 1.5)                       │   (>= 1.5)│
```

### Fallback

When volume data is unavailable (`DepthRatio == 0` or `MinDepthRatio` not configured), the gate falls back to the static `min_depth` floor. The static floor values have been **lowered** since they're now just a last-resort safety net:

| Symbol | Old `min_depth` | New `min_depth` (fallback) |
|--------|----------------|---------------------------|
| BTC | 0.5 BTC | 0.01 BTC |
| ETH | 50.0 ETH | 1.0 ETH |
| SOL | 50.0 SOL | 1.0 SOL |

### Code Path

**Signals** (`signals.go:63-77`):
```
flowIntensity30s         = spotState.FlowIntensity30s()
volumePerSec             = flowIntensity30s / 30
liquidityToFlipUSD       = liquidityToFlip * clPrice
expectedVolumeRemaining  = volumePerSec * timeRemainingS
depthRatio               = liquidityToFlipUSD / expectedVolumeRemaining
```

**Gate** (`engine.go:234-241`):
```
if MinDepthRatio[symbol] > 0 AND DepthRatio > 0:
    if DepthRatio < MinDepthRatio[symbol]:
        reject "gate5_depth_ratio_too_low"
else:
    if LiquidityToFlip < MinDepth[symbol]:       ◄── static fallback
        reject "gate5_depth_floor"
```

### Config

```yaml
strategy:
  min_depth_ratio:
    BTC: 1.5
    ETH: 1.5
    SOL: 1.5
  min_depth:          # lowered — now just a fallback floor
    BTC: 0.01
    ETH: 1.0
    SOL: 1.0
```

---

## New Signals Reference

All new fields added to `MarketSignals` (`internal/engine/types.go`):

| Signal | Type | Source | Description |
|--------|------|--------|-------------|
| `SustainedPMConf` | float64 | Engine state | Min PM confidence over last N seconds |
| `GapTrend` | float64 | Chainlink ticks | `gap_now / gap_Ns_ago` (0 if sign flipped) |
| `FlowMomentum` | float64 | Binance spot | Net trade flow, -1 (all sells) to +1 (all buys) |
| `FlowIntensity30s` | float64 | Binance spot | Total trade volume in last 30 seconds |
| `VolumePerSec` | float64 | Derived | `FlowIntensity30s / 30` |
| `LiquidityToFlipUSD` | float64 | Derived | `LiquidityToFlip * CLPrice` |
| `ExpectedVolumeRemaining` | float64 | Derived | `VolumePerSec * TimeRemainingS` |
| `DepthRatio` | float64 | Derived | `LiquidityToFlipUSD / ExpectedVolumeRemaining` |

All appear in SKIP/TRADE log lines (zerolog fields: `sustained_pm_conf`, `gap_trend`, `flow_momentum`, `depth_ratio`, `volume_per_sec`).

---

## Config Reference

New fields added to `strategy:` section in `configs/config.yaml`:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_entry_price` | map[string]float64 | absent (disabled) | Cap FOK price ceiling per symbol |
| `sustained_conf_window_secs` | int | 0 (disabled) | Rolling window for min PM confidence |
| `min_gap_trend` | map[string]float64 | absent (disabled) | Min gap ratio to pass gate4b |
| `gap_trend_lookback_s` | int | 10 | How far back to look for previous gap |
| `max_flow_disagreement` | map[string]float64 | absent (disabled) | Max opposing flow momentum before rejection |
| `min_depth_ratio` | map[string]float64 | absent (disabled) | Min depth/volume ratio for gate5 |

All are **backwards-compatible** — absent or zero values disable the feature and fall back to previous behavior.

---

## Files Modified

```
internal/config/types.go       6 new fields on StrategyConfig
internal/engine/types.go       8 new fields on MarketSignals, new confEntry type
internal/engine/signals.go     2 new params on ComputeSignals, 4 new signal blocks
internal/engine/engine.go      recordAndGetSustainedConf method, tick cleanup,
                               3 new gates (4b, 4c, 5-dynamic), price cap,
                               5 new log fields
internal/backtest/replay.go    Updated ComputeSignals call (pass 0 and snapTime)
configs/config.yaml            6 new config entries, lowered min_depth
```

No changes to: tracker, state store, connectors, or `ActiveWindowSummary`.

---

## Verification Checklist

- [ ] `go build ./...` compiles clean
- [ ] `go vet ./...` passes
- [ ] `go test ./...` has no regressions
- [ ] Deploy in paper mode, confirm new log fields appear:
  - `sustained_pm_conf`, `gap_trend`, `flow_momentum`, `depth_ratio`, `volume_per_sec`
- [ ] Check new gate rejection reasons in SKIP logs:
  - `gate4b_gap_fading`
  - `gate4c_flow_disagrees`
  - `gate5_depth_ratio_too_low`
  - `gate5_depth_floor` (replaces old `gate5_depth_too_thin`)
- [ ] Confirm FOK orders use capped price:
  - Check `price_ceiling` in `engine: live trade filled` log lines (should be <= 0.95)
- [ ] Run 1-2 hours in live mode with $1 trades
- [ ] Compare gate rejection distribution vs previous version
