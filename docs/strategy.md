# Late-Entry Confidence Trading Strategy

## Overview

Trade BTC Up/Down 5-minute and 15-minute Polymarket prediction markets by entering only in the final 60-120 seconds when the outcome is nearly certain. Use Binance order-book depth and Chainlink price data to confirm the outcome will not flip before resolution.

**Target**: $0.03-$0.05 net per dollar risked, ~288 5m + ~96 15m opportunities per day.
**Required win rate**: >97% to be profitable at these margins.

---

## Resolution Rule

A market resolves "Up" if the Chainlink BTC/USD price at the window end timestamp is **>=** the Chainlink price at the window start timestamp. The "price to beat" (`start_price`) is recorded when the market is first registered.

---

## Entry Gates (evaluated in order)

All 6 gates must pass for a trade to be entered.

| # | Gate | Condition to PASS |
|---|---|---|
| 1 | **Time window** | `60 <= time_remaining_secs <= 120` |
| 2 | **No existing position** | No open position in this market |
| 3 | **PM confidence** | Winning side's best bid > 0.95 |
| 4 | **CL-Binance agreement** | Both CL and Binance mid on same side of `start_price`, AND `\|binance_mid - cl_price\| / cl_price < 0.05%` |
| 5 | **Depth safety** | `liquidity_to_flip(start_price) > 2.0 × expected_move` |
| 6 | **No liquidation cascade** | `BinancePerpsState.LiquidationFlag == false` |

If any gate fails, the decision is logged as `SKIP` with the failing gate name.

---

## Signal Definitions

- **`WinningSide`**: `"YES"` if `cl_price >= start_price`, else `"NO"`.
- **`PMConfidence`**: Best bid of the winning side on Polymarket CLOB.
- **`DivergencePct`**: `|binance_mid - cl_price| / cl_price * 100`.
- **`CLBinanceAgree`**: `true` if both CL price and Binance mid are on the same side of `start_price`.
- **`LiquidityToFlip`**: Sum of order-book notional between current Binance mid and `start_price` (BTC units).
- **`ExpectedMove`**: Estimated BTC price move over `time_remaining` seconds, computed as `stddev(per-tick returns over last 180 CL ticks) * sqrt(time_remaining) * cl_price`.

---

## Data Sources

| Signal | Source |
|---|---|
| `start_price` | `ActiveMarket.StartPrice` (Chainlink REST at window open) |
| `cl_price` | `ChainlinkState.LatestPrice` |
| CL tick ring buffer | `ChainlinkState.Ticks` (180 ticks, ~3 min) |
| `binance_mid` | `BinanceSpotState.OrderBook.MidPrice` |
| Order-book depth | `BinanceSpotState.LiquidityToPrice(start_price)` |
| Liquidation flag | `BinancePerpsState.LiquidationFlag` |
| YES/NO prices | `ActiveWindowSummary.YesBid`, `NoBid` |
| Time remaining | `ActiveWindowSummary.TimeRemainingS` |

---

## Paper Trading

Phase 1 is paper-only. All decisions are logged but no real orders are placed.

- `SKIP` log: all 6 signals + `reject_reason` (first failed gate).
- `PAPER_TRADE` log: all 6 signals + NDJSON event written via persistence.
- Paper trades are stored on the `ActiveMarket.PaperTrades` slice and accumulate until market resolution.
- On resolution, `CompletedMarket.PnL` reflects what the paper trade would have earned.

---

## Configuration

```yaml
strategy:
  enabled: false                       # set true to activate
  entry_window_min_secs: 60
  entry_window_max_secs: 120
  min_pm_confidence: 0.95
  max_cl_binance_divergence_pct: 0.05  # percent
  depth_safety_multiplier: 2.0
  max_shares_per_trade: 200
  max_concurrent_positions: 5
```

---

## Out of Scope (Phase 1)

- Real order execution
- Historical backtest data download
- Circuit breaker (auto-pause after N losses)
- Multi-asset support validation
- Limit order / early-exit strategies
- Per-window-type parameter tuning
