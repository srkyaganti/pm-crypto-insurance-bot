# Backtester

The backtester replays recorded state snapshots (NDJSON) through the engine's signal computation and gate logic. It evaluates multiple strategy configs in a single pass over the data, avoiding redundant I/O.

## Build & Run

```bash
go build -o bin/backtest ./cmd/backtest
./bin/backtest -config ./configs/config.yaml -addr :8081
# Override data directory:
./bin/backtest -config ./configs/config.yaml -data-dir ./logs/recorder
```

The server loads strategy defaults from `config.yaml`. Each sweep request can override individual fields per config.

## API Reference

### `GET /data`

List available data files.

**Query params**: `data_dir` (optional, overrides default)

**Response**:
```json
{
  "files": [
    {"name": "data-2026-03-19-08.ndjson", "size_mb": 45.2, "hour": "2026-03-19-08"},
    {"name": "data-2026-03-19-09.ndjson", "size_mb": 48.1, "hour": "2026-03-19-09"}
  ],
  "earliest": "2026-03-19-08",
  "latest": "2026-03-19-09"
}
```

### `POST /sweep`

Run multiple strategy configs over a time range in a single pass.

**Request**:
```json
{
  "from": "2026-03-19T08:00:00Z",
  "to": "2026-03-19T09:00:00Z",
  "data_dir": "./logs/recorder",
  "configs": [
    {
      "id": "aggressive",
      "min_pm_confidence": {"BTC": 0.80, "ETH": 0.80},
      "depth_safety_multiplier": {"BTC": 1.0}
    },
    {
      "id": "conservative",
      "min_pm_confidence": {"BTC": 0.95},
      "entry_window_min_secs": 90,
      "entry_window_max_secs": 110
    }
  ]
}
```

**Config override fields** (all optional — defaults from config.yaml):

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | **Required**. Unique identifier for this config variant. |
| `entry_window_min_secs` | int | Minimum seconds before resolution to enter a trade. |
| `entry_window_max_secs` | int | Maximum seconds before resolution to enter a trade. |
| `min_pm_confidence` | map[symbol]float64 | Minimum winning-side bid on Polymarket (per symbol). |
| `max_cl_binance_divergence_pct` | map[symbol]float64 | Max allowed Chainlink-Binance price divergence %. |
| `depth_safety_multiplier` | map[symbol]float64 | Gap must be >= multiplier x expected_move. |
| `min_depth` | map[symbol]float64 | Minimum BTC liquidity to flip in the order book. |
| `max_shares_per_trade` | int | Number of shares per simulated trade. |

**Response**:
```json
{
  "metadata": {
    "from": "2026-03-19T08:00:00Z",
    "to": "2026-03-19T09:00:00Z",
    "files_processed": 1,
    "file_names": ["data-2026-03-19-08.ndjson"],
    "total_snapshots": 3600,
    "processing_time_ms": 1234
  },
  "results": {
    "aggressive": {
      "trades": [...],
      "summary": {
        "total_snapshots": 3600,
        "total_evaluations": 36000,
        "total_trades": 12,
        "trades_by_symbol": {"BTC": 8, "ETH": 4},
        "rejection_counts": {"gate1_time_window": 30000, ...},
        "simulated_pnl": 15.50,
        "wins": 9,
        "losses": 3,
        "unresolved": 0,
        "win_rate": 0.75,
        "avg_entry_price": 0.88,
        "avg_pnl_per_trade": 1.29,
        "best_trade_pnl": 3.60,
        "worst_trade_pnl": -2.64,
        "best_trade_slug": "btc-updown-5min-...",
        "worst_trade_slug": "btc-updown-15min-...",
        "trades_by_window": {"5m": 8, "15m": 4},
        "gate_pass_rates": {
          "gate1_time_window": 0.17,
          "gate2_existing_position": 0.99,
          ...
        },
        "config_used": {
          "entry_window_min_secs": 60,
          "min_pm_confidence": {"BTC": 0.80, "ETH": 0.80},
          ...
        }
      }
    },
    "conservative": { ... }
  },
  "ranking": [
    {"config_id": "aggressive", "pnl": 15.50, "win_rate": 0.75, "trades": 12},
    {"config_id": "conservative", "pnl": 5.20, "win_rate": 0.80, "trades": 5}
  ]
}
```

## Strategy Gates

The engine evaluates 6 sequential gates for each market at each snapshot:

1. **gate1_time_window** — Is `time_remaining_s` between `entry_window_min_secs` and `entry_window_max_secs`?
2. **gate2_existing_position** — Do we already have an open position on this slug?
3. **gate3_pm_confidence** — Is the winning side's Polymarket bid >= `min_pm_confidence[symbol]`?
4. **gate4_cl_binance_agreement** — Do Chainlink and Binance agree on the winning side? Is divergence within threshold?
5. **gate5_gap/depth** — Is |gap| >= `depth_safety_multiplier[symbol]` x expected_move? Is liquidity >= `min_depth[symbol]`?
6. **gate6_liquidation_cascade** — Is the Binance perps liquidation flag inactive?

## Output Fields

### Trade entry
- `entry_price` — Ask price on the winning side at trade time (cost per share)
- `pnl` — `(1 - entry_price) * shares` if won, `-(entry_price * shares)` if lost
- `signals` — Full snapshot of computed signals at trade time

### Summary verbose fields
- `win_rate` — wins / (wins + losses), excludes unresolved
- `avg_entry_price` — mean entry price across all trades
- `gate_pass_rates` — for each gate, what fraction of evaluations reaching that gate passed it
- `trades_by_window` — trade count bucketed by window duration (5m, 15m)
- `config_used` — the fully merged strategy config (defaults + overrides)

## Example: Parameter Sweep

```bash
curl -X POST http://localhost:8081/sweep \
  -H 'Content-Type: application/json' \
  -d '{
    "from": "2026-03-19T08:00:00Z",
    "to": "2026-03-19T20:00:00Z",
    "configs": [
      {"id": "pm80", "min_pm_confidence": {"BTC": 0.80}},
      {"id": "pm85", "min_pm_confidence": {"BTC": 0.85}},
      {"id": "pm90", "min_pm_confidence": {"BTC": 0.90}}
    ]
  }'
```

Check the `ranking` array to see which confidence threshold yielded the best PnL.

## S3 Archival

The `tools/archive-recorder.sh` script uploads completed NDJSON files to S3 and removes local copies.

```bash
# One-off
./tools/archive-recorder.sh ./logs/recorder s3://pm-recorder-data/logs/recorder

# Cron (hourly)
0 * * * * /path/to/archive-recorder.sh ./logs/recorder s3://pm-recorder-data/logs/recorder

# Pull data back for backtesting
aws s3 sync s3://pm-recorder-data/logs/recorder ./logs/recorder
```

## Architecture Notes

- **Single-pass design**: Data files are read once. For each NDJSON snapshot, market data is converted and signals are computed once per market. Only the per-config gate evaluation (cheap arithmetic comparisons) is repeated N times.
- **Memory**: One snapshot parsed at a time (~few KB). Per-config overhead is minimal (~evaluator + positions map + trades slice). 100 configs with ~500 trades each uses ~15MB.
- **I/O savings**: 100 configs over 3.5GB of data = 3.5GB read (not 350GB with 100 separate requests).
- **Signal sharing**: `ComputeSignals()` depends only on price/book data, not strategy config. `HasPosition` is only checked in `runGates()` gate 2, so signals can be computed once and shared across all configs.
