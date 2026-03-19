# StateStore Recorder for Backtesting Data Collection

## Purpose

Records live StateStore snapshots (Chainlink prices, Binance order books, Binance perps, Polymarket markets) every second to NDJSON files. This captures data that isn't available historically via APIs — needed for building a backtester with 3-5 days of data.

## Changes Made

### Created: `internal/recorder/recorder.go`

New package. 1-second ticker loop captures full StateStore + active PM market summaries to NDJSON.

- **Snapshot schema**: `RecorderSnapshot` contains timestamp, per-symbol data (Chainlink price + all ring buffer ticks, Binance spot full 20-level order book, Binance perps mark price + liquidation flag), and all active PM markets (slug, symbol, start price, time remaining, yes/no bid/ask).
- **Buffered I/O**: `bufio.Writer` wrapping `os.File`, flushed every 5 captures and on rotation/shutdown.
- **Hourly file rotation**: Files named `data-YYYY-MM-DD-HH.ndjson`. New file opened when UTC hour changes.
- **Lifecycle**: `Run(ctx)` respects context cancellation. `Stop()` flushes and closes.
- **Constructor**: `New(store, getWindows, symbols, interval, outputDir, logger)` — creates output dir if needed.

### Modified: `internal/config/types.go`

Added `RecorderConfig` struct:
```go
type RecorderConfig struct {
    Enabled      bool   `yaml:"enabled"`
    OutputDir    string `yaml:"output_dir"`
    IntervalSecs int    `yaml:"interval_secs"`
}
```
Added `Recorder RecorderConfig \`yaml:"recorder"\`` field to `Config` struct.

### Modified: `configs/config.yaml`

- Added `recorder:` section (enabled, output to `./logs/recorder`, 1s interval).
- Set `strategy.enabled: false` (paper trading off during data collection).
- Enabled all 6 markets: BTC, ETH, SOL, XRP, DOGE, BNB (each with 5m + 15m windows).

### Modified: `internal/app/app.go`

- Imported `internal/recorder` package.
- Added `recorder *recorder.Recorder` field to `App` struct.
- In `New()`: initializes recorder after tracker when `cfg.Recorder.Enabled && app.trackerConn != nil`. Uses `trackerConn.GetActiveWindowSummaries()` callback and `cfg.Symbols()` helper.
- In `runLoop()`: starts recorder as supervised goroutine via `supervisor.Add/Done` + `ctx.Done()` (same pattern as decision engine).

## Data Format

Each line is a JSON object:
```json
{
  "ts": 1773907565004,
  "ts_iso": "2026-03-19T08:06:05.004075Z",
  "symbols": {
    "BTC": {
      "chainlink": { "price": 70144.92, "stale": false, "ticks": [{"price": ..., "ts": ...}, ...] },
      "binance_spot": { "mid": 70145.74, "best_bid": ..., "best_ask": ..., "bids": [...20 levels...], "asks": [...20 levels...], "stale": false },
      "binance_perps": { "mark_price": 70099.11, "liquidation_flag": false, "stale": false }
    },
    "ETH": { ... },
    "SOL": { ... }
  },
  "markets": [
    { "slug": "btc-updown-5m-...", "symbol": "BTC", "start_price": 70118.04, "time_remaining_s": 234, "yes_bid": 0.55, "yes_ask": 0.58, "no_bid": 0.42, "no_ask": 0.45 }
  ]
}
```

## Disk Usage

~15-20MB/hour with 6 symbols. ~360-480MB/day. 5 days = ~1.5-2.5GB.

## Running

```bash
go build -o bin/engine ./cmd/engine
./bin/engine -config ./configs/config.yaml
```

Output goes to `./logs/recorder/data-YYYY-MM-DD-HH.ndjson`. Verify with:
```bash
wc -l ./logs/recorder/data-*.ndjson
head -1 ./logs/recorder/data-*.ndjson | python3 -m json.tool
```
