# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

A single-process Go trading bot for Polymarket crypto insurance markets (BTC up/down prediction markets). It ingests real-time data from Chainlink, Binance (spot + perps), and Polymarket, runs a deterministic decision engine, and executes trades on Polymarket. Designed for reliability over performance — latency tolerance is 100-500ms.

## Build & Run

```bash
# Build the main engine
go build -o bin/engine ./cmd/engine

# Build the standalone polymarket tracker integration test
go build -o bin/polymarket_tracker ./cmd/integration_tests/polymarket_tracker

# Run the engine (requires env vars for API keys — see configs/config.yaml for ${} placeholders)
./bin/engine -config ./configs/config.yaml

# Run tests
go test ./...

# Run tests for a specific package
go test ./internal/connectors/polymarket/market/...
```

Config is YAML with `${ENV_VAR}` expansion via `os.ExpandEnv`. Required env vars: `CHAINLINK_API_KEY`, `CHAINLINK_API_SECRET`, `CHAINLINK_FEED_ID`, `POLYMARKET_API_KEY`, `POLYMARKET_API_SECRET`, `POLYMARKET_PASSPHRASE`, `POLYMARKET_PRIVATE_KEY`, `BINANCE_SYMBOL`, `SYMBOL`.

## Architecture

**Single-process daemon** — no microservices, no Kafka, no heavy frameworks.

### Data Flow
```
Connectors (goroutines) → In-Memory StateStore (RWMutex) → Decision Engine (ticker loop) → Order Execution → NDJSON Persistence
```

### Key Packages

- `internal/app/` — Orchestration. `App` struct wires config, connectors, state, persistence, and lifecycle together.
- `internal/state/` — Thread-safe `StateStore` with `Update*` (write) and `Get*` (read) methods. All connector data flows through here. State is hot/small (<10MB) — no historical data stored.
- `internal/connectors/` — All implement the `Connector` interface (`Name()`, `Start(ctx)`, `Stop()`, `Health()`). Each runs as its own goroutine(s).
  - `chainlink/` — REST + WebSocket client for Chainlink Data Streams price feeds. Also has a `Client` type used independently by the tracker.
  - `binance/spot/` — WebSocket for order book depth + aggregate trades.
  - `binance/perps/` — WebSocket for mark price + liquidation events.
  - `polymarket/gamma/` — REST client for Polymarket Gamma API (market discovery). Uses slug-based window system (e.g. `btc-updown-5min-...`).
  - `polymarket/market/` — WebSocket for Polymarket CLOB market data. Supports dynamic subscribe/unsubscribe at runtime. Handles 7 event types (book, price_change, tick_size_change, last_trade_price, best_bid_ask, new_market, market_resolved).
  - `polymarket/tracker/` — High-level connector that coordinates gamma (market discovery) + market (WebSocket data) + chainlink (price at timestamp). Manages market lifecycle: discovery → subscribe → monitor → resolve.
- `internal/lifecycle/` — `Supervisor` manages root `context.Context` + `sync.WaitGroup` + signal handling (SIGINT/SIGTERM).
- `internal/persistence/` — Append-only NDJSON event writer with hourly rotation and buffered flushing.
- `internal/config/` — YAML config loading with env var expansion and validation.
- `internal/logging/` — zerolog setup with console + file multi-writer. Use `logging.Component(logger, "name")` to create component-scoped loggers.

### Concurrency Model
- One goroutine per connector (some have multiple: WS read + monitoring)
- Single-threaded decision loop on a fixed-interval ticker
- `StateStore` is the only shared mutable state, protected by `sync.RWMutex`
- All connectors use `context.Context` for cancellation

### Design Principles
- Reliability > performance; determinism > concurrency magic
- Fail soft on connector errors — skip trades if any feed is stale
- No network calls inside decision logic
- Prefer NO TRADE over risky trade
- Every loss must be explainable via logs

## Connector Pattern

New connectors must implement `connectors.Connector` (defined in `internal/connectors/connector.go`):
```go
type Connector interface {
    Name() string
    Start(ctx context.Context) error
    Stop() error
    Health() error
}
```
Must respect `ctx.Done()`, auto-reconnect with backoff, never panic, log structured events. Register in `app.initializeConnectors()`.

## Config

`configs/config.yaml` — sections: `app`, `logging`, `chainlink`, `binance_spot`, `binance_perps`, `polymarket` (with nested `tracker`), `risk`, `persistence`. Types defined in `internal/config/types.go`.
