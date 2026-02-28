# GO_SYSTEM_ARCHITECTURE.md

## Polymarket Last-Minute Insurance Bot — Go Native System Architecture (Reliability First)

### One-Sentence Goal

Build a **boring, survivable, single-market trading system** in Go that is deterministic, auditable, and safe to operate with real capital (<$500 experimental phase).

---

# 0. Core Design Philosophy (Non-Negotiable)

* Reliability > Performance
* Clarity > Cleverness
* Determinism > Concurrency Magic
* Observability > Optimization
* Operator Understanding > Language Prestige

This system is NOT:

* HFT
* Multi-market portfolio engine
* ML prediction system
* Ultra-low latency trading infra

Latency tolerance: **100–500ms acceptable**

---

# 1. High-Level Architecture (Language-Agnostic, Go Implementation)

```
[ Data Connectors (goroutines) ]
    - Chainlink
    - Binance Spot
    - Binance Perps
    - Polymarket WS
                |
                v
        [ In-Memory State ]
        (Hot snapshots only, RWMutex protected)
                |
                v
        [ Deterministic Decision Engine ]
        (Single-threaded ticker loop)
                |
                v
        [ Order Execution Module ]
                |
                v
        [ NDJSON Persistence Layer ]
                |
                v
        [ S3 Archival Worker ]
```

---

# 2. Lifecycle & Process Model (MANDATORY)

## Lifecycle Controller

The system MUST use:

* `context.Context` (root cancellation)
* `sync.WaitGroup` (goroutine supervision)
* Graceful shutdown on SIGINT/SIGTERM

### Startup Sequence

1. Load config
2. Initialize logger
3. Initialize state store
4. Initialize connectors (no start yet)
5. Start persistence writer
6. Start connectors (goroutines)
7. Start decision engine (main loop)
8. Block until context cancelled

### Shutdown Sequence (Critical for Capital Safety)

1. Cancel root context
2. Stop decision loop
3. Stop all connectors
4. Flush NDJSON logs
5. Close S3 uploader
6. Exit cleanly

No forced exits. No orphan goroutines.

---

# 3. Folder Structure (Strict)

```
cmd/
  bot/
    main.go

internal/
  app/
    app.go                 # Orchestration
  config/
    config.go
  lifecycle/
    supervisor.go          # ctx + WaitGroup control
  logging/
    logger.go              # structured logging setup
  state/
    state.go               # hot in-memory state
  connectors/
    chainlink/
    binance_spot/
    binance_perps/
    polymarket/
    s3/
  decision/
    engine.go              # deterministic logic loop
  execution/
    orders.go              # polymarket order submission
  persistence/
    ndjson_writer.go       # append-only logs
```

---

# 4. Concurrency Model (Go-Specific)

## Allowed Concurrency

* Goroutines per connector
* Single goroutine decision loop
* Single writer goroutine for logs

## Forbidden Patterns

* Hidden background goroutines
* Untracked async jobs
* Shared mutable state without locks
* Async mutation inside decision logic

---

# 5. In-Memory State Design (Hot State Only)

### What MUST be stored in memory:

* Latest Chainlink ticks (ring buffer ~3 min)
* Latest Binance partial depth snapshot
* Wall persistence counters
* Perps risk flags (liquidations/OI trend)
* Polymarket best bid/ask + freshness
* Expiry timestamp

### What MUST NOT be stored:

* Full historical candles
* Full orderbook history
* Analytics datasets
* Backtesting data

State size should remain small (<10MB).

---

# 6. Connector Interface Standard (Strict Contract)

All connectors MUST implement:

```go
type Connector interface {
    Name() string
    Start(ctx context.Context, wg *sync.WaitGroup) error
    Health() error
}
```

Rules:

* Must respect `ctx.Done()`
* Must auto-reconnect with backoff
* Must never panic
* Must log structured events

---

# 7. Deterministic Decision Engine

## Core Properties

* Single-threaded
* Pure function style (no async side effects)
* Fixed interval ticker (e.g. 100ms)
* Read-only state access

Example Loop:

```go
ticker := time.NewTicker(100 * time.Millisecond)
for {
  select {
    case <-ctx.Done():
      return
    case now := <-ticker.C:
      decision := engine.Decide(state, now)
      executor.Handle(decision)
  }
}
```

NO network calls inside decision logic.

---

# 8. Logging & Persistence (NDJSON Only for v1)

## Format: NDJSON (Append Only)

Example:

```json
{"ts":"2026-02-28T12:00:01Z","component":"chainlink","price":61234.2}
{"ts":"2026-02-28T12:00:02Z","decision":"NO_TRADE","reason":"volatility_high"}
```

### Requirements:

* Structured logs (zerolog or zap)
* Hourly file rotation
* fsync every few seconds
* Human-readable for debugging losses

Parquet is intentionally NOT used in v1 for simplicity and auditability.

---

# 9. S3 Archival Worker

Responsibilities:

* Periodic upload of rotated NDJSON logs
* Non-blocking background worker
* Retry with exponential backoff
* Never block trading loop

---

# 10. Error Handling Philosophy

* Fail soft on connector errors
* Skip trades if any feed is stale
* No retries on order execution timeouts
* Prefer NO TRADE over risky trade

---

# 11. Risk Guardrails (System-Level)

* Hard position size cap
* Max 2 losses/day → auto stop
* Kill switch via config/env
* Skip trade if:

    * Feed stale
    * Latency spike
    * Empty orderbook
    * High volatility burst

---

# 12. Observability Requirements

System MUST log:

* Feed freshness metrics
* Volatility metrics
* Decision reasons
* Execution latency
* Connector reconnect events

Goal: Every loss must be explainable via logs.

---

# 13. Deployment Target

* AWS EC2 (c7g.large or equivalent)
* Amazon Linux 2023
* systemd service
* One bot = One market = One process

---

# 14. Non-Goals (Important)

* No microservices
* No Kafka
* No heavy frameworks
* No ORM
* No distributed systems complexity

This is a **single-process, high-reliability trading daemon**.
