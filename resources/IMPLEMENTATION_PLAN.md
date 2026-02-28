# IMPLEMENTATION_PLAN.md

## Step-by-Step Go Implementation Plan (AI-Assisted, Deterministic Build)

### CRITICAL INSTRUCTION FOR AI

DO NOT generate the full codebase at once.
Implement module-by-module following this exact order.

---

# Phase 0 — Context Ingestion (Before Coding)

AI MUST:

1. Read GO_SYSTEM_ARCHITECTURE.md fully
2. Read original strategy/build guides
3. Read provided API references (Binance, Chainlink, Polymarket, AWS S3)
4. Produce a dependency and module map
5. Confirm understanding before writing code

---

# Phase 1 — Core Foundations (No Trading Logic Yet)

## 1.1 Config Module

Deliverables:

* Environment + YAML config loader
* Struct-based config
* Validation layer

Config should include:

* API keys
* market_id
* risk limits
* log paths
* S3 settings
* tick interval

---

## 1.2 Logging System (Production Grade)

Requirements:

* zerolog (preferred) or zap
* JSON structured logs
* file + stdout dual output
* log rotation support
* component tagging

Output Example:

```json
{"level":"info","component":"binance_spot","msg":"connected"}
```

---

## 1.3 Lifecycle Supervisor (CRITICAL)

Implement:

* Root context with cancel()
* WaitGroup orchestration
* Graceful shutdown handler (SIGINT/SIGTERM)
* Component registration system

Must expose:

```go
type Supervisor struct {
    ctx    context.Context
    cancel context.CancelFunc
    wg     sync.WaitGroup
}
```

---

# Phase 2 — In-Memory State Layer

## 2.1 State Store Design

Requirements:

* Thread-safe (RWMutex)
* Minimal hot state only
* Getter methods (read-heavy)
* No business logic inside state layer

Core Struct:

* Chainlink ring buffer
* Spot depth snapshot
* Perps risk flags
* Polymarket book snapshot
* Feed timestamps (for staleness checks)

---

## 2.2 Ring Buffer (Go Native)

Implement:

* Fixed capacity circular buffer
* Timestamped ticks
* Recent iterator function
* O(1) insertion

Purpose:

* 30s / 60s volatility checks
* Not full historical storage

---

# Phase 3 — Data Connectors (Plumbing Layer Only)

IMPORTANT:

* No trading logic inside connectors
* Strict API reference usage (no hallucinated endpoints)

## 3.1 Chainlink Connector

Responsibilities:

* WebSocket subscription
* Decode price reports
* Push ticks to state ring buffer
* Staleness tracking
* Auto reconnect with backoff

---

## 3.2 Binance Spot Connector

Responsibilities:

* Depth stream (top 10–20 levels)
* Aggregate liquidity between price and strike
* Wall detection signals
* Snapshot updates to state

---

## 3.3 Binance Perps Connector

Responsibilities:

* Liquidation stream ingestion
* OI trend polling (REST)
* Risk flag updates in state

---

## 3.4 Polymarket Market Data Connector

Responsibilities:

* Best bid/ask updates
* Orderbook freshness timestamp
* Book sanity metrics

---

# Phase 4 — Persistence Layer (NDJSON)

## 4.1 NDJSON Writer

Requirements:

* Append-only file writes
* Buffered async writer goroutine
* Channel-based log ingestion
* Periodic flush (fsync)
* Hourly file rotation

No Parquet in v1.

---

## 4.2 S3 Uploader (Background Worker)

Responsibilities:

* Periodic sync of rotated log files
* Retry with exponential backoff
* Non-blocking uploads
* Safe shutdown on ctx cancel

---

# Phase 5 — Deterministic Decision Engine (Core Brain)

IMPORTANT CONSTRAINTS:

* Single-threaded
* No network calls
* Read-only state access
* Fully explainable decisions

Implementation:

* 100ms ticker loop
* Feed freshness validation
* Entry window validation
* Risk filter evaluation
* Decision struct output

Decision Types:

* NO_TRADE
* TRADE_UP
* TRADE_DOWN

---

# Phase 6 — Execution Module (Polymarket CLOB)

Requirements:

* Limit orders only
* Timeout ≤ 700ms
* No automatic retries
* Client order ID generation
* Latency measurement logging

Failure behavior:

* Log and skip (never force retry)

---

# Phase 7 — App Orchestration (Final Assembly)

app.go must:

1. Initialize config
2. Initialize logger
3. Create root context
4. Initialize state store
5. Register all connectors
6. Start persistence worker
7. Start decision engine
8. Block until shutdown signal
9. Graceful cleanup

---

# Phase 8 — Safety & Testing Modes (Mandatory)

## 8.1 Paper Trading Mode (Default)

* No real order submission
* Full decision logging
* Dry-run execution path

## 8.2 Capital Safety Guards

* Max order size cap (hardcoded + config)
* Daily loss counter
* Kill switch flag
* Feed staleness auto-disable trading

---

# Phase 9 — Observability & Debugging

System MUST log:

* Decision reasoning
* Volatility metrics
* Feed staleness checks
* Connector health
* Execution latency
* Reconnect events

Goal: Post-mortem analysis must be possible from logs alone.

---

# Final AI Execution Rules

* Generate code module-by-module only
* Never introduce unnecessary abstractions
* Prefer readability over optimization
* Follow GO_SYSTEM_ARCHITECTURE.md strictly
* Ask for approval after each phase before proceeding

This is a capital-sensitive system. Reliability and clarity are the highest priorities.
