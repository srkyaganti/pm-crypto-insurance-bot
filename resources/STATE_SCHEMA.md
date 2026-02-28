# STATE_SCHEMA.md

## Canonical In-Memory State Contract (Deterministic Market Engine)

### PURPOSE

Define the single source of truth for ALL in-memory data used by:

* Connectors (Binance, Polymarket, Chainlink)
* Decision Engine (read-only)
* Execution module (read-only pricing)
* Risk filters (feed freshness, volatility, liquidations)

This schema MUST NOT contain:

* Business logic
* Network clients
* Historical datasets
* Analytics pipelines

Hot state only.

---

# 0. CORE DESIGN PRINCIPLES (NON-NEGOTIABLE)

1. Single shared state store (thread-safe)
2. Read-heavy, write-light design
3. Minimal memory footprint (<10MB)
4. Deterministic reads for decision engine
5. No blocking operations inside state layer
6. No external API calls inside state structs

State = Data Only.
Logic lives elsewhere.

---

# 1. TOP-LEVEL STATE STRUCT (GO CONTRACT)

```go
type AppState struct {
    Meta        MetaState
    Chainlink   ChainlinkState
    BinanceSpot BinanceSpotState
    BinancePerps BinancePerpsState
    Polymarket  PolymarketState
    Risk        RiskState
}
```

Access Pattern:

* Connectors → WRITE
* Decision Engine → READ ONLY
* Execution → READ ONLY

Protected via:

```go
type StateStore struct {
    mu    sync.RWMutex
    state AppState
}
```

---

# 2. META STATE (SYSTEM HEALTH & CLOCK)

```go
type MetaState struct {
    StartedAt        time.Time
    LastDecisionTime time.Time
    UptimeSeconds    int64
    IsHealthy        bool
}
```

Purpose:

* Monitoring
* Supervisor health checks
* Debug logging context

---

# 3. CHAINLINK STATE (PRIMARY REFERENCE PRICE)

## 3.1 Price Tick

```go
type PriceTick struct {
    Timestamp time.Time
    Price     float64
}
```

## 3.2 Ring Buffer (MANDATORY)

```go
type RingBuffer struct {
    Buffer   []PriceTick
    Capacity int
    Index    int
    Full     bool
}
```

## 3.3 Chainlink State

```go
type ChainlinkState struct {
    LatestPrice   float64
    LastUpdate    time.Time
    IsStale       bool
    Ticks         RingBuffer  // ~3 minutes of ticks
}
```

Purpose:

* Volatility filtering (30s / 60s windows)
* Flatness detection before expiry
* Primary fair price anchor

---

# 4. BINANCE SPOT STATE (LIQUIDITY & BARRIER SIGNALS)

## 4.1 Order Book Level

```go
type OrderLevel struct {
    Price float64
    Size  float64
}
```

## 4.2 Spot Order Book Snapshot (PARTIAL DEPTH ONLY)

```go
type OrderBookSnapshot struct {
    Bids         []OrderLevel // Top 10–20 levels
    Asks         []OrderLevel
    BestBid      float64
    BestAsk      float64
    MidPrice     float64
    LastUpdate   time.Time
    SequenceID   int64
}
```

## 4.3 Spot State

```go
type BinanceSpotState struct {
    Symbol            string
    OrderBook         OrderBookSnapshot
    WallPersistence   int           // ticks of detected wall
    LastDepthUpdate   time.Time
    LastBookTicker    time.Time
    IsStale           bool
}
```

Used For:

* Liquidity wall detection
* Barrier between price and strike
* Market sanity validation

---

# 5. BINANCE PERPS STATE (CRASH RISK FILTER)

```go
type LiquidationEvent struct {
    Timestamp time.Time
    Side      string  // BUY or SELL
    Price     float64
    Quantity  float64
}
```

```go
type BinancePerpsState struct {
    Symbol              string
    LastMarkPrice       float64
    LastMarkUpdate      time.Time
    RecentLiquidations  []LiquidationEvent // last N events
    LiquidationFlag     bool
    IsStale             bool
}
```

Purpose:

* Detect liquidation clusters
* Identify sudden crash risk
* Validate “perps calm” condition

---

# 6. POLYMARKET STATE (EXECUTION VENUE — CRITICAL)

## 6.1 Polymarket Order Book Level

```go
type PMOrderLevel struct {
    Price float64
    Size  float64
}
```

## 6.2 Polymarket Order Book

```go
type PMOrderBook struct {
    Bids       []PMOrderLevel
    Asks       []PMOrderLevel
    BestBid    float64
    BestAsk    float64
    MidPrice   float64
    LastUpdate time.Time
}
```

## 6.3 Market Metadata (BOOTSTRAP)

```go
type MarketMeta struct {
    ConditionID string
    YesTokenID  string
    NoTokenID   string
    ExpiryTime  time.Time
}
```

## 6.4 Order Tracking State

```go
type OrderStatus struct {
    OrderID     string
    Status      string  // OPEN, FILLED, PARTIAL, CANCELLED
    FilledSize  float64
    AvgFillPrice float64
    LastUpdate  time.Time
}
```

## 6.5 Polymarket State

```go
type PolymarketState struct {
    Market        MarketMeta
    OrderBook     PMOrderBook
    LastWSUpdate  time.Time
    ActiveOrder   *OrderStatus
    IsStale       bool
}
```

The CLOB provides orderbook states and programmatic trading via API and WebSocket feeds. ([docs.polymarket.com][1])

---

# 7. RISK STATE (GLOBAL SAFETY LAYER)

```go
type RiskState struct {
    FeedsHealthy        bool
    ChainlinkFresh      bool
    BinanceFresh        bool
    PolymarketFresh     bool
    MaxMove30sPct       float64
    MaxMove60sPct       float64
    DailyLossCount      int
    TradingDisabled     bool
    LastRiskUpdate      time.Time
}
```

Purpose:

* Centralized kill-switch logic
* Feed staleness aggregation
* Volatility guardrails

---

# 8. FEED STALENESS CONTRACT (MANDATORY)

Each connector MUST update:

```go
LastUpdate time.Time
IsStale    bool
```

Staleness thresholds (recommended):

* Chainlink: > 300ms
* Binance: > 2s
* Polymarket WS: > 2s

If any critical feed is stale:

```go
RiskState.TradingDisabled = true
```

---

# 9. STATE UPDATE RULES (STRICT)

Connectors MUST:

* Acquire write lock (mu.Lock)
* Update only their sub-state
* Release immediately

Decision Engine MUST:

* Use read lock (mu.RLock)
* Never mutate state
* Never block on long operations

---

# 10. MEMORY BOUNDARIES (ANTI-BLOAT RULES)

ALLOWED:

* Last 3 minutes of ticks
* Partial orderbook (≤20 levels)
* Recent liquidations (≤100 events)

FORBIDDEN:

* Full historical candles
* Full depth history
* Raw websocket message logs in memory
* Analytics datasets

Logs go to NDJSON persistence, NOT state.

---

# 11. DETERMINISTIC READ CONTRACT (FOR DECISION ENGINE)

Decision engine reads ONLY:

```go
state.Chainlink.LatestPrice
state.BinanceSpot.OrderBook
state.BinancePerps.LiquidationFlag
state.Polymarket.OrderBook
state.Risk.*
```

No network calls.
No async awaits.
No mutations.

---

# 12. VERSIONING RULE (FOR FUTURE UPGRADES)

Add:

```go
type StateVersion struct {
    SchemaVersion string
}
```

This allows:

* Backward compatibility
* Safe migrations
* Replay testing with NDJSON logs

Current:

```text
SchemaVersion = "v1.0.0"
```

---

# FINAL NOTE FOR AI AGENTS

This STATE_SCHEMA.md is the canonical contract.

AI MUST:

* Not invent new top-level state fields
* Not embed logic inside state structs
* Not create alternative state models
* Strictly follow this schema when implementing connectors and decision engine

Any deviation risks non-deterministic trading behavior.

[1]: https://docs.polymarket.com/developers/CLOB/introduction?utm_source=chatgpt.com "CLOB Introduction"
