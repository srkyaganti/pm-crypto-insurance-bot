# CHAINLINK_DATASTREAMS_INTEGRATION_SPEC.md

## Minimal, Production-Grade Chainlink Data Streams Integration (Go SDK)

### For Deterministic Market Engine (Reference Price Feed Only)

---

# 0. ROLE OF CHAINLINK IN THIS SYSTEM (CRITICAL CONTEXT)

Chainlink Data Streams is the **primary reference price oracle** used for:

* Fair price anchoring
* Volatility filtering
* Market sanity validation
* Risk gating (before execution)

It is NOT used for:

* Execution
* Order placement
* Market discovery
* Historical analytics in hot loop

Binance = microstructure + liquidity
Polymarket = execution venue
Chainlink = trusted benchmark price (source of truth)

---

# 1. OFFICIAL SDK (MANDATORY)

Go SDK:

```bash
go get github.com/smartcontractkit/data-streams-sdk/go
```

The SDK provides:

* REST retrieval (latest reports)
* Real-time streaming via WebSocket
* Built-in reconnection & HA mode
  ([Chainlink Documentation][1])

Use SDK only.
DO NOT implement raw WebSocket client manually.

---

# 2. REQUIRED CLIENT CONFIGURATION (GO)

## Import

```go
import streams "github.com/smartcontractkit/data-streams-sdk/go"
import feed "github.com/smartcontractkit/data-streams-sdk/go/feed"
```

## Config Contract (MANDATORY FIELDS)

```go
type Config struct {
    ApiKey     string
    ApiSecret  string
    RestURL    string
    WsURL      string
    WsHA       bool
    WsMaxReconnect int
    Logger     func(format string, a ...any)
}
```

The SDK supports high availability mode with multiple concurrent connections and automatic deduplication of reports.
([Chainlink Documentation][1])

---

# 3. ENDPOINTS (ENVIRONMENT VARIABLES)

## Production (Example Pattern)

```env
CHAINLINK_REST_URL=https://api.dataengine.chain.link
CHAINLINK_WS_URL=wss://ws.dataengine.chain.link
CHAINLINK_API_KEY=YOUR_KEY
CHAINLINK_API_SECRET=YOUR_SECRET
```

Testnet example (docs pattern):

```text
https://api.testnet-dataengine.chain.link
wss://ws.testnet-dataengine.chain.link
```

([Chainlink Documentation][2])

---

# 4. REQUIRED API METHODS (STRICT MINIMAL SET)

## 4.1 Stream (PRIMARY — REAL-TIME PRICE)

```go
Stream(ctx context.Context, feedIDs []feed.ID) (Stream, error)
```

Purpose:

* Continuous real-time price reports
* Low latency benchmark price
* Deterministic event-driven updates

The Stream interface blocks on `Read()` until a report, context cancel, or error occurs.
([Chainlink Documentation][1])

This is the MAIN integration method for your bot.

---

## 4.2 GetLatestReport (COLD START ONLY)

```go
GetLatestReport(ctx context.Context, id feed.ID)
```

Use cases:

* Startup state initialization
* Recovery if stream reconnects
* Sanity validation

DO NOT poll this in a loop.

---

## 4.3 StreamWithStatusCallback (HIGHLY RECOMMENDED)

```go
StreamWithStatusCallback(ctx, feedIDs, callback)
```

Purpose:

* Connection health monitoring
* Feed freshness tracking
* RiskState integration

This directly supports your:

```text
RiskState.ChainlinkFresh
FeedsHealthy
```

---

# 5. FEED SELECTION (CRITICAL)

For BTC prediction markets, you should use:

* BTC/USD Crypto Stream (v3 schema typically)

Feed IDs are 32-byte identifiers representing each stream.
([Chainlink Documentation][1])

Architecture Rule:

```yaml
chainlink:
  feeds:
    - BTC/USD
```

Resolved at bootstrap into:

```go
var btcFeedID feed.ID
btcFeedID.FromString("0x...")
```

---

# 6. STREAM PROCESSING FLOW (MANDATORY DESIGN)

## Correct Lifecycle Pattern

```go
client, _ := streams.New(config)

stream, _ := client.Stream(ctx, []feed.ID{btcFeedID})

for {
    report, err := stream.Read(ctx)
    if err != nil {
        break
    }
    // decode + update state
}
```

`Read()` blocks until:

* New report
* Context cancellation
* Connection error
  ([Chainlink Documentation][1])

This fits perfectly with:

* ctx.cancel supervisor
* graceful shutdown
* deterministic loop design

---

# 7. REPORT DECODING (MANDATORY)

Chainlink returns:

```go
type ReportResponse struct {
    FeedID []byte
    FullReport []byte
    ObservationsTimestamp uint64
}
```

You MUST decode the payload:

```go
import report "github.com/smartcontractkit/data-streams-sdk/go/report"
import v3 "github.com/smartcontractkit/data-streams-sdk/go/report/v3"

decoded, err := report.Decode[v3.Data](fullReport)
```

Different feed IDs correspond to different schema versions (v1–v10).
([Chainlink Documentation][1])

For crypto feeds: usually v3 schema.

---

# 8. HIGH AVAILABILITY MODE (STRONGLY RECOMMENDED)

Enable:

```go
config := streams.Config{
    WsHA: true,
    WsMaxReconnect: 10,
}
```

HA mode:

* Opens multiple WS connections
* Deduplicates reports
* Handles partial reconnects automatically
  ([Chainlink Documentation][1])

This is IDEAL for capital-sensitive trading systems.

---

# 9. GO CONNECTOR ARCHITECTURE (MANDATORY)

Folder:

```text
internal/connectors/chainlink/
  client.go
  stream.go
  decoder.go
  health.go
  types.go
```

Responsibilities:

* client.go → SDK initialization
* stream.go → streaming loop (ctx-aware)
* decoder.go → report decoding
* health.go → freshness + heartbeat logic

---

# 10. STATE UPDATE CONTRACT (CRITICAL)

Connector MUST update ONLY:

```go
state.Chainlink.LatestPrice
state.Chainlink.LastUpdate
state.Chainlink.IsStale
state.Chainlink.Ticks (ring buffer)
```

NEVER:

* Run decision logic inside connector
* Perform blocking IO inside state lock
* Store raw reports in memory

---

# 11. FEED FRESHNESS & RISK RULES (NON-NEGOTIABLE)

Define stale if:

```text
No new report > 500ms (recommended threshold)
```

If stale:

```go
RiskState.ChainlinkFresh = false
RiskState.TradingDisabled = true
```

Because:
Decision engine relies on oracle price stability.

---

# 12. SHUTDOWN & LIFECYCLE (CTX-CANCEL COMPLIANCE)

Required behavior:

* Stream must stop on ctx.Done()
* Stream.Close() called during shutdown
* No orphan goroutines
* Supervisor manages lifecycle

Example:

```go
select {
case <-ctx.Done():
    stream.Close()
    return
default:
    report, _ := stream.Read(ctx)
}
```

---

# 13. APIS YOU MUST NOT USE (ANTI-PATTERN)

DO NOT:

* Poll GetReports in a loop
* Use REST as primary price source
* Implement custom WebSocket client
* Store full raw reports in state
* Decode inside decision engine

SDK streaming already provides:

* Fault tolerance
* reconnection
* report deduplication
  ([Chainlink Documentation][1])

---

# 14. FINAL MINIMAL V1 INTEGRATION CHECKLIST

### Required:

* [ ] SDK Client initialization
* [ ] HA Stream (primary)
* [ ] Report decoding (v3 schema)
* [ ] Feed freshness tracking
* [ ] ctx.cancel shutdown handling

### Optional:

* [ ] GetLatestReport at startup
* [ ] Stats() monitoring

This is the COMPLETE and sufficient Chainlink integration surface for a deterministic trading engine.

[1]: https://docs.chain.link/data-streams/reference/data-streams-api/go-sdk?utm_source=chatgpt.com "Data Streams SDK (Go)"
[2]: https://docs.chain.link/data-streams/reference/data-streams-api/authentication/go-examples?utm_source=chatgpt.com "API Authentication - Go examples"
