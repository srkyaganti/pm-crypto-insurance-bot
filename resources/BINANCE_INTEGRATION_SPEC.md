# BINANCE_INTEGRATION_SPEC.md

## Minimal, Production-Safe Binance Integration (Spot + Perps)

### For Deterministic Market Engine (Config-Driven, Single Symbol)

---

# 0. Purpose of Binance in This System (Critical Context)

Binance is used ONLY as a **risk and market reference feed**, NOT for execution.

This system requires:

* Real-time fair price validation
* Liquidity barrier detection (spot)
* Liquidation risk detection (perps)
* Short-term volatility sanity checks

This is NOT:

* A trading system on Binance
* An HFT system
* A multi-symbol stream aggregator (v1)

One process = One symbol (config-driven)

---

# 1. Base Endpoints (Authoritative)

## Spot WebSocket Base

```
wss://stream.binance.com:9443
```

## Futures (USDT-M Perps) WebSocket Base

```
wss://fstream.binance.com
```

Binance supports:

* Raw streams: `/ws/<stream>`
* Combined streams: `/stream?streams=a/b/c`
  ([developers.binance.com][1])

Combined streams are REQUIRED for reliability and lower connection overhead.

---

# 2. REQUIRED STREAMS (Spot + Perps)

## 2.1 Spot — Diff Depth Stream (MANDATORY)

### Stream:

```
<symbol>@depth@100ms
Example: btcusdt@depth@100ms
```

### Why Required:

Provides incremental order book updates (bids & asks) pushed at high frequency for local book management.
([developers.binance.com][2])

Used For:

* Liquidity wall detection
* Barrier between price and strike
* Order book imbalance
* Market microstructure sanity

---

## 2.2 Spot — Book Ticker Stream (MANDATORY)

### Stream:

```
<symbol>@bookTicker
Example: btcusdt@bookTicker
```

### Description:

Pushes real-time best bid and ask price/quantity updates.
([developers.binance.com][3])

Used For:

* Fast mid-price calculation
* Spread monitoring
* Backup price feed if depth lags
* Low-latency sanity checks

---

## 2.3 Futures (Perps) — Liquidation Stream (CRITICAL)

### Stream:

```
<symbol>@forceOrder
Example: btcusdt@forceOrder
```

### Description:

Pushes force liquidation order snapshots for the symbol.
([developers.binance.com][4])

Used For:

* Crash risk detection
* Volatility spike filtering
* “Perps calm” condition in decision engine

If liquidation clusters appear near expiry → NO TRADE.

---

## 2.4 Futures (Perps) — Mark Price Stream (HIGHLY RECOMMENDED)

### Stream:

```
<symbol>@markPrice@1s
Example: btcusdt@markPrice@1s
```

Used For:

* Fair price vs spot divergence
* Funding/market stress proxy
* Better crash detection than last trade price

(Defined in Binance Futures WebSocket Market Streams documentation)
([developers.binance.com][5])

---

# 3. REQUIRED REST ENDPOINTS (Spot Only)

## 3.1 Initial Order Book Snapshot (MANDATORY)

Endpoint:

```
GET https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000
```

Why:
Depth WebSocket is incremental and MUST be synced with a snapshot to maintain a correct local order book.

Without snapshot sync:

* Book desync
* False wall detection
* Unsafe trading decisions

---

# 4. FINAL MINIMAL STREAM CONFIG (Single Symbol)

## Recommended Combined Connections

### Spot Connection:

```
wss://stream.binance.com:9443/stream?streams=
{symbol_lower}@depth@100ms/
{symbol_lower}@bookTicker
```

Example:

```
btcusdt@depth@100ms/btcusdt@bookTicker
```

---

### Futures (Perps) Connection:

```
wss://fstream.binance.com/stream?streams=
{symbol_lower}@forceOrder/
{symbol_lower}@markPrice@1s
```

Example:

```
btcusdt@forceOrder/btcusdt@markPrice@1s
```

Binance supports combined streams on one connection for multiple stream names.
([developers.binance.com][5])

---

# 5. CONFIG-DRIVEN SYMBOL DESIGN (REQUIRED)

All symbols MUST be:

* Uppercase in REST (BTCUSDT)
* Lowercase in WebSocket streams (btcusdt)

Example config:

```yaml
binance:
  symbol: "BTCUSDT"
  use_spot: true
  use_perps: true
  depth_speed: "100ms"
```

Derived internally:

* ws_symbol = strings.ToLower(symbol)

---

# 6. GO CONNECTOR ARCHITECTURE (MANDATORY)

## Folder Structure

```
internal/connectors/binance/
  spot_ws.go
  futures_ws.go
  snapshot.go
  parser.go
  reconnect.go
  types.go
```

---

## Connector Responsibilities

### Spot Connector:

* Maintain local order book
* Process depth updates
* Track wall persistence
* Update best bid/ask state

### Futures Connector:

* Track liquidation events
* Maintain perps risk flags
* Track mark price deviations

---

# 7. LIFECYCLE REQUIREMENTS (CRITICAL FOR RELIABILITY)

Each connector MUST:

* Accept context.Context
* Support graceful shutdown
* Implement auto-reconnect with exponential backoff
* Respond to ping/pong frames
* Stop cleanly on ctx.Cancel()

Binance WebSocket servers send periodic ping frames and disconnect inactive clients.
([developers.binance.com][1])

---

# 8. RATE LIMIT & CONNECTION SAFETY

Important constraints:

* Max streams per connection: 1024
* Connections expire ~24 hours
* Must handle reconnect logic
  ([developers.binance.com][5])

Best Practice:

* 1 spot WS connection
* 1 futures WS connection
* NOT multiple sockets per stream

---

# 9. STREAMS YOU MUST NOT USE (Context & CPU Waste)

DO NOT SUBSCRIBE TO:

* kline/candles
* miniTicker
* aggTrade
* all market streams (!ticker, !miniTicker)
* user data streams (not trading on Binance)

These add noise and complexity without improving decision quality.

---

# 10. DATA FLOW INTO STATE LAYER

| Stream      | State Field       | Used By                  |
| ----------- | ----------------- | ------------------------ |
| depth@100ms | OrderBookSnapshot | Barrier & wall detection |
| bookTicker  | BestBid/BestAsk   | Fair price sanity        |
| forceOrder  | LiquidationFlags  | Risk filter              |
| markPrice   | PerpsFairPrice    | Crash detection          |

---

# 11. FAILURE SAFETY RULES

If ANY of the following occurs:

* WebSocket stale > 2 seconds
* Reconnect loop active
* Snapshot sync failed
* Message sequence mismatch

System MUST:

```
Disable trading immediately (NO_TRADE mode)
```

Reliability > trade frequency.

---

# 12. v1 Implementation Scope (STRICT)

Include:

* Spot depth
* Spot bookTicker
* Perps liquidation
* Perps mark price
* Snapshot sync
* Reconnect logic
* Structured logging

Exclude:

* Multi-symbol support (v1)
* User streams
* Advanced analytics
* Order execution on Binance

[1]: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams?utm_source=chatgpt.com "WebSocket Streams | Binance Open Platform"
[2]: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Diff-Book-Depth-Streams?utm_source=chatgpt.com "Diff Book Depth Streams | Binance Open Platform"
[3]: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Individual-Symbol-Book-Ticker-Streams?utm_source=chatgpt.com "Individual Symbol Book Ticker Streams"
[4]: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Liquidation-Order-Streams?utm_source=chatgpt.com "Liquidation Order Streams | Binance Open Platform"
[5]: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams?utm_source=chatgpt.com "Websocket Market Streams"