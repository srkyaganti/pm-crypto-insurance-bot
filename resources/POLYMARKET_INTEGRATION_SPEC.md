# POLYMARKET_INTEGRATION_SPEC.md

## Minimal, Production-Grade Polymarket API Integration (CLOB Only)

### For Deterministic Market Engine (Single Market, Config Driven)

---

# 0. Purpose of Polymarket Integration (Critical Context)

Polymarket is the **execution venue** and primary prediction market data source.

This system requires Polymarket for:

* Real-time orderbook (execution pricing)
* Best bid/ask tracking
* Order placement (limit orders only)
* Order status & fills tracking
* Market metadata discovery (token IDs, condition IDs)

This system does NOT require:

* RTDS feeds (crypto prices)
* Sports WebSocket feeds
* Social/comment streams
* Full historical analytics in hot path

The core trading interface is the **CLOB (Central Limit Order Book)** API.
([docs.polymarket.com][1])

---

# 1. API SURFACE OVERVIEW (What Exists vs What We Use)

Polymarket provides multiple APIs:

| API            | Base URL                                       | Purpose                     |
| -------------- | ---------------------------------------------- | --------------------------- |
| CLOB API       | https://clob.polymarket.com                    | Orderbook, prices, trading  |
| Gamma API      | https://gamma-api.polymarket.com               | Market discovery & metadata |
| Data API       | https://data-api.polymarket.com                | Positions & history         |
| CLOB WebSocket | wss://ws-subscriptions-clob.polymarket.com/ws/ | Real-time book & orders     |

([docs.polymarket.com][2])

For a trading bot, the **CLOB API + CLOB WebSocket** are the primary interfaces.

---

# 2. REQUIRED WEBSOCKET APIs (REAL-TIME CORE)

## 2.1 CLOB WebSocket Base Endpoint (MANDATORY)

```text
wss://ws-subscriptions-clob.polymarket.com/ws/
```

This WebSocket provides near real-time updates for:

* Markets
* Orders
* Trades
* Orderbook state
  ([docs.polymarket.com][3])

---

## 2.2 Market Channel (CRITICAL — PRIMARY DATA FEED)

### Subscription Type:

```json
{
  "type": "market",
  "assets_ids": ["<TOKEN_ID>"]
}
```

Purpose:

* Real-time orderbook updates
* Price changes
* Liquidity levels
* Best bid / best ask

The market channel emits:

* `book` events (orderbook snapshots)
* `price_change` events (level updates)
  ([docs.polymarket.com][4])

Example payload:

```json
{
  "event_type": "book",
  "bids": [{ "price": "0.48", "size": "30" }],
  "asks": [{ "price": "0.52", "size": "25" }]
}
```

Used by system for:

* Pricing decisions
* Slippage checks
* Liquidity sanity
* Execution price anchoring

---

## 2.3 User Channel (MANDATORY FOR LIVE TRADING)

### Subscription Type:

```json
{
  "type": "user",
  "auth": {
    "apiKey": "...",
    "secret": "...",
    "passphrase": "..."
  }
}
```

This channel provides authenticated updates for:

* Order fills
* Trade matches
* Order failures
* Status transitions (MATCHED, CONFIRMED, FAILED)

The user channel streams real-time updates related to orders and trades for the authenticated user.
([docs.polymarket.com][5])

WITHOUT this channel:

* You cannot safely track fills
* You cannot detect partial fills
* You cannot manage risk

This is NON-NEGOTIABLE for execution bots.

---

# 3. REQUIRED REST APIs (CLOB — EXECUTION + SNAPSHOTS)

## Base URL

```text
https://clob.polymarket.com
```

([docs.polymarket.com][2])

---

## 3.1 Get Orderbook Snapshot (MANDATORY)

```http
GET /book?token_id=<TOKEN_ID>
```

Purpose:

* Cold start sync
* WebSocket desync recovery
* Debug validation

Returns:

* Full bids
* Full asks
* Aggregated book state

The CLOB API exposes orderbook states and live trading data programmatically.
([Medium][6])

---

## 3.2 Get Current Price (REQUIRED)

```http
GET /price?token_id=<TOKEN_ID>&side=BUY
```

Used for:

* Fair price sanity
* Pre-trade validation
* Backup to WebSocket price feed

---

## 3.3 Place Order (MANDATORY — EXECUTION)

```http
POST /order
```

Purpose:

* Submit limit orders to CLOB
* Core execution path

Note:
Orders are matched off-chain via the CLOB and later settled on-chain.
([docs.polymarket.com][1])

---

## 3.4 Cancel Order (MANDATORY RISK CONTROL)

```http
DELETE /order
```

Used for:

* Timeout handling
* Risk management
* Emergency exits

---

## 3.5 Midpoint Price (OPTIONAL BUT RECOMMENDED)

```http
GET /midpoint?token_id=<TOKEN_ID>
```

Use cases:

* Fair value estimation
* Spread analysis
* Decision engine sanity checks

---

# 4. GAMMA API (BOOTSTRAP ONLY — NOT HOT PATH)

## Base URL

```text
https://gamma-api.polymarket.com
```

([docs.polymarket.com][2])

---

## Required Endpoint:

```http
GET /markets
```

Purpose:

* Discover tradeable markets
* Retrieve:

    * condition_id (market identifier)
    * clobTokenIds (YES/NO token IDs)
    * expiry timestamp
    * metadata

Gamma API is used for market discovery and metadata indexing.
([docs.polymarket.com][7])

Usage Pattern:

* Call at startup ONLY
* Cache locally
* Never call in decision loop

---

# 5. AUTHENTICATION MODEL (CRITICAL — DO NOT MISIMPLEMENT)

Polymarket CLOB uses TWO authentication layers:

## 5.1 L1 Authentication (Private Key Signing)

* Uses wallet private key
* EIP-712 signature
* Proves wallet ownership

## 5.2 L2 Authentication (API Credentials)

* apiKey
* secret
* passphrase
* HMAC signed requests

The CLOB uses two levels of authentication: L1 (private key) and L2 (API key).
([docs.polymarket.com][8])

Important:

* Public endpoints (book/price) do NOT require auth
* Order placement REQUIRES L2 auth headers

---

# 6. REQUIRED HEADERS (TRADING REQUESTS)

For authenticated REST calls:

```
POLY_ADDRESS
POLY_SIGNATURE
POLY_TIMESTAMP
POLY_NONCE
```

Signature is generated via EIP-712 signing using wallet key.
([docs.polymarket.com][8])

---

# 7. DATA FLOW INTO YOUR GO ARCHITECTURE

| Source      | Module               | State Usage              |
| ----------- | -------------------- | ------------------------ |
| Market WS   | polymarket_market_ws | orderbook + best bid/ask |
| User WS     | polymarket_user_ws   | fills + order status     |
| GET /book   | snapshot_sync        | cold start recovery      |
| POST /order | execution module     | limit order placement    |
| Gamma API   | bootstrap            | market + token IDs       |

---

# 8. GO CONNECTOR STRUCTURE (MANDATORY)

Recommended folder:

```
internal/connectors/polymarket/
  market_ws.go        # public market data stream
  user_ws.go          # authenticated order updates
  clob_rest.go        # order + book + price
  gamma_client.go     # market discovery
  auth.go             # L1/L2 signing logic
  reconnect.go        # exponential backoff
  types.go            # payload schemas
```

---

# 9. WEBSOCKET BEST PRACTICES (RELIABILITY)

System MUST implement:

* Ping/Pong heartbeat handling
* Automatic reconnection with exponential backoff
* Local orderbook reconstruction
* Context-based shutdown (ctx.Done())

WebSocket feeds provide low-latency real-time market data suitable for market-making and trading systems.
([docs.polymarket.com][9])

---

# 10. APIS YOU MUST NOT USE (Context & Complexity Waste)

DO NOT integrate:

* RTDS (crypto price feeds)
* Sports WebSocket API
* Comment streams
* Data API (positions/history) in hot loop
* UI/internal endpoints

These add latency, noise, and unnecessary complexity.

---

# 11. MINIMAL V1 INTEGRATION CHECKLIST (STRICT)

### WebSocket:

* [ ] Market Channel (public)
* [ ] User Channel (authenticated)

### REST (CLOB):

* [ ] GET /book
* [ ] GET /price
* [ ] POST /order
* [ ] DELETE /order

### Bootstrap:

* [ ] Gamma API /markets (startup only)

This is the COMPLETE and sufficient Polymarket integration surface for a deterministic trading bot.

[1]: https://docs.polymarket.com/developers/CLOB/introduction?utm_source=chatgpt.com "CLOB Introduction"
[2]: https://docs.polymarket.com/quickstart/reference/endpoints?utm_source=chatgpt.com "Endpoints"
[3]: https://docs.polymarket.com/developers/CLOB/websocket/wss-overview?utm_source=chatgpt.com "WSS Overview"
[4]: https://docs.polymarket.com/developers/CLOB/websocket/market-channel?utm_source=chatgpt.com "Market Channel"
[5]: https://docs.polymarket.com/developers/CLOB/websocket/user-channel?utm_source=chatgpt.com "User Channel"
[6]: https://medium.com/%40gwrx2005/the-polymarket-api-architecture-endpoints-and-use-cases-f1d88fa6c1bf?utm_source=chatgpt.com "The Polymarket API: Architecture, Endpoints, and Use Cases"
[7]: https://docs.polymarket.com/quickstart/overview?utm_source=chatgpt.com "Developer Quickstart"
[8]: https://docs.polymarket.com/developers/CLOB/authentication?utm_source=chatgpt.com "Authentication"
[9]: https://docs.polymarket.com/developers/market-makers/data-feeds?utm_source=chatgpt.com "Data Feeds"
