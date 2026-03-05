# Polymarket Market Tracker

Timer-based integration test that tracks Polymarket markets with precise Chainlink price verification and complete market lifecycle data collection.

## Overview

This integration test:
- Fetches ongoing markets using the Gamma API
- Subscribes to Yes/No asset IDs using the Market WebSocket connector
- Uses Chainlink prices at exact timestamps for market resolution
- Tracks active markets with timer-based rotation (5m, 15m windows)
- Logs detailed market data for verification
- Stores historical data for strategy development

## Features

### 1. Timer-Based Market Rotation
- Markets rotate exactly at window boundaries
- No polling overhead
- Pre-scheduled future windows (next 3 windows)

### 2. Chainlink Price Integration
- Uses `GetReportAtTimestampForFeed(feedID, timestamp)` for exact prices
- Start price at market start timestamp
- End price at market end timestamp
- Source of truth for market resolution

### 3. Paper Trading
- Execute simulated trades
- Track positions and PnL
- Strategy testing foundation

### 4. Data Persistence
- NDJSON format for market history
- Append-only for performance
- Each market saved immediately on completion

## Prerequisites

1. **Chainlink API Credentials**
   - REST URL
   - API Key
   - API Secret
   - BTC/USD Feed ID

2. **Environment Variables**
   ```bash
   export CHAINLINK_REST_URL="https://..."
   export CHAINLINK_API_KEY="your-api-key"
   export CHAINLINK_API_SECRET="your-api-secret"
   export BTC_USD_FEED_ID="0x..."
   ```

## Usage

### Basic Usage

```bash
go run cmd/integration_tests/polymarket_tracker/main.go \
  -chainlink-rest-url $CHAINLINK_REST_URL \
  -chainlink-api-key $CHAINLINK_API_KEY \
  -chainlink-api-secret $CHAINLINK_API_SECRET \
  -btc-feed-id $BTC_USD_FEED_ID
```

### Track Multiple Prefixes

```bash
go run cmd/integration_tests/polymarket_tracker/main.go \
  -prefix btc-updown- -prefix eth-updown- \
  -window 5 -window 15 \
  -chainlink-rest-url $CHAINLINK_REST_URL \
  -chainlink-api-key $CHAINLINK_API_KEY \
  -chainlink-api-secret $CHAINLINK_API_SECRET \
  -btc-feed-id $BTC_USD_FEED_ID
```

### Debug Mode

```bash
go run cmd/integration_tests/polymarket_tracker/main.go \
  -log-level debug \
  -log-interval 5s \
  -chainlink-rest-url $CHAINLINK_REST_URL \
  -chainlink-api-key $CHAINLINK_API_KEY \
  -chainlink-api-secret $CHAINLINK_API_SECRET \
  -btc-feed-id $BTC_USD_FEED_ID
```

## Command-Line Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-gamma-url` | `https://gamma-api.polymarket.com` | Gamma API base URL |
| `-ws-url` | `wss://ws-subscriptions-clob.polymarket.com/ws/market` | Market WebSocket URL |
| `-chainlink-rest-url` | (required) | Chainlink REST URL |
| `-chainlink-ws-url` | (optional) | Chainlink WebSocket URL |
| `-chainlink-api-key` | (required) | Chainlink API key |
| `-chainlink-api-secret` | (required) | Chainlink API secret |
| `-btc-feed-id` | (required) | Chainlink BTC/USD feed ID |
| `-prefix` | `btc-updown-` | Market prefixes (repeatable) |
| `-window` | `5, 15` | Window sizes in minutes (repeatable) |
| `-pre-schedule` | `3` | Number of future windows to pre-schedule |
| `-data-dir` | `./data/market_history` | Data directory for market history |
| `-log-interval` | `10s` | State log interval |
| `-log-level` | `info` | Log level (debug, info, warn, error) |
| `-custom-features` | `true` | Enable custom features |

## Market Lifecycle

```
1. Fetch current market from Gamma API
   ↓
2. Get Chainlink price at market START timestamp
   ↓
3. Subscribe to Yes/No assets via WebSocket
   ↓
4. Set timer for market END
   ↓
5. Pre-schedule next 3 windows
   ↓
6. [Active monitoring - log state every 10s]
   ↓
7. Timer fires at market END
   ↓
8. Get Chainlink price at market END timestamp
   ↓
9. Determine winning outcome (yes/no)
   ↓
10. Calculate paper PnL (if traded)
   ↓
11. Archive to market_history.ndjson
   ↓
12. Unsubscribe and repeat
```

## Market Resolution Logic

### Price Comparison
- **Start Price**: Chainlink BTC price at market start timestamp
- **End Price**: Chainlink BTC price at market end timestamp
- **Price Change**: End Price - Start Price

### Winning Outcome
- **Yes**: If Price Change > 0 (price went up)
- **No**: If Price Change <= 0 (price went down or stayed same)

### Example
```
Start: Chainlink Price = $50,000 (at timestamp 1772696700)
End:   Chainlink Price = $50,100 (at timestamp 1772697000)

Price Change = +$100 (+0.2%)

Winning Outcome = "YES" (price went up)

If we had YES position at $0.52:
  PnL = size * (1.0 - 0.52) = size * 0.48 (WIN)

If we had NO position at $0.48:
  PnL = -size * 0.48 (LOSS)
```

## Output

### Console Logs

```
2024-01-15T10:00:00Z INFO [market_tracker] Starting Polymarket market tracker
2024-01-15T10:00:00Z INFO [market_tracker] Market tracker is running. Press Ctrl+C to stop.
2024-01-15T10:00:01Z INFO [polymarket_market] Connected to websocket
2024-01-15T10:00:01Z INFO [market_tracker] Successfully subscribed to market slug=btc-updown-5m-1772649000
2024-01-15T10:00:10Z INFO [market_tracker] Market tracker state active_markets=2 total_subscriptions=4

2024-01-15T10:00:10Z INFO [market_tracker] Active market status
  slug=btc-updown-5m-1772649000
  window=5
  time_remaining=4m50s
  chainlink_start_price=50000.00
  yes_bid=0.5234 yes_ask=0.5245 yes_mid=0.5239
  no_bid=0.4755 no_ask=0.4766 no_mid=0.4760
  paper_trades=0

2024-01-15T10:05:00Z INFO [market_tracker] Market window ending slug=btc-updown-5m-1772649000
2024-01-15T10:05:00Z INFO [market_tracker] Market completed and archived
  chainlink_start_price=50000.00
  chainlink_end_price=50100.00
  price_change=100.00
  price_change_pct=0.20
  winning_outcome=yes
  market_result=no_position
  did_we_trade=false
  paper_pnl=0.00
```

### Market History File

Location: `data/market_history/market_history.ndjson`

Each line contains a JSON object with completed market data:

```json
{
  "slug": "btc-updown-5m-1772649000",
  "window_type": 5,
  "start_time": "2024-01-15T10:00:00Z",
  "end_time": "2024-01-15T10:05:00Z",
  "yes_asset_id": "65818619657568813474341868652308942079804919287380422192892211131408793125422",
  "no_asset_id": "71321045679252212594626385532706912750332728571942532289631379312455583992563",
  "condition_id": "0x...",
  "start_price": 50000.00,
  "end_price": 50100.00,
  "price_change": 100.00,
  "price_change_pct": 0.20,
  "winning_outcome": "yes",
  "market_result": "no_position",
  "paper_trades": [],
  "position": null,
  "pnl": 0.00,
  "did_we_trade": false,
  "yes_final_price": 0.5239,
  "no_final_price": 0.4760,
  "timestamp": "2024-01-15T10:05:00Z"
}
```

## Verification Checklist

### Gamma API Client ✓
- Can connect to gamma API
- Can fetch market by slug
- Returns valid ConditionID
- Returns valid token IDs (Up/Down)
- Extracts correct window timestamp

### Market Connector ✓
- WebSocket connects successfully
- Subscriptions work correctly
- Receives book events
- Receives price_change events
- Receives best_bid_ask events
- Updates state store correctly
- Health checks pass

### Chainlink Integration ✓
- Can fetch price at exact timestamp
- Uses correct feed ID
- Start and end prices accurate
- Price resolution matches Polymarket

### Integration ✓
- Both components work together
- State management is consistent
- Logging shows all events
- Window rotation works correctly
- No subscription leaks
- Clean startup and shutdown

### Data Quality ✓
- Orderbooks have multiple levels
- Prices in valid range [0, 1]
- Bid < Ask always
- Data is fresh (not stale)
- Updates are frequent

## Historical Analysis

The tracker provides periodic analysis of historical performance:

```
2024-01-15T11:00:00Z INFO [market_tracker] Historical performance analysis
  stats={
    "total_markets": 12,
    "trades_made": 5,
    "wins": 3,
    "losses": 2,
    "win_rate": 60.0,
    "total_pnl": 0.45,
    "avg_pnl": 0.0375
  }
```

## Architecture

```
polymarket_tracker/
├── main.go                 # Entry point
├── config.go               # Configuration parsing
├── types.go                # Data structures
├── tracker.go              # MarketTracker core
├── market_lifecycle.go     # Market subscription/end handling
├── paper_trading.go        # Paper trade execution
├── monitoring.go           # State logging and analysis
├── history.go              # Market history persistence
└── README.md               # Documentation
```

## Error Handling

- **Gamma API failures**: Log error, retry next cycle, keep existing subscriptions
- **Market not found**: Log warning, skip window (market doesn't exist yet)
- **Subscription errors**: Log error, mark market as failed, don't retry
- **WebSocket disconnect**: Connector auto-reconnects, subscriptions preserved
- **Chainlink API errors**: Log error, use 0 for price, continue tracking
- **File I/O errors**: Log error, continue in-memory operation

## Future Enhancements

1. **Real Trading**: Integrate with Polymarket CLOB for actual trade execution
2. **Advanced Strategy**: Implement trading strategies based on historical analysis
3. **Alerting**: Add alerts for significant market movements or PnL thresholds
4. **Web Dashboard**: Create real-time visualization of active markets
5. **Backtesting**: Use historical data to test and optimize strategies
6. **Multi-Feed Support**: Track multiple assets (ETH, SOL, etc.)

## Troubleshooting

### No Data Received
1. Check network connectivity
2. Verify asset IDs are correct
3. Check if assets are active on Polymarket
4. Monitor connector health

### Chainlink Price Errors
1. Verify API credentials are correct
2. Check feed ID is valid
3. Ensure REST URL is accessible
4. Check rate limits

### Stale Data
1. Check if `IsStale` flag is set
2. Verify WebSocket connection is active
3. Check logs for disconnection events

## License

Part of the pm-crypto-insurance-bot project.
