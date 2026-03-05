# Quick Start Guide: Polymarket Market Tracker

## Build

```bash
go build -o bin/polymarket_tracker ./cmd/integration_tests/polymarket_tracker
```

## Required Environment Variables

```bash
export CHAINLINK_REST_URL="https://your-chainlink-rest-url"
export CHAINLINK_API_KEY="your-api-key"
export CHAINLINK_API_SECRET="your-api-secret"
export BTC_USD_FEED_ID="0xyour-btc-usd-feed-id"
```

## Run

### Option 1: Using the script
```bash
./scripts/run_polymarket_tracker.sh
```

### Option 2: Direct execution
```bash
./bin/polymarket_tracker \
  -chainlink-rest-url $CHAINLINK_REST_URL \
  -chainlink-api-key $CHAINLINK_API_KEY \
  -chainlink-api-secret $CHAINLINK_API_SECRET \
  -btc-feed-id $BTC_USD_FEED_ID
```

### Option 3: With all options
```bash
./bin/polymarket_tracker \
  -gamma-url "https://gamma-api.polymarket.com" \
  -ws-url "wss://ws-subscriptions-clob.polymarket.com/ws/market" \
  -chainlink-rest-url $CHAINLINK_REST_URL \
  -chainlink-api-key $CHAINLINK_API_KEY \
  -chainlink-api-secret $CHAINLINK_API_SECRET \
  -btc-feed-id $BTC_USD_FEED_ID \
  -prefix btc-updown- \
  -window 5 -window 15 \
  -pre-schedule 3 \
  -data-dir ./data/market_history \
  -log-interval 10s \
  -log-level info \
  -custom-features true
```

## Verify It's Working

1. **Check logs** for successful startup:
   ```
   INFO [market_tracker] Starting Polymarket market tracker
   INFO [polymarket_market] Connected to websocket
   INFO [market_tracker] Successfully subscribed to market slug=btc-updown-5m-xxxxx
   ```

2. **Check market data** is being received:
   ```
   INFO [market_tracker] Active market status
     slug=btc-updown-5m-xxxxx
     chainlink_start_price=50000.00
     yes_bid=0.5234 yes_ask=0.5245
   ```

3. **Check window rotation** (wait 5 minutes for 5m window):
   ```
   INFO [market_tracker] Market window ending slug=btc-updown-5m-xxxxx
   INFO [market_tracker] Market completed and archived
   ```

4. **Check data file**:
   ```bash
   cat data/market_history/market_history.ndjson | jq .
   ```

## Stop the Tracker

Press `Ctrl+C` to gracefully shutdown. You should see:
```
INFO [market_tracker] Shutdown signal received
INFO [market_tracker] Stopping market tracker
INFO [market_tracker] Market tracker stopped
```

## What Gets Tracked

For each market window:
- **Start Price**: Chainlink BTC price at market start
- **End Price**: Chainlink BTC price at market end
- **Market Prices**: Polymarket Yes/No prices
- **Outcome**: "yes" if price went up, "no" if down
- **Timestamps**: Precise timing data

## Historical Data

Located in: `data/market_history/market_history.ndjson`

Each line is a complete market record:
```json
{
  "slug": "btc-updown-5m-1772649000",
  "window_type": 5,
  "start_time": "2024-01-15T10:00:00Z",
  "end_time": "2024-01-15T10:05:00Z",
  "start_price": 50000.00,
  "end_price": 50100.00,
  "price_change": 100.00,
  "winning_outcome": "yes",
  "yes_final_price": 0.5239,
  "no_final_price": 0.4760
}
```

## Analyze Data

```bash
# Count markets
cat data/market_history/market_history.ndjson | wc -l

# View last 5 markets
tail -5 data/market_history/market_history.ndjson | jq .

# Extract specific fields
cat data/market_history/market_history.ndjson | jq '{slug, price_change, winning_outcome}'
```

## Tests

Run unit tests:
```bash
go test ./cmd/integration_tests/polymarket_tracker -v
```

All tests should pass:
```
PASS: TestConfigValidation
PASS: TestConfigValidationMissingFields
PASS: TestMarketHistory
PASS: TestMarketHistoryStats
PASS: TestPaperTrade
PASS: TestPosition
```

## Troubleshooting

### "Configuration error"
- Ensure all required environment variables are set
- Check CHAINLINK_REST_URL, API_KEY, API_SECRET, BTC_USD_FEED_ID

### "Failed to get Chainlink price"
- Verify Chainlink API credentials
- Check feed ID is correct
- Ensure REST URL is accessible

### "No orderbook data"
- Wait 10-15 seconds for initial data to arrive
- Check network connectivity
- Verify WebSocket URL is correct

### "Market not found" from Gamma API
- Normal if market doesn't exist yet
- Will retry on next window

## Architecture Overview

```
┌─────────────────────────────────────────────┐
│           MarketTracker                     │
│                                             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐ │
│  │  Gamma   │  │ Market   │  │Chainlink │ │
│  │  Client  │  │Connector │  │ Client   │ │
│  └──────────┘  └──────────┘  └──────────┘ │
│       ↓             ↓              ↓       │
│  Fetch         Subscribe      Get Price    │
│  Market        to Assets      at Timestamp │
│       ↓             ↓              ↓       │
│  ┌────────────────────────────────────┐   │
│  │      Active Markets (with timers)  │   │
│  └────────────────────────────────────┘   │
│       ↓                                    │
│  ┌────────────────────────────────────┐   │
│  │     Market History (NDJSON file)   │   │
│  └────────────────────────────────────┘   │
└─────────────────────────────────────────────┘
```

## Next Steps

1. **Run for 30+ minutes** to see multiple window rotations
2. **Analyze historical data** to understand market patterns
3. **Implement paper trading strategy** based on collected data
4. **Tune parameters** (window sizes, prefixes, etc.)

## Support

See full documentation:
- `cmd/integration_tests/polymarket_tracker/README.md`
- `cmd/integration_tests/polymarket_tracker/IMPLEMENTATION_SUMMARY.md`
