# Polymarket Market Tracker Connector

Timer-based connector that orchestrates Polymarket market tracking with Chainlink price verification and complete market lifecycle data collection.

## Overview

This connector integrates with the main application as a proper connector that:
- Fetches ongoing markets using the Gamma API
- Subscribes to Yes/No asset IDs using the Market WebSocket connector
- Uses Chainlink prices at exact timestamps for market resolution
- Tracks active markets with timer-based rotation (5m, 15m windows)
- Logs detailed market data for verification via persistence system
- Stores historical data for strategy development

## Architecture

The Tracker connector orchestrates three other connectors:
1. **Gamma Client**: REST API client for Polymarket Gamma API
2. **Market Connector**: WebSocket connector for Polymarket market data
3. **Chainlink Connector**: WebSocket connector for Chainlink price feeds

## Features

### 1. Timer-Based Market Rotation
- Markets rotate exactly at window boundaries using `time.AfterFunc`
- Pre-scheduled future windows (configurable, default: 3 windows)
- No polling overhead

### 2. Chainlink Price Integration
- Uses `GetReportAtTimestampForFeed(feedID, timestamp)` for exact prices
- Start price at market START timestamp
- End price at market END timestamp
- Source of truth for market resolution

### 3. Market Lifecycle Tracking
```
Start → Fetch Gamma → Get Chainlink Start Price → Subscribe → 
Set End Timer → Pre-schedule Future → [Monitor] → 
End Timer Fires → Get Chainlink End Price → Calculate Outcome → 
Archive → Unsubscribe → Subscribe Next Window
```

### 4. Paper Trading Support
- `ExecutePaperTrade()` method for strategy testing
- Position tracking per market
- PnL calculation based on Chainlink resolution
- All trades logged via persistence system

### 5. Data Persistence
- Market history stored as persistence events
- One event per completed market with all data
- In-memory cache for fast access to recent markets
- Automatic rotation and flush via persistence system

## Configuration

### Configuration Structure

```go
type TrackerConfig struct {
    MarketPrefixes     []string  // Market prefixes to track (e.g., ["btc-updown-"])
    Windows            []int     // Window sizes in minutes (e.g., [5, 15])
    PreScheduleWindows int       // Number of future windows to pre-schedule (default: 3)
    CustomFeatures     bool      // Enable custom features
}
```

### YAML Configuration

```yaml
polymarket:
  tracker:
    enabled: true
    market_prefixes:
      - "btc-updown-"
    windows:
      - 5
      - 15
    pre_schedule_windows: 3
    custom_features: true
```

## Market Resolution Logic

Uses Chainlink prices as source of truth:
- **Start Price**: Chainlink BTC price at market start timestamp
- **End Price**: Chainlink BTC price at market end timestamp
- **Price Change**: End - Start
- **Winning Outcome**: 
  - "yes" if Price Change > 0 (price went up)
  - "no" if Price Change <= 0 (price went down/same)

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

## Usage

### Initialize in Main App

The tracker is automatically initialized by the main app if `polymarket.tracker.enabled` is true in config.

```go
// In internal/app/app.go
if a.config.Polymarket.Tracker.Enabled {
    trackerConnector := tracker.NewConnector(
        gammaClient,
        marketConnector,
        chainlinkConnector,
        trackerCfg,
        feedID,
        a.state,
        a.persistence,
        logger,
    )
    a.connectors = append(a.connectors, trackerConnector)
}
```

### Execute Paper Trades

```go
// Get tracker connector from app
trackerConn := app.Connectors()[3].(*tracker.Connector)

// Execute paper trade
err := trackerConn.ExecutePaperTrade(
    "btc-updown-5m-1772696700",  // slug
    "6581861965756881347...",    // yes asset ID
    "buy",                       // side
    "Strategy: trend following", // reason
    10.0,                        // size
)
```

### Monitor Statistics

```go
stats := trackerConn.GetStats()
// Returns:
// {
//   "active_markets": 2,
//   "total_markets": 10,
//   "trades_made": 5,
//   "wins": 3,
//   "losses": 2,
//   "win_rate": 60.0,
//   "total_pnl": 0.45,
//   "avg_pnl": 0.045
// }
```

### Get Active Markets

```go
activeSlugs := trackerConn.GetCurrentActiveMarkets()
// Returns: ["btc-updown-5m-1772696700", "btc-updown-15m-1772696700"]

market := trackerConn.GetActiveMarket("btc-updown-5m-1772696700")
// Returns ActiveMarket struct with:
// - StartPrice, EndPrice
// - YesAssetID, NoAssetID
// - StartTime, EndTime
// - MyPosition (if any)
// - PaperTrades
```

## Persistence Events

### Market Completed Event

When a market window ends, a `market_completed` event is logged:

```json
{
  "ts": "2024-01-15T10:05:00Z",
  "component": "polymarket_tracker",
  "event_type": "STATE_EVENT",
  "msg": "market_completed",
  "payload": {
    "slug": "btc-updown-5m-1772696700",
    "window_type": 5,
    "start_time": "2024-01-15T10:00:00Z",
    "end_time": "2024-01-15T10:05:00Z",
    "start_price": 50000.00,
    "end_price": 50100.00,
    "price_change": 100.00,
    "price_change_pct": 0.20,
    "winning_outcome": "yes",
    "market_result": "no_position",
    "did_we_trade": false,
    "pnl": 0.00,
    "num_paper_trades": 0,
    "yes_final_price": 0.5239,
    "no_final_price": 0.4760
  }
}
```

### Paper Trade Event

When a paper trade is executed:

```json
{
  "ts": "2024-01-15T10:02:30Z",
  "component": "polymarket_tracker",
  "event_type": "DECISION_EVENT",
  "msg": "paper_trade_executed",
  "payload": {
    "slug": "btc-updown-5m-1772696700",
    "asset": "6581861965756881347...",
    "side": "buy",
    "price": 0.52,
    "size": 10.0,
    "reason": "Strategy: trend following"
  }
}
```

## Connector Lifecycle

### Start
1. Subscribe to current windows (5m, 15m)
2. Fetch market data from Gamma API
3. Get Chainlink start prices
4. Set end timers
5. Pre-schedule future windows
6. Start monitoring goroutines

### Run
1. Monitor state every 10 seconds
2. Analyze performance every 1 minute
3. Handle timer fires at market end
4. Rotate to next window

### Stop
1. Cancel all timers
2. Unsubscribe from all markets
3. Wait for goroutines to finish
4. Log final statistics

## Health Checks

```go
err := trackerConnector.Health()
// Checks:
// - Connector is running
// - Market connector is healthy
```

## Testing

Run the main application with tracker enabled:

```bash
# 1. Set environment variables
export CHAINLINK_API_KEY="..."
export CHAINLINK_API_SECRET="..."
export CHAINLINK_FEED_ID="0x..."
export POLYMARKET_GAMMA_URL="https://gamma-api.polymarket.com"

# 2. Run app
go run cmd/engine/main.go -config configs/config.yaml

# 3. Check logs for tracker events
tail -f logs/engine.log | grep "polymarket_tracker"

# 4. Verify persistence file has market_completed events
cat logs/events.ndjson | jq 'select(.component=="polymarket_tracker")'
```

## Monitoring

### Logs

The tracker logs at various levels:
- **Info**: Market lifecycle events, rotations, completions
- **Debug**: Chainlink price fetches, timer fires
- **Warn**: Stale data, missing markets
- **Error**: Failed operations

### Metrics

Access via `GetStats()`:
- Active markets count
- Total markets tracked
- Trades made
- Win rate
- Total PnL
- Average PnL

## Troubleshooting

### No Markets Tracked
- Check `polymarket.tracker.enabled` is true
- Verify market prefixes are correct
- Check Gamma API is accessible

### Chainlink Price Errors
- Verify Chainlink connector is healthy
- Check feed ID is correct
- Ensure API credentials are valid

### Market Rotation Not Working
- Check logs for timer fires
- Verify window durations are correct
- Check market connector is healthy

## Future Enhancements

1. **Real Trading**: Integrate with Polymarket CLOB for actual trade execution
2. **Advanced Strategy**: Implement trading strategies based on historical analysis
3. **Alerting**: Add alerts for significant market movements or PnL thresholds
4. **Web Dashboard**: Create real-time visualization of active markets
5. **Backtesting**: Use historical data to test and optimize strategies
6. **Multi-Feed Support**: Track multiple assets (ETH, SOL, etc.)

## Dependencies

- **Gamma Client**: `internal/connectors/polymarket/gamma`
- **Market Connector**: `internal/connectors/polymarket/market`
- **Chainlink Connector**: `internal/connectors/chainlink`
- **State Store**: `internal/state`
- **Persistence Writer**: `internal/persistence`

## Notes

- The tracker does NOT create its own connectors; it re-uses existing ones
- All subscriptions are managed by the tracker (market connector starts with empty InitialAssetIDs)
- Market history is kept in memory for fast access; persisted via events for durability
- Paper trades are for strategy testing only; no real execution
- The tracker follows the connector pattern exactly like other connectors in the system
