# Polymarket Market WebSocket Connector

Production-grade WebSocket connector for Polymarket Market Channel with dynamic subscription management and multi-market state tracking.

## Features

- **Dynamic Subscriptions**: Subscribe/unsubscribe to multiple assets at runtime
- **All Event Types**: Supports all 7 Market Channel event types
- **Multi-Market State**: Tracks orderbooks for multiple assets simultaneously
- **Auto-Reconnect**: Automatic reconnection with exponential backoff
- **Thread-Safe**: Concurrent-safe operations with proper mutex handling
- **State Management**: Integrates with StateStore for consistent state updates

## Event Types Supported

1. **book** - Orderbook snapshots with bids/asks
2. **price_change** - Price level changes
3. **tick_size_change** - Tick size adjustments
4. **last_trade_price** - Trade executions
5. **best_bid_ask** - Best bid/ask updates (requires `custom_feature_enabled`)
6. **new_market** - New market creation (requires `custom_feature_enabled`)
7. **market_resolved** - Market resolution (requires `custom_feature_enabled`)

## Usage

### Basic Setup

```go
import (
    "context"
    "github.com/srkyaganti/pm-crypto-insurance-bot/internal/connectors/polymarket/market"
    "github.com/srkyaganti/pm-crypto-insurance-bot/internal/state"
    "github.com/rs/zerolog"
)

func main() {
    logger := zerolog.New(os.Stdout)
    stateStore := state.NewStateStore()
    
    cfg := &market.Config{
        CustomFeatureEnabled: true,
        InitialAssetIDs: []string{
            "65818619657568813474341868652308942079804919287380422192892211131408793125422",
        },
    }
    
    connector := market.NewConnector(cfg, stateStore, &logger)
    
    ctx := context.Background()
    if err := connector.Start(ctx); err != nil {
        log.Fatal(err)
    }
    defer connector.Stop()
    
    // Keep running...
    select {}
}
```

### Dynamic Subscriptions

```go
// Subscribe to new assets
err := connector.Subscribe("another_asset_id")
if err != nil {
    log.Error(err)
}

// Subscribe to multiple assets
err = connector.SubscribeMany([]string{"asset1", "asset2", "asset3"})

// Unsubscribe
err = connector.Unsubscribe("asset_id")

// Get current subscriptions
subs := connector.GetCurrentSubscriptions()
```

### Accessing State

```go
// Get all orderbooks
polymarketState := stateStore.GetPolymarket()
for assetID, orderbook := range polymarketState.OrderBooks {
    fmt.Printf("Asset: %s, Best Bid: %f, Best Ask: %f\n", 
        assetID, orderbook.BestBid, orderbook.BestAsk)
}

// Get specific orderbook
if ob := polymarketState.OrderBooks["asset_id"]; ob != nil {
    fmt.Printf("Mid Price: %f\n", ob.MidPrice)
    fmt.Printf("Bids: %d levels\n", len(ob.Bids))
    fmt.Printf("Asks: %d levels\n", len(ob.Asks))
}

// Get market metadata
if meta := polymarketState.MarketAssets["asset_id"]; meta != nil {
    fmt.Printf("Question: %s\n", meta.Question)
    fmt.Printf("Condition ID: %s\n", meta.ConditionID)
    fmt.Printf("Outcome: %s\n", meta.Outcome)
}
```

### Health Monitoring

```go
// Check connector health
if err := connector.Health(); err != nil {
    log.Warnf("Connector health issue: %v", err)
}

// Check if running
if connector.IsRunning() {
    fmt.Println("Connector is running")
}
```

## Configuration

```go
type Config struct {
    WsURL                string        // WebSocket URL (default: wss://ws-subscriptions-clob.polymarket.com/ws/market)
    CustomFeatureEnabled bool          // Enable best_bid_ask, new_market, market_resolved events (default: true)
    ReconnectMinWait     time.Duration // Min reconnect backoff (default: 1s)
    ReconnectMaxWait     time.Duration // Max reconnect backoff (default: 30s)
    PingPeriod           time.Duration // Ping interval (default: 20s)
    PongWait             time.Duration // Pong wait timeout (default: 30s)
    WriteWait            time.Duration // Write timeout (default: 10s)
    InitialAssetIDs      []string      // Assets to subscribe on connect
}
```

## State Structure

The connector updates `PolymarketState` in the StateStore:

```go
type PolymarketState struct {
    OrderBooks       map[string]*PMOrderBook      // assetID -> orderbook
    MarketAssets     map[string]*MarketAssetMeta  // assetID -> metadata
    SubscribedAssets []string                      // List of subscribed asset IDs
    LastWSUpdate     time.Time                     // Last WebSocket update
    ActiveOrders     map[string]*OrderStatus       // orderID -> status (for future use)
    IsStale          bool                          // Feed staleness flag
}

type PMOrderBook struct {
    Bids       []PMOrderLevel
    Asks       []PMOrderLevel
    BestBid    float64
    BestAsk    float64
    MidPrice   float64
    LastUpdate time.Time
}

type MarketAssetMeta struct {
    AssetID     string
    ConditionID string
    Outcome     string  // "Yes" or "No"
    Question    string
}
```

## Event Flow

1. WebSocket receives message
2. Parse `event_type` field
3. Unmarshal to specific event struct
4. Call registered event handler
5. Update StateStore
6. Log event

## Error Handling

- Connection failures trigger automatic reconnection
- Parse errors are logged but don't stop the connector
- Subscription errors are returned to caller
- Staleness detection (5s without updates)

## Best Practices

1. **Initial Subscriptions**: Use `InitialAssetIDs` in config for assets needed at startup
2. **State Access**: Always check if orderbook exists before accessing (nil check)
3. **Graceful Shutdown**: Always call `Stop()` when done
4. **Health Checks**: Monitor connector health in production
5. **Context**: Use context for cancellation and timeout control

## Testing

Run tests:
```bash
go test ./internal/connectors/polymarket/market/... -v
```

## Architecture

```
market/
├── types.go           # Event structures and config
├── parser.go          # Event parsing functions
├── websocket.go       # Low-level WebSocket client
├── subscription.go    # Subscription manager
├── connector.go       # High-level connector with state management
├── parser_test.go     # Parser tests
└── subscription_test.go # Subscription manager tests
```

## Integration with Existing Codebase

The connector follows the same patterns as existing Binance connectors:
- Implements `connectors.Connector` interface
- Uses `StateStore` for state management
- Uses `zerolog` for structured logging
- Thread-safe operations with mutex/atomic
- Context-based lifecycle management
