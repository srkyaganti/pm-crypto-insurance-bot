# Integration Guide: Polymarket Market WebSocket

This guide shows how to integrate the Polymarket Market WebSocket connector into your trading bot application.

## Step 1: Update Configuration

Add Polymarket configuration to your config file (e.g., `configs/config.yaml`):

```yaml
polymarket:
  clob_url: "https://clob.polymarket.com"
  ws_url: "wss://ws-subscriptions-clob.polymarket.com/ws/market"
  gamma_url: "https://gamma-api.polymarket.com"
  api_key: "${POLYMARKET_API_KEY}"
  api_secret: "${POLYMARKET_API_SECRET}"
  passphrase: "${POLYMARKET_PASSPHRASE}"
  private_key: "${POLYMARKET_PRIVATE_KEY}"
  
  # Market WebSocket specific settings
  market_ws:
    custom_feature_enabled: true
    initial_asset_ids:
      - "65818619657568813474341868652308942079804919287380422192892211131408793125422"
```

## Step 2: Initialize in Application

In your `internal/app/app.go` or similar:

```go
import (
    "github.com/srkyaganti/pm-crypto-insurance-bot/internal/connectors/polymarket/market"
    "github.com/srkyaganti/pm-crypto-insurance-bot/internal/config"
    "github.com/srkyaganti/pm-crypto-insurance-bot/internal/state"
)

type App struct {
    stateStore      *state.StateStore
    connectors      map[string]connectors.Connector
}

func (a *App) initializePolymarketMarket(cfg *config.Config) error {
    marketCfg := &market.Config{
        WsURL:                cfg.Polymarket.WsURL,
        CustomFeatureEnabled: cfg.Polymarket.MarketWs.CustomFeatureEnabled,
        InitialAssetIDs:      cfg.Polymarket.MarketWs.InitialAssetIDs,
    }
    
    marketConnector := market.NewConnector(marketCfg, a.stateStore, a.logger)
    a.connectors["polymarket_market"] = marketConnector
    
    return nil
}
```

## Step 3: Start the Connector

```go
func (a *App) Start(ctx context.Context) error {
    // Initialize connectors
    if err := a.initializePolymarketMarket(a.config); err != nil {
        return err
    }
    
    // Start all connectors
    for name, conn := range a.connectors {
        a.logger.Info().Str("connector", name).Msg("starting connector")
        if err := conn.Start(ctx); err != nil {
            return fmt.Errorf("start connector %s: %w", name, err)
        }
    }
    
    return nil
}
```

## Step 4: Use Dynamic Subscriptions

Access the connector for dynamic subscriptions:

```go
func (a *App) SubscribeToMarket(assetID string) error {
    conn := a.connectors["polymarket_market"]
    
    // Type assert to access market-specific methods
    if marketConn, ok := conn.(*market.Connector); ok {
        return marketConn.Subscribe(assetID)
    }
    
    return fmt.Errorf("connector is not a market connector")
}

func (a *App) UnsubscribeFromMarket(assetID string) error {
    conn := a.connectors["polymarket_market"]
    
    if marketConn, ok := conn.(*market.Connector); ok {
        return marketConn.Unsubscribe(assetID)
    }
    
    return fmt.Errorf("connector is not a market connector")
}
```

## Step 5: Access Market Data

```go
func (a *App) GetMarketPrice(assetID string) (float64, error) {
    pmState := a.stateStore.GetPolymarket()
    
    orderbook, exists := pmState.OrderBooks[assetID]
    if !exists {
        return 0, fmt.Errorf("no orderbook for asset %s", assetID)
    }
    
    return orderbook.MidPrice, nil
}

func (a *App) GetBestBidAsk(assetID string) (bid, ask float64, err error) {
    pmState := a.stateStore.GetPolymarket()
    
    orderbook, exists := pmState.OrderBooks[assetID]
    if !exists {
        return 0, 0, fmt.Errorf("no orderbook for asset %s", assetID)
    }
    
    return orderbook.BestBid, orderbook.BestAsk, nil
}

func (a *App) GetAllMarketPrices() map[string]float64 {
    pmState := a.stateStore.GetPolymarket()
    prices := make(map[string]float64)
    
    for assetID, ob := range pmState.OrderBooks {
        prices[assetID] = ob.MidPrice
    }
    
    return prices
}
```

## Step 6: Monitor Health

```go
func (a *App) monitorHealth(ctx context.Context) {
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()
    
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            for name, conn := range a.connectors {
                if err := conn.Health(); err != nil {
                    a.logger.Error().
                        Str("connector", name).
                        Err(err).
                        Msg("connector health check failed")
                }
            }
        }
    }
}
```

## Step 7: Graceful Shutdown

```go
func (a *App) Stop() error {
    a.logger.Info().Msg("stopping application")
    
    var wg sync.WaitGroup
    errCh := make(chan error, len(a.connectors))
    
    for name, conn := range a.connectors {
        wg.Add(1)
        go func(name string, conn connectors.Connector) {
            defer wg.Done()
            a.logger.Info().Str("connector", name).Msg("stopping connector")
            if err := conn.Stop(); err != nil {
                errCh <- fmt.Errorf("stop %s: %w", name, err)
            }
        }(name, conn)
    }
    
    wg.Wait()
    close(errCh)
    
    for err := range errCh {
        if err != nil {
            return err
        }
    }
    
    a.logger.Info().Msg("application stopped")
    return nil
}
```

## Step 8: Example Trading Logic

```go
func (a *App) analyzeMarket(assetID string) {
    pmState := a.stateStore.GetPolymarket()
    
    orderbook := pmState.OrderBooks[assetID]
    if orderbook == nil {
        a.logger.Warn().Str("asset", assetID).Msg("no orderbook data")
        return
    }
    
    // Calculate spread
    spread := orderbook.BestAsk - orderbook.BestBid
    spreadPct := (spread / orderbook.MidPrice) * 100
    
    // Log market state
    a.logger.Info().
        Str("asset", assetID).
        Float64("mid_price", orderbook.MidPrice).
        Float64("spread", spread).
        Float64("spread_pct", spreadPct).
        Int("bid_levels", len(orderbook.Bids)).
        Int("ask_levels", len(orderbook.Asks)).
        Msg("market analysis")
    
    // Check for arbitrage or trading opportunities
    if spreadPct < 1.0 {
        a.logger.Info().
            Str("asset", assetID).
            Msg("tight spread detected - potential trading opportunity")
    }
    
    // Check market metadata
    if meta := pmState.MarketAssets[assetID]; meta != nil {
        a.logger.Debug().
            Str("question", meta.Question).
            Str("outcome", meta.Outcome).
            Msg("market metadata")
    }
}
```

## Step 9: Event-Driven Logic

Subscribe to state changes:

```go
func (a *App) watchMarketChanges(ctx context.Context) {
    ticker := time.NewTicker(1 * time.Second)
    defer ticker.Stop()
    
    var lastUpdate time.Time
    
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            pmState := a.stateStore.GetPolymarket()
            
            if pmState.LastWSUpdate.After(lastUpdate) {
                // New data received
                lastUpdate = pmState.LastWSUpdate
                
                // Trigger analysis for all subscribed markets
                for _, assetID := range pmState.SubscribedAssets {
                    a.analyzeMarket(assetID)
                }
            }
        }
    }
}
```

## Testing the Integration

Create a test to verify the integration:

```go
func TestPolymarketMarketIntegration(t *testing.T) {
    cfg := &config.Config{
        Polymarket: config.PolymarketConfig{
            WsURL: "wss://ws-subscriptions-clob.polymarket.com/ws/market",
            MarketWs: config.MarketWsConfig{
                CustomFeatureEnabled: true,
                InitialAssetIDs: []string{"test_asset_id"},
            },
        },
    }
    
    logger := zerolog.New(os.Stdout)
    stateStore := state.NewStateStore()
    
    app := &App{
        config:     cfg,
        stateStore: stateStore,
        logger:     &logger,
        connectors: make(map[string]connectors.Connector),
    }
    
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    
    if err := app.initializePolymarketMarket(cfg); err != nil {
        t.Fatalf("Failed to initialize: %v", err)
    }
    
    if err := app.Start(ctx); err != nil {
        t.Fatalf("Failed to start: %v", err)
    }
    defer app.Stop()
    
    // Wait for data
    time.Sleep(5 * time.Second)
    
    // Verify state
    pmState := stateStore.GetPolymarket()
    if len(pmState.SubscribedAssets) == 0 {
        t.Error("No assets subscribed")
    }
}
```

## Troubleshooting

### Connection Issues

1. Check network connectivity
2. Verify WebSocket URL is correct
3. Check firewall settings

### No Data Received

1. Verify asset IDs are correct
2. Check if assets are active on Polymarket
3. Monitor connector health

### Stale Data

1. Check if `IsStale` flag is set
2. Verify WebSocket connection is active
3. Check logs for disconnection events

## Best Practices

1. **Always handle errors** from Subscribe/Unsubscribe
2. **Check for nil** when accessing orderbooks from state
3. **Use context** for cancellation and timeout
4. **Monitor health** regularly in production
5. **Log important events** for debugging
6. **Test with real asset IDs** from Polymarket

## Next Steps

After integrating the Market WebSocket:

1. Implement the User Channel WebSocket for order tracking
2. Add REST API client for order placement
3. Integrate Gamma API client for market discovery
4. Build trading logic on top of the data feeds
5. Add persistence layer for historical data
