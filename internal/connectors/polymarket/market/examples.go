package market

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/state"
)

func ExampleBasicUsage() {
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()
	stateStore := state.NewStateStore()

	cfg := &Config{
		CustomFeatureEnabled: true,
		InitialAssetIDs: []string{
			"65818619657568813474341868652308942079804919287380422192892211131408793125422",
		},
	}

	connector := NewConnector(cfg, stateStore, &logger)

	ctx := context.Background()
	if err := connector.Start(ctx); err != nil {
		fmt.Printf("Failed to start connector: %v\n", err)
		return
	}
	defer connector.Stop()

	time.Sleep(5 * time.Second)

	if healthErr := connector.Health(); healthErr != nil {
		fmt.Printf("Connector health issue: %v\n", healthErr)
	} else {
		fmt.Println("Connector is healthy")
	}

	pmState := stateStore.GetPolymarket()
	for assetID, orderbook := range pmState.OrderBooks {
		fmt.Printf("Asset: %s\n", assetID)
		fmt.Printf("  Best Bid: %.4f\n", orderbook.BestBid)
		fmt.Printf("  Best Ask: %.4f\n", orderbook.BestAsk)
		fmt.Printf("  Mid Price: %.4f\n", orderbook.MidPrice)
		fmt.Printf("  Bid Levels: %d\n", len(orderbook.Bids))
		fmt.Printf("  Ask Levels: %d\n", len(orderbook.Asks))
	}

	fmt.Printf("Subscribed Assets: %v\n", pmState.SubscribedAssets)
	fmt.Printf("Is Stale: %v\n", pmState.IsStale)
}

func ExampleDynamicSubscriptions() {
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()
	stateStore := state.NewStateStore()

	cfg := &Config{
		CustomFeatureEnabled: true,
	}

	connector := NewConnector(cfg, stateStore, &logger)
	marketConnector := connector.(*Connector)

	ctx := context.Background()
	connector.Start(ctx)
	defer connector.Stop()

	asset1 := "65818619657568813474341868652308942079804919287380422192892211131408793125422"
	asset2 := "71321045679252212594626385532706912750332728571942532289631379312455583992563"

	if err := marketConnector.SubscribeMany([]string{asset1, asset2}); err != nil {
		fmt.Printf("Failed to subscribe: %v\n", err)
		return
	}

	fmt.Printf("Subscribed to: %v\n", marketConnector.GetCurrentSubscriptions())

	time.Sleep(10 * time.Second)

	if err := marketConnector.Unsubscribe(asset1); err != nil {
		fmt.Printf("Failed to unsubscribe: %v\n", err)
		return
	}

	fmt.Printf("After unsubscribe: %v\n", marketConnector.GetCurrentSubscriptions())
}

func ExampleMultiMarketMonitoring() {
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()
	stateStore := state.NewStateStore()

	cfg := &Config{
		CustomFeatureEnabled: true,
		InitialAssetIDs: []string{
			"asset_id_1",
			"asset_id_2",
			"asset_id_3",
		},
	}

	connector := NewConnector(cfg, stateStore, &logger)

	ctx := context.Background()
	connector.Start(ctx)
	defer connector.Stop()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pmState := stateStore.GetPolymarket()

			fmt.Println("\n=== Market Monitor ===")
			for assetID, ob := range pmState.OrderBooks {
				meta := pmState.MarketAssets[assetID]
				if meta != nil {
					fmt.Printf("\nMarket: %s\n", meta.Question)
					fmt.Printf("Outcome: %s\n", meta.Outcome)
				}
				fmt.Printf("Asset ID: %s\n", assetID)
				fmt.Printf("Price: %.4f (Bid: %.4f, Ask: %.4f)\n",
					ob.MidPrice, ob.BestBid, ob.BestAsk)
				fmt.Printf("Liquidity: %d bids, %d asks\n",
					len(ob.Bids), len(ob.Asks))
				fmt.Printf("Last Update: %s\n", ob.LastUpdate.Format(time.RFC3339))
			}

			if pmState.IsStale {
				fmt.Println("\n⚠️  WARNING: Feed is stale!")
			}
		}
	}
}

func ExampleHealthMonitoring() {
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()
	stateStore := state.NewStateStore()

	cfg := &Config{
		CustomFeatureEnabled: true,
		InitialAssetIDs:      []string{"asset_id_1"},
	}

	connector := NewConnector(cfg, stateStore, &logger)

	ctx := context.Background()
	connector.Start(ctx)
	defer connector.Stop()

	healthTicker := time.NewTicker(10 * time.Second)
	defer healthTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-healthTicker.C:
			if err := connector.Health(); err != nil {
				fmt.Printf("❌ Health check failed: %v\n", err)

				pmState := stateStore.GetPolymarket()
				if pmState.IsStale {
					fmt.Println("⚠️  Market data is stale - check connection")
				}
			} else {
				fmt.Println("✅ Connector is healthy")
			}
		}
	}
}
