package gamma

import (
	"context"
	"fmt"
	"time"
)

func Example_client() {
	client := NewClient(ClientConfig{
		BaseURL: "https://gamma-api.polymarket.com",
	})

	ctx := context.Background()

	slug15m := GetCurrentSlug("btc-updown-", 15)
	market15m, err := client.GetMarketBySlug(ctx, slug15m)
	if err != nil {
		fmt.Printf("Error fetching 15m market: %v\n", err)
		return
	}

	fmt.Printf("15m Market:\n")
	fmt.Printf("  ID: %s\n", market15m.ID)
	fmt.Printf("  Question: %s\n", market15m.Question)
	fmt.Printf("  ConditionID: %s\n", market15m.ConditionID)
	fmt.Printf("  Slug: %s\n", market15m.Slug)
	fmt.Printf("  Up Token: %s\n", market15m.Tokens.UpTokenID)
	fmt.Printf("  Down Token: %s\n", market15m.Tokens.DownTokenID)
	fmt.Printf("  Window Start: %s\n", market15m.WindowStart.Format(time.RFC3339))

	slug5m := GetCurrentSlug("btc-updown-", 5)
	market5m, err := client.GetMarketBySlug(ctx, slug5m)
	if err != nil {
		fmt.Printf("Error fetching 5m market: %v\n", err)
		return
	}

	fmt.Printf("\n5m Market:\n")
	fmt.Printf("  ID: %s\n", market5m.ID)
	fmt.Printf("  Question: %s\n", market5m.Question)
	fmt.Printf("  Window Start: %s\n", market5m.WindowStart.Format(time.RFC3339))

	now := time.Now()
	timestamp15m := CalculateWindowTimestamp(now, 15)
	slug15mManual := BuildSlug("btc-updown-", "15m", timestamp15m)
	fmt.Printf("\nManual slug: %s\n", slug15mManual)
}
