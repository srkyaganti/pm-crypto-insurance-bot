package main

import (
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestConfigValidation(t *testing.T) {
	cfg := &Config{
		GammaURL:           "https://gamma-api.polymarket.com",
		MarketWsURL:        "wss://ws-subscriptions-clob.polymarket.com/ws/market",
		ChainlinkRestURL:   "https://test.com",
		ChainlinkApiKey:    "test-key",
		ChainlinkApiSecret: "test-secret",
		BTCFeedID:          "test-feed-id",
		MarketPrefixes:     []string{"btc-updown-"},
		Windows:            []int{5, 15},
	}

	if err := cfg.Validate(); err != nil {
		t.Errorf("Config validation failed: %v", err)
	}
}

func TestConfigValidationMissingFields(t *testing.T) {
	cfg := &Config{
		GammaURL: "https://gamma-api.polymarket.com",
	}

	if err := cfg.Validate(); err == nil {
		t.Error("Expected config validation to fail with missing fields")
	}
}

func TestMarketHistory(t *testing.T) {
	logger := zerolog.Nop()
	history := NewMarketHistory(&logger, t.TempDir())

	market := &CompletedMarket{
		Slug:           "test-market",
		WindowType:     5,
		StartTime:      time.Now(),
		EndTime:        time.Now().Add(5 * time.Minute),
		StartPrice:     50000.0,
		EndPrice:       50100.0,
		PriceChange:    100.0,
		PriceChangePct: 0.2,
		WinningOutcome: "yes",
		MarketResult:   "no_position",
		DidWeTrade:     false,
		Timestamp:      time.Now(),
	}

	if err := history.Add(market); err != nil {
		t.Errorf("Failed to add market to history: %v", err)
	}

	if history.Count() != 1 {
		t.Errorf("Expected 1 market in history, got %d", history.Count())
	}

	recent := history.GetRecent(1)
	if len(recent) != 1 {
		t.Errorf("Expected 1 recent market, got %d", len(recent))
	}

	if recent[0].Slug != "test-market" {
		t.Errorf("Expected slug 'test-market', got '%s'", recent[0].Slug)
	}
}

func TestMarketHistoryStats(t *testing.T) {
	logger := zerolog.Nop()
	history := NewMarketHistory(&logger, t.TempDir())

	for i := 0; i < 5; i++ {
		pnl := 0.1
		if i%2 == 0 {
			pnl = -0.05
		}

		market := &CompletedMarket{
			Slug:           "test-market-" + string(rune(i)),
			WindowType:     5,
			StartTime:      time.Now(),
			EndTime:        time.Now().Add(5 * time.Minute),
			PnL:            pnl,
			DidWeTrade:     true,
			WinningOutcome: "yes",
			Timestamp:      time.Now(),
		}

		if err := history.Add(market); err != nil {
			t.Errorf("Failed to add market: %v", err)
		}
	}

	stats := history.GetStats()

	if stats["total_markets"].(int) != 5 {
		t.Errorf("Expected 5 total markets, got %v", stats["total_markets"])
	}

	if stats["trades_made"].(int) != 5 {
		t.Errorf("Expected 5 trades, got %v", stats["trades_made"])
	}
}

func TestPaperTrade(t *testing.T) {
	// This is a unit test for the data structures
	trade := PaperTrade{
		Timestamp: time.Now(),
		AssetID:   "test-asset",
		Side:      "buy",
		Price:     0.52,
		Size:      10.0,
		Reason:    "test trade",
	}

	if trade.AssetID != "test-asset" {
		t.Errorf("Expected asset ID 'test-asset', got '%s'", trade.AssetID)
	}

	if trade.Side != "buy" {
		t.Errorf("Expected side 'buy', got '%s'", trade.Side)
	}
}

func TestPosition(t *testing.T) {
	position := &Position{
		AssetID:    "test-asset",
		Side:       "yes",
		EntryPrice: 0.52,
		Size:       10.0,
		OpenedAt:   time.Now(),
	}

	if position.Side != "yes" {
		t.Errorf("Expected side 'yes', got '%s'", position.Side)
	}

	// Calculate PnL for winning position
	winPnL := position.Size * (1.0 - position.EntryPrice)
	expectedPnL := 10.0 * 0.48 // 4.8
	if winPnL != expectedPnL {
		t.Errorf("Expected win PnL %f, got %f", expectedPnL, winPnL)
	}

	// Calculate PnL for losing position
	lossPnL := -position.Size * position.EntryPrice
	expectedLossPnL := -5.2
	if lossPnL != expectedLossPnL {
		t.Errorf("Expected loss PnL %f, got %f", expectedLossPnL, lossPnL)
	}
}
