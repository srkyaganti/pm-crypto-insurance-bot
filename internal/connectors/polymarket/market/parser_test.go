package market

import (
	"testing"
)

func TestParseBookEvent(t *testing.T) {
	data := []byte(`{
		"event_type": "book",
		"asset_id": "12345",
		"market": "0xabc",
		"bids": [{"price": "0.48", "size": "30"}],
		"asks": [{"price": "0.52", "size": "25"}],
		"timestamp": "123456789000",
		"hash": "0x123"
	}`)

	event, err := ParseBookEvent(data)
	if err != nil {
		t.Fatalf("ParseBookEvent failed: %v", err)
	}

	if event.EventType != "book" {
		t.Errorf("Expected event_type 'book', got '%s'", event.EventType)
	}
	if event.AssetID != "12345" {
		t.Errorf("Expected asset_id '12345', got '%s'", event.AssetID)
	}
	if len(event.Bids) != 1 {
		t.Errorf("Expected 1 bid, got %d", len(event.Bids))
	}
	if event.Bids[0].Price != "0.48" {
		t.Errorf("Expected bid price '0.48', got '%s'", event.Bids[0].Price)
	}
}

func TestParsePriceChangeEvent(t *testing.T) {
	data := []byte(`{
		"event_type": "price_change",
		"market": "0xabc",
		"price_changes": [
			{
				"asset_id": "12345",
				"price": "0.5",
				"size": "200",
				"side": "BUY",
				"hash": "abc123",
				"best_bid": "0.5",
				"best_ask": "1"
			}
		],
		"timestamp": "1757908892351"
	}`)

	event, err := ParsePriceChangeEvent(data)
	if err != nil {
		t.Fatalf("ParsePriceChangeEvent failed: %v", err)
	}

	if event.EventType != "price_change" {
		t.Errorf("Expected event_type 'price_change', got '%s'", event.EventType)
	}
	if len(event.PriceChanges) != 1 {
		t.Errorf("Expected 1 price change, got %d", len(event.PriceChanges))
	}
	if event.PriceChanges[0].AssetID != "12345" {
		t.Errorf("Expected asset_id '12345', got '%s'", event.PriceChanges[0].AssetID)
	}
}

func TestParseBestBidAskEvent(t *testing.T) {
	data := []byte(`{
		"event_type": "best_bid_ask",
		"market": "0xabc",
		"asset_id": "12345",
		"best_bid": "0.73",
		"best_ask": "0.77",
		"spread": "0.04",
		"timestamp": "1766789469958"
	}`)

	event, err := ParseBestBidAskEvent(data)
	if err != nil {
		t.Fatalf("ParseBestBidAskEvent failed: %v", err)
	}

	if event.EventType != "best_bid_ask" {
		t.Errorf("Expected event_type 'best_bid_ask', got '%s'", event.EventType)
	}
	if event.BestBid != "0.73" {
		t.Errorf("Expected best_bid '0.73', got '%s'", event.BestBid)
	}
	if event.BestAsk != "0.77" {
		t.Errorf("Expected best_ask '0.77', got '%s'", event.BestAsk)
	}
}

func TestParseNewMarketEvent(t *testing.T) {
	data := []byte(`{
		"id": "1031769",
		"question": "Will NVIDIA close above $240?",
		"market": "0xabc",
		"slug": "nvda-above-240",
		"description": "This market will resolve to Yes if...",
		"assets_ids": ["12345", "67890"],
		"outcomes": ["Yes", "No"],
		"timestamp": "1766790415550",
		"event_type": "new_market"
	}`)

	event, err := ParseNewMarketEvent(data)
	if err != nil {
		t.Fatalf("ParseNewMarketEvent failed: %v", err)
	}

	if event.EventType != "new_market" {
		t.Errorf("Expected event_type 'new_market', got '%s'", event.EventType)
	}
	if event.Question != "Will NVIDIA close above $240?" {
		t.Errorf("Unexpected question: %s", event.Question)
	}
	if len(event.AssetsIDs) != 2 {
		t.Errorf("Expected 2 asset IDs, got %d", len(event.AssetsIDs))
	}
}

func TestParseMarketResolvedEvent(t *testing.T) {
	data := []byte(`{
		"id": "1031769",
		"question": "Will NVIDIA close above $240?",
		"market": "0xabc",
		"slug": "nvda-above-240",
		"description": "This market will resolve to Yes if...",
		"assets_ids": ["12345", "67890"],
		"outcomes": ["Yes", "No"],
		"winning_asset_id": "12345",
		"winning_outcome": "Yes",
		"timestamp": "1766790415550",
		"event_type": "market_resolved"
	}`)

	event, err := ParseMarketResolvedEvent(data)
	if err != nil {
		t.Fatalf("ParseMarketResolvedEvent failed: %v", err)
	}

	if event.EventType != "market_resolved" {
		t.Errorf("Expected event_type 'market_resolved', got '%s'", event.EventType)
	}
	if event.WinningOutcome != "Yes" {
		t.Errorf("Expected winning_outcome 'Yes', got '%s'", event.WinningOutcome)
	}
}
