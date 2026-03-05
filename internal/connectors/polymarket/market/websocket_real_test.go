package market

import (
	"encoding/json"
	"testing"
)

func TestHandleMessage_RealWorldArray(t *testing.T) {
	realData := `[{"market":"0x0c4d78496e991d444e22c4a79d0234b28398c34103de65ae57e6310270377215","asset_id":"10126328586671790193483374604767135838125518024885038458997426658932977408014","timestamp":"1772707556842","hash":"fd5226bf9576d1c88e875ef9629db5a1a8e76370","bids":[{"price":"0.01","size":"14567.88"}],"asks":[{"price":"0.99","size":"16058.58"}],"tick_size":"0.01","event_type":"book","last_trade_price":"0.540"},{"market":"0x0c4d78496e991d444e22c4a79d0234b28398c34103de65ae57e6310270377215","asset_id":"9689931980792922237160555583548379955935290237025456484184178648324448749627","timestamp":"1772707556842","hash":"b69178eecdb8bbcd4cb0d053582e4d3e355d32a5","bids":[{"price":"0.01","size":"16058.58"}],"asks":[{"price":"0.99","size":"14567.88"}],"tick_size":"0.01","event_type":"book","last_trade_price":"0.540"}]`

	data := []byte(realData)

	if len(data) == 0 || data[0] != '[' {
		t.Error("Expected array indicator")
	}

	var events []json.RawMessage
	if err := json.Unmarshal(data, &events); err != nil {
		t.Errorf("Failed to parse event array: %v", err)
	}

	if len(events) != 2 {
		t.Errorf("Expected 2 events, got %d", len(events))
	}

	bookCount := 0
	for i, event := range events {
		var baseMsg struct {
			EventType string `json:"event_type"`
		}
		if err := json.Unmarshal(event, &baseMsg); err != nil {
			t.Errorf("Failed to parse event %d: %v", i, err)
		}

		if baseMsg.EventType == "book" {
			bookCount++
			bookEvent, err := ParseBookEvent(event)
			if err != nil {
				t.Errorf("Failed to parse book event %d: %v", i, err)
			}
			if bookEvent.Market == "" {
				t.Errorf("Book event %d has empty market", i)
			}
			if bookEvent.AssetID == "" {
				t.Errorf("Book event %d has empty asset_id", i)
			}
		}
	}

	if bookCount != 2 {
		t.Errorf("Expected 2 book events, got %d", bookCount)
	}
}
