package market

import (
	"encoding/json"
	"testing"
)

func TestHandleMessage_SingleEvent(t *testing.T) {
	data := []byte(`{"market":"0x123","asset_id":"456","event_type":"book","bids":[],"asks":[]}`)

	var baseMsg struct {
		EventType string `json:"event_type"`
	}
	if err := json.Unmarshal(data, &baseMsg); err != nil {
		t.Errorf("Failed to parse single event: %v", err)
	}

	if baseMsg.EventType != "book" {
		t.Errorf("Expected event_type 'book', got '%s'", baseMsg.EventType)
	}
}

func TestHandleMessage_ArrayEvent(t *testing.T) {
	data := []byte(`[
		{"market":"0x123","asset_id":"456","event_type":"book","bids":[],"asks":[]},
		{"market":"0x789","asset_id":"012","event_type":"book","bids":[],"asks":[]}
	]`)

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

	for i, event := range events {
		var baseMsg struct {
			EventType string `json:"event_type"`
		}
		if err := json.Unmarshal(event, &baseMsg); err != nil {
			t.Errorf("Failed to parse event %d: %v", i, err)
		}

		if baseMsg.EventType != "book" {
			t.Errorf("Event %d: expected event_type 'book', got '%s'", i, baseMsg.EventType)
		}
	}
}

func TestHandleMessage_EmptyArray(t *testing.T) {
	data := []byte(`[]`)

	var events []json.RawMessage
	if err := json.Unmarshal(data, &events); err != nil {
		t.Errorf("Failed to parse empty array: %v", err)
	}

	if len(events) != 0 {
		t.Errorf("Expected 0 events, got %d", len(events))
	}
}

func TestHandleMessage_NonJSON(t *testing.T) {
	testCases := []struct {
		name    string
		data    string
		isValid bool
	}{
		{
			name:    "INVALID OPERATION",
			data:    "INVALID OPERATION",
			isValid: false,
		},
		{
			name:    "INVALID",
			data:    "INVALID",
			isValid: false,
		},
		{
			name:    "plain text",
			data:    "some plain text",
			isValid: false,
		},
		{
			name:    "valid json object",
			data:    `{"event_type":"book"}`,
			isValid: true,
		},
		{
			name:    "valid json array",
			data:    `[{"event_type":"book"}]`,
			isValid: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			isValid := json.Valid([]byte(tc.data))
			if isValid != tc.isValid {
				t.Errorf("Expected json.Valid() to return %v for %q, got %v", tc.isValid, tc.data, isValid)
			}
		})
	}
}
