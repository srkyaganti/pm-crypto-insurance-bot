package gamma

import (
	"testing"
	"time"
)

func TestCalculateWindowTimestamp(t *testing.T) {
	tests := []struct {
		name          string
		now           time.Time
		windowMinutes int
		expected      int64
	}{
		{
			name:          "15m window",
			now:           time.Unix(1772649123, 0),
			windowMinutes: 15,
			expected:      1772649000,
		},
		{
			name:          "5m window",
			now:           time.Unix(1772649123, 0),
			windowMinutes: 5,
			expected:      1772649000,
		},
		{
			name:          "exact boundary",
			now:           time.Unix(1772649000, 0),
			windowMinutes: 15,
			expected:      1772649000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CalculateWindowTimestamp(tt.now, tt.windowMinutes)
			if result != tt.expected {
				t.Errorf("CalculateWindowTimestamp() = %d, want %d", result, tt.expected)
			}
		})
	}
}

func TestBuildSlug(t *testing.T) {
	tests := []struct {
		name      string
		prefix    string
		window    string
		timestamp int64
		expected  string
	}{
		{
			name:      "15m slug",
			prefix:    "btc-updown-",
			window:    "15m",
			timestamp: 1772649000,
			expected:  "btc-updown-15m-1772649000",
		},
		{
			name:      "5m slug",
			prefix:    "btc-updown-",
			window:    "5m",
			timestamp: 1772649000,
			expected:  "btc-updown-5m-1772649000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BuildSlug(tt.prefix, tt.window, tt.timestamp)
			if result != tt.expected {
				t.Errorf("BuildSlug() = %s, want %s", result, tt.expected)
			}
		})
	}
}

func TestExtractTimestampFromSlug(t *testing.T) {
	tests := []struct {
		name     string
		slug     string
		expected int64
		hasError bool
	}{
		{
			name:     "valid slug",
			slug:     "btc-updown-15m-1772649000",
			expected: 1772649000,
			hasError: false,
		},
		{
			name:     "valid 5m slug",
			slug:     "btc-updown-5m-1772639700",
			expected: 1772639700,
			hasError: false,
		},
		{
			name:     "invalid slug - too few parts",
			slug:     "btc-updown",
			expected: 0,
			hasError: true,
		},
		{
			name:     "invalid slug - non-numeric timestamp",
			slug:     "btc-updown-15m-abc",
			expected: 0,
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := extractTimestampFromSlug(tt.slug)
			if tt.hasError {
				if err == nil {
					t.Errorf("extractTimestampFromSlug() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("extractTimestampFromSlug() unexpected error: %v", err)
				}
				if result != tt.expected {
					t.Errorf("extractTimestampFromSlug() = %d, want %d", result, tt.expected)
				}
			}
		})
	}
}

func TestParseTokenIDs(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		expectedUp   string
		expectedDown string
		hasError     bool
	}{
		{
			name:         "valid token IDs",
			input:        `["83867715080096202857531905834925637220198817705362280958076594615286515335609", "65282932454695228337287642625173832783331415457485241021430215648153870843323"]`,
			expectedUp:   "83867715080096202857531905834925637220198817705362280958076594615286515335609",
			expectedDown: "65282932454695228337287642625173832783331415457485241021430215648153870843323",
			hasError:     false,
		},
		{
			name:     "only one token",
			input:    `["token1"]`,
			hasError: true,
		},
		{
			name:     "empty array",
			input:    `[]`,
			hasError: true,
		},
		{
			name:     "invalid JSON",
			input:    `not json`,
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseTokenIDs(tt.input)
			if tt.hasError {
				if err == nil {
					t.Errorf("parseTokenIDs() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("parseTokenIDs() unexpected error: %v", err)
				}
				if result.UpTokenID != tt.expectedUp {
					t.Errorf("parseTokenIDs() UpTokenID = %s, want %s", result.UpTokenID, tt.expectedUp)
				}
				if result.DownTokenID != tt.expectedDown {
					t.Errorf("parseTokenIDs() DownTokenID = %s, want %s", result.DownTokenID, tt.expectedDown)
				}
			}
		})
	}
}
