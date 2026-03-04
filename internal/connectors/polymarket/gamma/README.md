# Polymarket Gamma API Client

Simple client for fetching Polymarket market data from the Gamma API with dynamic window-based slug generation.

## Overview

This package provides:
- **Gamma API Client**: Fetch market data by slug
- **Window Calculator**: Compute nearest 5m or 15m window timestamps
- **Slug Builder**: Generate market slugs dynamically

## Usage

### Basic Usage

```go
import (
    "context"
    "time"
    
    "github.com/srkyaganti/pm-crypto-insurance-bot/internal/connectors/polymarket/gamma"
)

// Create client
client := gamma.NewClient(gamma.ClientConfig{
    BaseURL: "https://gamma-api.polymarket.com",
})

// Get current 15m market
slug := gamma.GetCurrentSlug("btc-updown-", 15)
market, err := client.GetMarketBySlug(ctx, slug)
if err != nil {
    // Handle error
}

// Access market data
fmt.Println("ConditionID:", market.ConditionID)
fmt.Println("Up Token:", market.Tokens.UpTokenID)
fmt.Println("Down Token:", market.Tokens.DownTokenID)
fmt.Println("Window Start:", market.WindowStart)
```

### Window Calculations

```go
// Calculate window timestamp
now := time.Now()
timestamp := gamma.CalculateWindowTimestamp(now, 15) // 15-minute window

// Build slug
slug := gamma.BuildSlug("btc-updown-", "15m", timestamp)
// Result: "btc-updown-15m-1772649000"

// Or use convenience function
slug := gamma.GetCurrentSlug("btc-updown-", 15)
```

## API Reference

### Client

#### `NewClient(config ClientConfig) *Client`

Creates a new Gamma API client.

**Config Options**:
- `BaseURL`: Gamma API base URL (e.g., "https://gamma-api.polymarket.com")
- `HTTPClient`: Optional custom HTTP client (defaults to 10s timeout)

#### `GetMarketBySlug(ctx context.Context, slug string) (*MarketData, error)`

Fetches market data for a given slug.

**Returns**:
- `MarketData` containing:
  - `ID`: Market ID
  - `Question`: Market question
  - `ConditionID`: Condition ID for the market
  - `Slug`: Market slug
  - `QuestionID`: Question ID
  - `Tokens`: Token IDs (Up/Down)
  - `WindowStart`: Window start time (extracted from slug)

### Window Functions

#### `CalculateWindowTimestamp(now time.Time, windowMinutes int) int64`

Calculates the nearest window boundary timestamp.

**Example**:
```go
timestamp := CalculateWindowTimestamp(time.Now(), 15) // 1772649000
```

#### `BuildSlug(prefix string, window string, timestamp int64) string`

Builds a market slug from components.

**Example**:
```go
slug := BuildSlug("btc-updown-", "15m", 1772649000)
// Result: "btc-updown-15m-1772649000"
```

#### `GetCurrentSlug(prefix string, windowMinutes int) string`

Convenience function that calculates current slug.

**Example**:
```go
slug := GetCurrentSlug("btc-updown-", 15)
// Result: "btc-updown-15m-{current_timestamp}"
```

## Error Handling

The client returns errors for:
- Market not found (404)
- Network errors
- Invalid response format
- Token ID parsing errors

```go
market, err := client.GetMarketBySlug(ctx, slug)
if err != nil {
    if strings.Contains(err.Error(), "market not found") {
        // Market doesn't exist yet
    }
    // Handle other errors
}
```

## Testing

Run tests:
```bash
go test ./internal/connectors/polymarket/gamma/...
```

## Examples

See `example_test.go` for complete usage examples.

## Notes

- The Gamma API is public and doesn't require authentication
- Market slugs follow the pattern: `{prefix}-{window}-{timestamp}`
- Token IDs are parsed from the `clobTokenIds` JSON array (first = Up, second = Down)
- Window timestamps are Unix seconds aligned to window boundaries
