#!/bin/bash

# Example script to run the Polymarket Market Tracker
# Set your Chainlink credentials before running

# Required environment variables
export CHAINLINK_REST_URL="${CHAINLINK_REST_URL:-https://your-chainlink-rest-url}"
export CHAINLINK_API_KEY="${CHAINLINK_API_KEY:-your-api-key}"
export CHAINLINK_API_SECRET="${CHAINLINK_API_SECRET:-your-api-secret}"
export BTC_USD_FEED_ID="${BTC_USD_FEED_ID:-0xyour-feed-id}"

# Optional: Additional Chainlink WebSocket URL
export CHAINLINK_WS_URL="${CHAINLINK_WS_URL:-}"

# Run the tracker
echo "Starting Polymarket Market Tracker..."
echo "Make sure you have set the required environment variables:"
echo "  - CHAINLINK_REST_URL"
echo "  - CHAINLINK_API_KEY"
echo "  - CHAINLINK_API_SECRET"
echo "  - BTC_USD_FEED_ID"
echo ""

# Basic usage
./bin/polymarket_tracker \
  -chainlink-rest-url "$CHAINLINK_REST_URL" \
  -chainlink-api-key "$CHAINLINK_API_KEY" \
  -chainlink-api-secret "$CHAINLINK_API_SECRET" \
  -btc-feed-id "$BTC_USD_FEED_ID" \
  -log-level info \
  -log-interval 10s

# Alternative: Debug mode with verbose logging
# ./bin/polymarket_tracker \
#   -chainlink-rest-url "$CHAINLINK_REST_URL" \
#   -chainlink-api-key "$CHAINLINK_API_KEY" \
#   -chainlink-api-secret "$CHAINLINK_API_SECRET" \
#   -btc-feed-id "$BTC_USD_FEED_ID" \
#   -log-level debug \
#   -log-interval 5s \
#   -pre-schedule 5

# Alternative: Track multiple prefixes
# ./bin/polymarket_tracker \
#   -chainlink-rest-url "$CHAINLINK_REST_URL" \
#   -chainlink-api-key "$CHAINLINK_API_KEY" \
#   -chainlink-api-secret "$CHAINLINK_API_SECRET" \
#   -btc-feed-id "$BTC_USD_FEED_ID" \
#   -prefix btc-updown- -prefix eth-updown- \
#   -window 5 -window 15
