package market

import (
	"encoding/json"
	"fmt"
)

func ParseBookEvent(data []byte) (*BookEvent, error) {
	var event BookEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return nil, fmt.Errorf("unmarshal book event: %w", err)
	}
	return &event, nil
}

func ParsePriceChangeEvent(data []byte) (*PriceChangeEvent, error) {
	var event PriceChangeEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return nil, fmt.Errorf("unmarshal price_change event: %w", err)
	}
	return &event, nil
}

func ParseTickSizeChangeEvent(data []byte) (*TickSizeChangeEvent, error) {
	var event TickSizeChangeEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return nil, fmt.Errorf("unmarshal tick_size_change event: %w", err)
	}
	return &event, nil
}

func ParseLastTradePriceEvent(data []byte) (*LastTradePriceEvent, error) {
	var event LastTradePriceEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return nil, fmt.Errorf("unmarshal last_trade_price event: %w", err)
	}
	return &event, nil
}

func ParseBestBidAskEvent(data []byte) (*BestBidAskEvent, error) {
	var event BestBidAskEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return nil, fmt.Errorf("unmarshal best_bid_ask event: %w", err)
	}
	return &event, nil
}

func ParseNewMarketEvent(data []byte) (*NewMarketEvent, error) {
	var event NewMarketEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return nil, fmt.Errorf("unmarshal new_market event: %w", err)
	}
	return &event, nil
}

func ParseMarketResolvedEvent(data []byte) (*MarketResolvedEvent, error) {
	var event MarketResolvedEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return nil, fmt.Errorf("unmarshal market_resolved event: %w", err)
	}
	return &event, nil
}
