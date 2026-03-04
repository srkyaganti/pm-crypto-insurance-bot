package spot

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

func ParsePartialDepth(raw json.RawMessage) (*DepthData, error) {
	var event PartialDepthEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		return nil, fmt.Errorf("unmarshal partial depth: %w", err)
	}

	data := &DepthData{
		Symbol:    event.Symbol,
		EventTime: event.EventTime,
		Bids:      make([]PriceLevel, 0, len(event.Bids)),
		Asks:      make([]PriceLevel, 0, len(event.Asks)),
	}

	for _, bid := range event.Bids {
		if len(bid) >= 2 {
			price, _ := strconv.ParseFloat(bid[0], 64)
			size, _ := strconv.ParseFloat(bid[1], 64)
			data.Bids = append(data.Bids, PriceLevel{Price: price, Size: size})
		}
	}

	for _, ask := range event.Asks {
		if len(ask) >= 2 {
			price, _ := strconv.ParseFloat(ask[0], 64)
			size, _ := strconv.ParseFloat(ask[1], 64)
			data.Asks = append(data.Asks, PriceLevel{Price: price, Size: size})
		}
	}

	return data, nil
}

func ParseAggTrade(raw json.RawMessage) (*AggTradeData, error) {
	var event AggTradeEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		return nil, fmt.Errorf("unmarshal aggTrade: %w", err)
	}

	price, _ := strconv.ParseFloat(event.Price, 64)
	qty, _ := strconv.ParseFloat(event.Quantity, 64)

	isBuy := !event.IsBuyerMaker

	return &AggTradeData{
		Symbol:    event.Symbol,
		TradeID:   event.TradeID,
		Price:     price,
		Quantity:  qty,
		Notional:  price * qty,
		IsBuy:     isBuy,
		Timestamp: time.Unix(0, event.TradeTime*1e6),
	}, nil
}

func ParseCombinedStreamMessage(raw []byte) (*CombinedStreamMessage, error) {
	var msg CombinedStreamMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		return nil, fmt.Errorf("unmarshal combined stream: %w", err)
	}
	return &msg, nil
}
