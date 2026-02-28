package spot

import (
	"encoding/json"
	"fmt"
	"strconv"
)

func ParseDepthUpdate(raw json.RawMessage) (*DepthData, error) {
	var update DepthUpdate
	if err := json.Unmarshal(raw, &update); err != nil {
		return nil, fmt.Errorf("unmarshal depth update: %w", err)
	}

	data := &DepthData{
		Symbol:        update.Symbol,
		FirstUpdateID: update.FirstUpdateID,
		FinalUpdateID: update.FinalUpdateID,
		EventTime:     update.EventTime,
		Bids:          make([]PriceLevel, 0, len(update.Bids)),
		Asks:          make([]PriceLevel, 0, len(update.Asks)),
	}

	for _, bid := range update.Bids {
		if len(bid) >= 2 {
			price, err := strconv.ParseFloat(bid[0], 64)
			if err != nil {
				continue
			}
			size, err := strconv.ParseFloat(bid[1], 64)
			if err != nil {
				continue
			}
			data.Bids = append(data.Bids, PriceLevel{Price: price, Size: size})
		}
	}

	for _, ask := range update.Asks {
		if len(ask) >= 2 {
			price, err := strconv.ParseFloat(ask[0], 64)
			if err != nil {
				continue
			}
			size, err := strconv.ParseFloat(ask[1], 64)
			if err != nil {
				continue
			}
			data.Asks = append(data.Asks, PriceLevel{Price: price, Size: size})
		}
	}

	return data, nil
}

func ParseBookTicker(raw json.RawMessage) (*BookTickerData, error) {
	var ticker BookTicker
	if err := json.Unmarshal(raw, &ticker); err != nil {
		return nil, fmt.Errorf("unmarshal book ticker: %w", err)
	}

	data := &BookTickerData{
		Symbol:   ticker.Symbol,
		UpdateID: ticker.UpdateID,
	}

	var err error
	data.BidPrice, err = strconv.ParseFloat(ticker.BidPrice, 64)
	if err != nil {
		return nil, fmt.Errorf("parse bid price: %w", err)
	}

	data.BidQty, err = strconv.ParseFloat(ticker.BidQty, 64)
	if err != nil {
		return nil, fmt.Errorf("parse bid qty: %w", err)
	}

	data.AskPrice, err = strconv.ParseFloat(ticker.AskPrice, 64)
	if err != nil {
		return nil, fmt.Errorf("parse ask price: %w", err)
	}

	data.AskQty, err = strconv.ParseFloat(ticker.AskQty, 64)
	if err != nil {
		return nil, fmt.Errorf("parse ask qty: %w", err)
	}

	return data, nil
}

func ParseDepthSnapshot(raw []byte) (*DepthSnapshot, error) {
	var snapshot DepthSnapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return nil, fmt.Errorf("unmarshal depth snapshot: %w", err)
	}
	return &snapshot, nil
}

func ParsePriceLevelsFromSnapshot(snapshot *DepthSnapshot, maxLevels int) ([]PriceLevel, []PriceLevel) {
	bids := make([]PriceLevel, 0, maxLevels)
	asks := make([]PriceLevel, 0, maxLevels)

	count := maxLevels
	if count > len(snapshot.Bids) {
		count = len(snapshot.Bids)
	}

	for i := 0; i < count; i++ {
		level := snapshot.Bids[i]
		if len(level) >= 2 {
			price, err1 := strconv.ParseFloat(level[0], 64)
			size, err2 := strconv.ParseFloat(level[1], 64)
			if err1 == nil && err2 == nil {
				bids = append(bids, PriceLevel{Price: price, Size: size})
			}
		}
	}

	count = maxLevels
	if count > len(snapshot.Asks) {
		count = len(snapshot.Asks)
	}

	for i := 0; i < count; i++ {
		level := snapshot.Asks[i]
		if len(level) >= 2 {
			price, err1 := strconv.ParseFloat(level[0], 64)
			size, err2 := strconv.ParseFloat(level[1], 64)
			if err1 == nil && err2 == nil {
				asks = append(asks, PriceLevel{Price: price, Size: size})
			}
		}
	}

	return bids, asks
}

func ParseCombinedStreamMessage(raw []byte) (*CombinedStreamMessage, error) {
	var msg CombinedStreamMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		return nil, fmt.Errorf("unmarshal combined stream: %w", err)
	}
	return &msg, nil
}
