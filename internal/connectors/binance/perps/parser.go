package perps

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

func ParseMarkPrice(raw json.RawMessage) (*MarkPriceData, error) {
	var update MarkPriceUpdate
	if err := json.Unmarshal(raw, &update); err != nil {
		return nil, fmt.Errorf("unmarshal mark price: %w", err)
	}

	data := &MarkPriceData{
		Symbol:    update.Symbol,
		EventTime: update.EventTime,
	}

	var err error
	data.MarkPrice, err = strconv.ParseFloat(update.MarkPrice, 64)
	if err != nil {
		return nil, fmt.Errorf("parse mark price: %w", err)
	}

	if update.IndexPrice != "" {
		data.IndexPrice, _ = strconv.ParseFloat(update.IndexPrice, 64)
	}

	return data, nil
}

func ParseForceOrder(raw json.RawMessage) (*LiquidationData, error) {
	var event ForceOrderEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		return nil, fmt.Errorf("unmarshal force order: %w", err)
	}

	order := event.Order

	data := &LiquidationData{
		Symbol:    order.Symbol,
		Side:      order.Side,
		EventTime: order.TradeTime,
	}

	var err error
	data.Price, err = strconv.ParseFloat(order.AveragePrice, 64)
	if err != nil || data.Price == 0 {
		data.Price, _ = strconv.ParseFloat(order.Price, 64)
	}

	data.Quantity, err = strconv.ParseFloat(order.OriginalQuantity, 64)
	if err != nil || data.Quantity == 0 {
		data.Quantity, _ = strconv.ParseFloat(order.LastFilledQuantity, 64)
	}

	return data, nil
}

func ParseCombinedStreamMessage(raw []byte) (*CombinedStreamMessage, error) {
	var msg CombinedStreamMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		return nil, fmt.Errorf("unmarshal combined stream: %w", err)
	}
	return &msg, nil
}

func IsMarkPriceStream(stream string) bool {
	return strings.Contains(stream, "@markPrice")
}

func IsForceOrderStream(stream string) bool {
	return strings.Contains(stream, "@forceOrder")
}
