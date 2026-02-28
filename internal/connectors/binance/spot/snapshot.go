package spot

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type SnapshotClient struct {
	config     *Config
	httpClient *http.Client
}

func NewSnapshotClient(cfg *Config) *SnapshotClient {
	return &SnapshotClient{
		config: cfg,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (s *SnapshotClient) FetchDepthSnapshot(ctx context.Context) (*DepthSnapshot, error) {
	restURL := s.config.RestURL
	if restURL == "" {
		restURL = "https://api.binance.com"
	}

	endpoint, err := url.Parse(restURL)
	if err != nil {
		return nil, fmt.Errorf("parse rest url: %w", err)
	}

	endpoint.Path = "/api/v3/depth"
	query := endpoint.Query()
	query.Set("symbol", strings.ToUpper(s.config.Symbol))
	query.Set("limit", "1000")
	endpoint.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	snapshot, err := ParseDepthSnapshot(body)
	if err != nil {
		return nil, fmt.Errorf("parse snapshot: %w", err)
	}

	return snapshot, nil
}

func ValidateUpdateSequence(firstUpdateID, finalUpdateID, lastUpdateID int64) bool {
	return firstUpdateID <= lastUpdateID+1 && finalUpdateID >= lastUpdateID
}

func IsUpdateInSequence(firstUpdateID, prevFinalUpdateID int64) bool {
	return firstUpdateID <= prevFinalUpdateID+1
}

type OrderBook struct {
	Bids map[float64]float64
	Asks map[float64]float64
}

func NewOrderBook() *OrderBook {
	return &OrderBook{
		Bids: make(map[float64]float64),
		Asks: make(map[float64]float64),
	}
}

func (ob *OrderBook) ApplySnapshot(bids, asks []PriceLevel, maxLevels int) {
	ob.Bids = make(map[float64]float64)
	ob.Asks = make(map[float64]float64)

	count := maxLevels
	if count > len(bids) {
		count = len(bids)
	}
	for i := 0; i < count; i++ {
		ob.Bids[bids[i].Price] = bids[i].Size
	}

	count = maxLevels
	if count > len(asks) {
		count = len(asks)
	}
	for i := 0; i < count; i++ {
		ob.Asks[asks[i].Price] = asks[i].Size
	}
}

func (ob *OrderBook) ApplyUpdate(bids, asks []PriceLevel) {
	for _, bid := range bids {
		if bid.Size == 0 {
			delete(ob.Bids, bid.Price)
		} else {
			ob.Bids[bid.Price] = bid.Size
		}
	}

	for _, ask := range asks {
		if ask.Size == 0 {
			delete(ob.Asks, ask.Price)
		} else {
			ob.Asks[ask.Price] = ask.Size
		}
	}
}

func (ob *OrderBook) GetTopLevels(maxLevels int) ([]PriceLevel, []PriceLevel) {
	bids := make([]PriceLevel, 0, len(ob.Bids))
	for price, size := range ob.Bids {
		bids = append(bids, PriceLevel{Price: price, Size: size})
	}

	for i := 0; i < len(bids); i++ {
		for j := i + 1; j < len(bids); j++ {
			if bids[j].Price > bids[i].Price {
				bids[i], bids[j] = bids[j], bids[i]
			}
		}
	}

	if len(bids) > maxLevels {
		bids = bids[:maxLevels]
	}

	asks := make([]PriceLevel, 0, len(ob.Asks))
	for price, size := range ob.Asks {
		asks = append(asks, PriceLevel{Price: price, Size: size})
	}

	for i := 0; i < len(asks); i++ {
		for j := i + 1; j < len(asks); j++ {
			if asks[j].Price < asks[i].Price {
				asks[i], asks[j] = asks[j], asks[i]
			}
		}
	}

	if len(asks) > maxLevels {
		asks = asks[:maxLevels]
	}

	return bids, asks
}

func (ob *OrderBook) GetBestBidAsk() (float64, float64, float64) {
	if len(ob.Bids) == 0 || len(ob.Asks) == 0 {
		return 0, 0, 0
	}

	bestBid := 0.0
	for price := range ob.Bids {
		if price > bestBid {
			bestBid = price
		}
	}

	bestAsk := 0.0
	for price := range ob.Asks {
		if bestAsk == 0 || price < bestAsk {
			bestAsk = price
		}
	}

	midPrice := (bestBid + bestAsk) / 2
	return bestBid, bestAsk, midPrice
}
