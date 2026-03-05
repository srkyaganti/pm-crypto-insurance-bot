package main

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/rs/zerolog"
)

type MarketHistory struct {
	markets  []*CompletedMarket
	logger   *zerolog.Logger
	filePath string
	mu       sync.RWMutex
}

func NewMarketHistory(logger *zerolog.Logger, dataDir string) *MarketHistory {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		logger.Error().Err(err).Str("dir", dataDir).Msg("Failed to create data directory")
	}

	filePath := filepath.Join(dataDir, "market_history.ndjson")
	mh := &MarketHistory{
		markets:  make([]*CompletedMarket, 0),
		logger:   logger,
		filePath: filePath,
	}

	if err := mh.loadFromFile(); err != nil {
		logger.Error().Err(err).Msg("Failed to load market history from file")
	}

	return mh
}

func (h *MarketHistory) Add(market *CompletedMarket) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.markets = append(h.markets, market)

	if err := h.appendToFile(market); err != nil {
		h.logger.Error().Err(err).Str("slug", market.Slug).
			Msg("Failed to save market to file")
		return err
	}

	h.logger.Debug().
		Str("slug", market.Slug).
		Int("total_markets", len(h.markets)).
		Msg("Market added to history")

	return nil
}

func (h *MarketHistory) appendToFile(market *CompletedMarket) error {
	file, err := os.OpenFile(h.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "")
	return encoder.Encode(market)
}

func (h *MarketHistory) loadFromFile() error {
	file, err := os.Open(h.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			h.logger.Info().Str("file", h.filePath).Msg("Market history file does not exist, starting fresh")
			return nil
		}
		return err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	loaded := 0

	for {
		var market CompletedMarket
		if err := decoder.Decode(&market); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		h.markets = append(h.markets, &market)
		loaded++
	}

	h.logger.Info().
		Int("markets_loaded", loaded).
		Str("file", h.filePath).
		Msg("Loaded market history from file")

	return nil
}

func (h *MarketHistory) GetRecent(n int) []*CompletedMarket {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if n > len(h.markets) {
		n = len(h.markets)
	}
	if n <= 0 {
		return []*CompletedMarket{}
	}

	result := make([]*CompletedMarket, n)
	copy(result, h.markets[len(h.markets)-n:])
	return result
}

func (h *MarketHistory) Count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.markets)
}

func (h *MarketHistory) GetStats() map[string]interface{} {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.markets) == 0 {
		return map[string]interface{}{
			"total_markets": 0,
		}
	}

	totalPnL := 0.0
	wins := 0
	losses := 0
	trades := 0

	for _, m := range h.markets {
		totalPnL += m.PnL
		if m.PnL > 0 {
			wins++
		} else if m.PnL < 0 {
			losses++
		}
		if m.DidWeTrade {
			trades++
		}
	}

	winRate := 0.0
	if wins+losses > 0 {
		winRate = float64(wins) / float64(wins+losses) * 100
	}

	return map[string]interface{}{
		"total_markets": len(h.markets),
		"trades_made":   trades,
		"wins":          wins,
		"losses":        losses,
		"win_rate":      winRate,
		"total_pnl":     totalPnL,
		"avg_pnl":       totalPnL / float64(len(h.markets)),
	}
}
