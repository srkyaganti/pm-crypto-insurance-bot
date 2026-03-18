package sampler

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/logging"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/state"
)

type SymbolSnapshot struct {
	CLPrice    float64 `json:"cl_price"`
	CLStale    bool    `json:"cl_stale"`
	SpotMid    float64 `json:"spot_mid"`
	SpotBid    float64 `json:"spot_bid"`
	SpotAsk    float64 `json:"spot_ask"`
	SpotImb10s float64 `json:"spot_imb_10s"`
	SpotImb30s float64 `json:"spot_imb_30s"`
}

type WindowSummary struct {
	Slug           string    `json:"slug"`
	Symbol         string    `json:"symbol"`
	WindowMin      int       `json:"window_min"`
	StartPrice     float64   `json:"start_price"`
	EndTime        time.Time `json:"end_ts"`
	TimeRemainingS int64     `json:"time_remaining_s"`
	YesMid         float64   `json:"yes_mid"`
	YesBid         float64   `json:"yes_bid"`
	YesAsk         float64   `json:"yes_ask"`
	NoMid          float64   `json:"no_mid"`
	NoBid          float64   `json:"no_bid"`
	NoAsk          float64   `json:"no_ask"`
}

type Snapshot struct {
	TS      time.Time                 `json:"ts"`
	Symbols map[string]SymbolSnapshot `json:"symbols"`
	Windows []WindowSummary           `json:"windows"`
}

type Sampler struct {
	stateStore *state.StateStore
	getWindows func() []WindowSummary
	symbols    []string
	interval   time.Duration
	file       *os.File
	enc        *json.Encoder
	logger     *zerolog.Logger
	wg         sync.WaitGroup
}

func New(
	store *state.StateStore,
	getWindows func() []WindowSummary,
	symbols []string,
	interval time.Duration,
	outputPath string,
	logger *zerolog.Logger,
) (*Sampler, error) {
	f, err := os.OpenFile(outputPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("open snapshot file: %w", err)
	}

	samplerLogger := logging.Component(logger, "sampler")

	return &Sampler{
		stateStore: store,
		getWindows: getWindows,
		symbols:    symbols,
		interval:   interval,
		file:       f,
		enc:        json.NewEncoder(f),
		logger:     samplerLogger,
	}, nil
}

func (s *Sampler) Start(ctx context.Context) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(s.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.capture()
			}
		}
	}()
}

func (s *Sampler) Stop() {
	s.wg.Wait()
	if err := s.file.Close(); err != nil {
		s.logger.Error().Err(err).Msg("sampler: error closing snapshot file")
	}
}

func (s *Sampler) capture() {
	snap := Snapshot{
		TS:      time.Now().UTC(),
		Symbols: make(map[string]SymbolSnapshot, len(s.symbols)),
	}

	for _, sym := range s.symbols {
		cl := s.stateStore.GetChainlink(sym)
		spot := s.stateStore.GetBinanceSpot(sym)

		snap.Symbols[sym] = SymbolSnapshot{
			CLPrice:    cl.LatestPrice,
			CLStale:    cl.IsStale,
			SpotMid:    spot.OrderBook.MidPrice,
			SpotBid:    spot.OrderBook.BestBid,
			SpotAsk:    spot.OrderBook.BestAsk,
			SpotImb10s: spot.TradeFlow.Window10s.Imbalance,
			SpotImb30s: spot.TradeFlow.Window30s.Imbalance,
		}
	}

	if windows := s.getWindows(); windows != nil {
		snap.Windows = windows
	} else {
		snap.Windows = []WindowSummary{}
	}

	if err := s.enc.Encode(snap); err != nil {
		s.logger.Error().Err(err).Msg("sampler: failed to encode snapshot")
	}
}
