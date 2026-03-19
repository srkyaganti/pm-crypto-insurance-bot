package recorder

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/rs/zerolog"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/connectors/polymarket/tracker"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/state"
)

type RecorderSnapshot struct {
	TS      int64                    `json:"ts"`
	TsISO   string                   `json:"ts_iso"`
	Symbols map[string]SymbolData    `json:"symbols"`
	Markets []MarketData             `json:"markets"`
}

type SymbolData struct {
	Chainlink    CLData    `json:"chainlink"`
	BinanceSpot  SpotData  `json:"binance_spot"`
	BinancePerps PerpsData `json:"binance_perps"`
}

type CLData struct {
	Price float64    `json:"price"`
	Stale bool       `json:"stale"`
	Ticks []TickData `json:"ticks"`
}

type TickData struct {
	Price float64 `json:"price"`
	TS    int64   `json:"ts"`
}

type SpotData struct {
	Mid     float64     `json:"mid"`
	BestBid float64     `json:"best_bid"`
	BestAsk float64     `json:"best_ask"`
	Bids    []LevelData `json:"bids"`
	Asks    []LevelData `json:"asks"`
	Stale   bool        `json:"stale"`
}

type LevelData struct {
	Price float64 `json:"price"`
	Size  float64 `json:"size"`
}

type PerpsData struct {
	MarkPrice       float64 `json:"mark_price"`
	LiquidationFlag bool    `json:"liquidation_flag"`
	Stale           bool    `json:"stale"`
}

type MarketData struct {
	Slug           string  `json:"slug"`
	Symbol         string  `json:"symbol"`
	StartPrice     float64 `json:"start_price"`
	TimeRemainingS int64   `json:"time_remaining_s"`
	YesBid         float64 `json:"yes_bid"`
	YesAsk         float64 `json:"yes_ask"`
	NoBid          float64 `json:"no_bid"`
	NoAsk          float64 `json:"no_ask"`
}

type Recorder struct {
	state      *state.StateStore
	getWindows func() []tracker.ActiveWindowSummary
	symbols    []string
	interval   time.Duration
	outputDir  string
	logger     zerolog.Logger

	currentFile *os.File
	currentHour int
	enc         *json.Encoder
	buf         *bufio.Writer
	tickCount   int
}

func New(
	store *state.StateStore,
	getWindows func() []tracker.ActiveWindowSummary,
	symbols []string,
	interval time.Duration,
	outputDir string,
	logger *zerolog.Logger,
) (*Recorder, error) {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("create recorder output dir: %w", err)
	}

	return &Recorder{
		state:       store,
		getWindows:  getWindows,
		symbols:     symbols,
		interval:    interval,
		outputDir:   outputDir,
		logger:      logger.With().Str("component", "recorder").Logger(),
		currentHour: -1,
	}, nil
}

func (r *Recorder) Run(ctx context.Context) error {
	r.logger.Info().
		Str("output_dir", r.outputDir).
		Dur("interval", r.interval).
		Strs("symbols", r.symbols).
		Msg("recorder starting")

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			r.logger.Info().Msg("recorder stopping")
			r.flush()
			r.closeFile()
			return nil
		case <-ticker.C:
			r.capture()
		}
	}
}

func (r *Recorder) Stop() {
	r.flush()
	r.closeFile()
}

func (r *Recorder) capture() {
	now := time.Now().UTC()

	if err := r.rotateIfNeeded(now); err != nil {
		r.logger.Error().Err(err).Msg("failed to rotate file")
		return
	}

	if r.enc == nil {
		return
	}

	snap := RecorderSnapshot{
		TS:      now.UnixMilli(),
		TsISO:   now.Format(time.RFC3339Nano),
		Symbols: make(map[string]SymbolData, len(r.symbols)),
	}

	for _, sym := range r.symbols {
		cl := r.state.GetChainlink(sym)
		sp := r.state.GetBinanceSpot(sym)
		pp := r.state.GetBinancePerps(sym)

		// Convert CL ring buffer ticks
		allTicks := cl.Ticks.All()
		ticks := make([]TickData, len(allTicks))
		for i, t := range allTicks {
			ticks[i] = TickData{
				Price: t.Price,
				TS:    t.Timestamp.UnixMilli(),
			}
		}

		// Convert Binance spot order book levels
		bids := make([]LevelData, len(sp.OrderBook.Bids))
		for i, l := range sp.OrderBook.Bids {
			bids[i] = LevelData{Price: l.Price, Size: l.Size}
		}
		asks := make([]LevelData, len(sp.OrderBook.Asks))
		for i, l := range sp.OrderBook.Asks {
			asks[i] = LevelData{Price: l.Price, Size: l.Size}
		}

		snap.Symbols[sym] = SymbolData{
			Chainlink: CLData{
				Price: cl.LatestPrice,
				Stale: cl.IsStale,
				Ticks: ticks,
			},
			BinanceSpot: SpotData{
				Mid:     sp.OrderBook.MidPrice,
				BestBid: sp.OrderBook.BestBid,
				BestAsk: sp.OrderBook.BestAsk,
				Bids:    bids,
				Asks:    asks,
				Stale:   sp.IsStale,
			},
			BinancePerps: PerpsData{
				MarkPrice:       pp.LastMarkPrice,
				LiquidationFlag: pp.LiquidationFlag,
				Stale:           pp.IsStale,
			},
		}
	}

	// PM markets
	windows := r.getWindows()
	markets := make([]MarketData, len(windows))
	for i, w := range windows {
		markets[i] = MarketData{
			Slug:           w.Slug,
			Symbol:         w.Symbol,
			StartPrice:     w.StartPrice,
			TimeRemainingS: w.TimeRemainingS,
			YesBid:         w.YesBid,
			YesAsk:         w.YesAsk,
			NoBid:          w.NoBid,
			NoAsk:          w.NoAsk,
		}
	}
	snap.Markets = markets

	if err := r.enc.Encode(snap); err != nil {
		r.logger.Error().Err(err).Msg("failed to encode snapshot")
		return
	}

	r.tickCount++
	if r.tickCount%5 == 0 {
		r.flush()
	}
}

func (r *Recorder) rotateIfNeeded(now time.Time) error {
	hour := now.Hour() + now.YearDay()*24 + now.Year()*366*24
	if r.currentFile != nil && r.currentHour == hour {
		return nil
	}

	r.flush()
	r.closeFile()

	filename := fmt.Sprintf("data-%s.ndjson", now.Format("2006-01-02-15"))
	path := filepath.Join(r.outputDir, filename)

	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open recorder file %s: %w", path, err)
	}

	r.currentFile = f
	r.currentHour = hour
	r.buf = bufio.NewWriter(f)
	r.enc = json.NewEncoder(r.buf)

	r.logger.Info().Str("file", filename).Msg("opened new recorder file")
	return nil
}

func (r *Recorder) flush() {
	if r.buf != nil {
		if err := r.buf.Flush(); err != nil {
			r.logger.Error().Err(err).Msg("failed to flush recorder buffer")
		}
	}
}

func (r *Recorder) closeFile() {
	if r.currentFile != nil {
		if err := r.currentFile.Close(); err != nil {
			r.logger.Error().Err(err).Msg("failed to close recorder file")
		}
		r.currentFile = nil
		r.enc = nil
		r.buf = nil
	}
}
