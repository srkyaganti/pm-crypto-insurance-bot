package backtest

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/config"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/connectors/polymarket/tracker"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/engine"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/recorder"
)

type Request struct {
	DataDir  string
	From     time.Time
	To       time.Time
	Strategy config.StrategyConfig
}

type TradeEntry struct {
	Timestamp      time.Time            `json:"timestamp"`
	Slug           string               `json:"slug"`
	Symbol         string               `json:"symbol"`
	Side           string               `json:"side"`
	EntryPrice     float64              `json:"entry_price"`
	StartPrice     float64              `json:"start_price"`
	Shares         int                  `json:"shares"`
	Signals        engine.MarketSignals `json:"signals"`
	Resolved       bool                 `json:"resolved"`
	Won            bool                 `json:"won"`
	PnL            float64              `json:"pnl"`
	ResolutionTime *time.Time           `json:"resolution_time,omitempty"`
}

type Summary struct {
	TotalSnapshots   int            `json:"total_snapshots"`
	TotalEvaluations int            `json:"total_evaluations"`
	TotalTrades      int            `json:"total_trades"`
	TradesBySymbol   map[string]int `json:"trades_by_symbol"`
	RejectionCounts  map[string]int `json:"rejection_counts"`
	SimulatedPnL     float64        `json:"simulated_pnl"`
	Wins             int            `json:"wins"`
	Losses           int            `json:"losses"`
	Unresolved       int            `json:"unresolved"`
}

type Result struct {
	Trades  []TradeEntry `json:"trades"`
	Summary Summary      `json:"summary"`
}

// openPosition tracks an unresolved trade during replay.
type openPosition struct {
	tradeIdx    int
	startPrice  float64
	side        string
	lastCLPrice float64
	lastSeen    time.Time
}

// evaluator wraps StrategyConfig for gate evaluation (same logic as engine.runGates).
type evaluator struct {
	cfg *config.StrategyConfig
}

func (ev *evaluator) symbolFloat(m map[string]float64, symbol string) float64 {
	if v, ok := m[symbol]; ok {
		return v
	}
	return 0
}

func (ev *evaluator) runGates(w tracker.ActiveWindowSummary, sig engine.MarketSignals) engine.TradeDecision {
	if sig.TimeRemainingS < int64(ev.cfg.EntryWindowMinSecs) || sig.TimeRemainingS > int64(ev.cfg.EntryWindowMaxSecs) {
		return engine.TradeDecision{RejectReason: "gate1_time_window", Signals: sig}
	}
	if w.HasPosition {
		return engine.TradeDecision{RejectReason: "gate2_existing_position", Signals: sig}
	}
	if sig.PMConfidence < ev.symbolFloat(ev.cfg.MinPMConfidence, sig.Symbol) {
		return engine.TradeDecision{RejectReason: "gate3_pm_confidence", Signals: sig}
	}
	if !sig.CLBinanceAgree || sig.DivergencePct > ev.symbolFloat(ev.cfg.MaxCLBinanceDivergencePct, sig.Symbol) {
		return engine.TradeDecision{RejectReason: "gate4_cl_binance_agreement", Signals: sig}
	}
	if sig.ExpectedMove == 0 {
		return engine.TradeDecision{RejectReason: "gate5_expected_move_unavailable", Signals: sig}
	}
	gapAbs := sig.Gap
	if gapAbs < 0 {
		gapAbs = -gapAbs
	}
	if gapAbs < ev.symbolFloat(ev.cfg.DepthSafetyMultiplier, sig.Symbol)*sig.ExpectedMove {
		return engine.TradeDecision{RejectReason: "gate5_gap_too_small", Signals: sig}
	}
	if sig.LiquidityToFlip < ev.symbolFloat(ev.cfg.MinDepth, sig.Symbol) {
		return engine.TradeDecision{RejectReason: "gate5_depth_too_thin", Signals: sig}
	}
	if sig.LiquidationActive {
		return engine.TradeDecision{RejectReason: "gate6_liquidation_cascade", Signals: sig}
	}
	return engine.TradeDecision{
		ShouldTrade: true,
		Side:        sig.WinningSide,
		Signals:     sig,
	}
}

// Run replays recorded data through the engine's signal + gate logic.
func Run(req Request) (*Result, error) {
	files, err := selectFiles(req.DataDir, req.From, req.To)
	if err != nil {
		return nil, fmt.Errorf("select files: %w", err)
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no data files found in %s for range %s to %s", req.DataDir, req.From, req.To)
	}

	ev := &evaluator{cfg: &req.Strategy}
	result := &Result{
		Summary: Summary{
			TradesBySymbol:  make(map[string]int),
			RejectionCounts: make(map[string]int),
		},
	}
	positions := make(map[string]*openPosition)

	for _, f := range files {
		if err := processFile(f, req.From, req.To, ev, result, positions); err != nil {
			return nil, fmt.Errorf("process %s: %w", filepath.Base(f), err)
		}
	}

	// Resolve remaining open positions with last known CL price.
	for slug, pos := range positions {
		resolvePosition(result, pos, pos.lastCLPrice, pos.lastSeen)
		delete(positions, slug)
	}

	for _, t := range result.Trades {
		if !t.Resolved {
			result.Summary.Unresolved++
		}
	}

	return result, nil
}

func processFile(path string, from, to time.Time, ev *evaluator, result *Result, positions map[string]*openPosition) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	for scanner.Scan() {
		var snap recorder.RecorderSnapshot
		if err := json.Unmarshal(scanner.Bytes(), &snap); err != nil {
			continue
		}

		snapTime := time.UnixMilli(snap.TS).UTC()
		if snapTime.Before(from) || snapTime.After(to) {
			continue
		}

		result.Summary.TotalSnapshots++

		activeSlugs := make(map[string]bool, len(snap.Markets))

		for _, mkt := range snap.Markets {
			activeSlugs[mkt.Slug] = true

			symData, ok := snap.Symbols[mkt.Symbol]
			if !ok {
				continue
			}

			// Update open position tracking.
			if pos, exists := positions[mkt.Slug]; exists {
				pos.lastCLPrice = symData.Chainlink.Price
				pos.lastSeen = snapTime

				if mkt.TimeRemainingS <= 0 {
					resolvePosition(result, pos, symData.Chainlink.Price, snapTime)
					delete(positions, mkt.Slug)
					continue
				}
			}

			// Convert snapshot data to state types.
			w := convertMarket(mkt)
			if _, exists := positions[mkt.Slug]; exists {
				w.HasPosition = true
			}

			clState := convertChainlink(symData.Chainlink)
			spotState := convertSpot(symData.BinanceSpot)
			perpsState := convertPerps(symData.BinancePerps)

			signals := engine.ComputeSignals(w, clState, spotState, perpsState)
			decision := ev.runGates(w, signals)

			result.Summary.TotalEvaluations++

			if decision.ShouldTrade {
				var entryPrice float64
				if decision.Side == "YES" {
					entryPrice = mkt.YesAsk
				} else {
					entryPrice = mkt.NoAsk
				}

				trade := TradeEntry{
					Timestamp:  snapTime,
					Slug:       mkt.Slug,
					Symbol:     mkt.Symbol,
					Side:       decision.Side,
					EntryPrice: entryPrice,
					StartPrice: mkt.StartPrice,
					Shares:     ev.cfg.MaxSharesPerTrade,
					Signals:    signals,
				}
				result.Trades = append(result.Trades, trade)
				result.Summary.TotalTrades++
				result.Summary.TradesBySymbol[mkt.Symbol]++

				positions[mkt.Slug] = &openPosition{
					tradeIdx:    len(result.Trades) - 1,
					startPrice:  mkt.StartPrice,
					side:        decision.Side,
					lastCLPrice: symData.Chainlink.Price,
					lastSeen:    snapTime,
				}
			} else {
				result.Summary.RejectionCounts[decision.RejectReason]++
			}
		}

		// Resolve positions for markets that disappeared from this snapshot.
		for slug, pos := range positions {
			if !activeSlugs[slug] {
				resolvePosition(result, pos, pos.lastCLPrice, pos.lastSeen)
				delete(positions, slug)
			}
		}
	}

	return scanner.Err()
}

func resolvePosition(result *Result, pos *openPosition, finalCLPrice float64, resolvedAt time.Time) {
	trade := &result.Trades[pos.tradeIdx]

	yesWins := finalCLPrice >= pos.startPrice
	won := (pos.side == "YES" && yesWins) || (pos.side == "NO" && !yesWins)

	trade.Resolved = true
	trade.Won = won
	trade.ResolutionTime = &resolvedAt

	shares := float64(trade.Shares)
	if won {
		trade.PnL = (1 - trade.EntryPrice) * shares
		result.Summary.Wins++
	} else {
		trade.PnL = -trade.EntryPrice * shares
		result.Summary.Losses++
	}
	result.Summary.SimulatedPnL += trade.PnL
}

func selectFiles(dataDir string, from, to time.Time) ([]string, error) {
	pattern := filepath.Join(dataDir, "data-*.ndjson")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}

	sort.Strings(matches)

	var selected []string
	for _, path := range matches {
		name := filepath.Base(path)
		name = strings.TrimPrefix(name, "data-")
		name = strings.TrimSuffix(name, ".ndjson")

		fileHour, err := time.Parse("2006-01-02-15", name)
		if err != nil {
			continue
		}

		fileEnd := fileHour.Add(time.Hour)
		if fileEnd.After(from) && fileHour.Before(to) {
			selected = append(selected, path)
		}
	}

	return selected, nil
}
