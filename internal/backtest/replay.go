package backtest

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
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

// --- Request types ---

type SweepRequest struct {
	DataDir string
	From    time.Time
	To      time.Time
	Configs []ConfigEntry
}

type ConfigEntry struct {
	ID       string
	Strategy config.StrategyConfig
}

// --- Response types ---

type SweepResult struct {
	Metadata Metadata           `json:"metadata"`
	Results  map[string]*Result `json:"results"`
	Ranking  []RankEntry        `json:"ranking"`
}

type Metadata struct {
	From             time.Time `json:"from"`
	To               time.Time `json:"to"`
	FilesProcessed   int       `json:"files_processed"`
	FileNames        []string  `json:"file_names"`
	TotalSnapshots   int       `json:"total_snapshots"`
	ProcessingTimeMs int64     `json:"processing_time_ms"`
}

type RankEntry struct {
	ConfigID string  `json:"config_id"`
	PnL      float64 `json:"pnl"`
	WinRate  float64 `json:"win_rate"`
	Trades   int     `json:"trades"`
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

	// Verbose fields for AI analysis
	WinRate        float64                `json:"win_rate"`
	AvgEntryPrice  float64                `json:"avg_entry_price"`
	AvgPnLPerTrade float64                `json:"avg_pnl_per_trade"`
	BestTradePnL   float64                `json:"best_trade_pnl"`
	WorstTradePnL  float64                `json:"worst_trade_pnl"`
	BestTradeSlug  string                 `json:"best_trade_slug"`
	WorstTradeSlug string                 `json:"worst_trade_slug"`
	TradesByWindow map[string]int         `json:"trades_by_window"`
	GatePassRates  map[string]float64     `json:"gate_pass_rates"`
	ConfigUsed     config.StrategyConfig  `json:"config_used"`
}

type Result struct {
	Trades  []TradeEntry `json:"trades"`
	Summary Summary      `json:"summary"`
}

// --- Internal types ---

type openPosition struct {
	tradeIdx    int
	startPrice  float64
	side        string
	lastCLPrice float64
	lastSeen    time.Time
}

type evaluator struct {
	cfg *config.StrategyConfig
}

type configState struct {
	ev        *evaluator
	result    *Result
	positions map[string]*openPosition
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

// Sweep replays recorded data through the engine's signal + gate logic for multiple configs in a single pass.
func Sweep(req SweepRequest) (*SweepResult, error) {
	startTime := time.Now()

	files, err := selectFiles(req.DataDir, req.From, req.To)
	if err != nil {
		return nil, fmt.Errorf("select files: %w", err)
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no data files found in %s for range %s to %s", req.DataDir, req.From, req.To)
	}

	// Initialize per-config state.
	states := make([]configState, len(req.Configs))
	for i, ce := range req.Configs {
		states[i] = configState{
			ev: &evaluator{cfg: &req.Configs[i].Strategy},
			result: &Result{
				Summary: Summary{
					TradesBySymbol:  make(map[string]int),
					RejectionCounts: make(map[string]int),
					TradesByWindow:  make(map[string]int),
					ConfigUsed:      ce.Strategy,
				},
			},
			positions: make(map[string]*openPosition),
		}
	}

	globalSnapCount := 0

	fileNames := make([]string, len(files))
	for i, f := range files {
		fileNames[i] = filepath.Base(f)
	}

	for _, f := range files {
		if err := sweepFile(f, req.From, req.To, states, &globalSnapCount); err != nil {
			return nil, fmt.Errorf("process %s: %w", filepath.Base(f), err)
		}
	}

	// Post-processing per config.
	results := make(map[string]*Result, len(req.Configs))
	for i, ce := range req.Configs {
		cs := &states[i]

		// Resolve remaining open positions.
		for slug, pos := range cs.positions {
			resolvePosition(cs.result, pos, pos.lastCLPrice, pos.lastSeen)
			delete(cs.positions, slug)
		}

		cs.result.Summary.TotalSnapshots = globalSnapCount
		computeVerboseSummary(cs.result)
		results[ce.ID] = cs.result
	}

	// Build ranking sorted by PnL descending.
	ranking := buildRanking(req.Configs, results)

	return &SweepResult{
		Metadata: Metadata{
			From:             req.From,
			To:               req.To,
			FilesProcessed:   len(files),
			FileNames:        fileNames,
			TotalSnapshots:   globalSnapCount,
			ProcessingTimeMs: time.Since(startTime).Milliseconds(),
		},
		Results: results,
		Ranking: ranking,
	}, nil
}

func sweepFile(path string, from, to time.Time, states []configState, globalSnapCount *int) error {
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

		*globalSnapCount++

		activeSlugs := make(map[string]bool, len(snap.Markets))

		for _, mkt := range snap.Markets {
			activeSlugs[mkt.Slug] = true

			symData, ok := snap.Symbols[mkt.Symbol]
			if !ok {
				continue
			}

			// Shared work: convert ONCE per market.
			w := convertMarket(mkt)
			clState := convertChainlink(symData.Chainlink)
			spotState := convertSpot(symData.BinanceSpot)
			perpsState := convertPerps(symData.BinancePerps)
			signals := engine.ComputeSignals(w, clState, spotState, perpsState)

			// Per-config evaluation.
			for ci := range states {
				cs := &states[ci]

				// Update open position tracking.
				if pos, exists := cs.positions[mkt.Slug]; exists {
					pos.lastCLPrice = symData.Chainlink.Price
					pos.lastSeen = snapTime

					if mkt.TimeRemainingS <= 0 {
						resolvePosition(cs.result, pos, symData.Chainlink.Price, snapTime)
						delete(cs.positions, mkt.Slug)
						continue
					}
				}

				// Set HasPosition per config.
				wCopy := w
				if _, exists := cs.positions[mkt.Slug]; exists {
					wCopy.HasPosition = true
				}

				decision := cs.ev.runGates(wCopy, signals)
				cs.result.Summary.TotalEvaluations++

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
						Shares:     cs.ev.cfg.MaxSharesPerTrade,
						Signals:    signals,
					}
					cs.result.Trades = append(cs.result.Trades, trade)
					cs.result.Summary.TotalTrades++
					cs.result.Summary.TradesBySymbol[mkt.Symbol]++
					cs.result.Summary.TradesByWindow[classifyWindow(mkt.Slug)]++

					cs.positions[mkt.Slug] = &openPosition{
						tradeIdx:    len(cs.result.Trades) - 1,
						startPrice:  mkt.StartPrice,
						side:        decision.Side,
						lastCLPrice: symData.Chainlink.Price,
						lastSeen:    snapTime,
					}
				} else {
					cs.result.Summary.RejectionCounts[decision.RejectReason]++
				}
			}
		}

		// Resolve positions for markets that disappeared from this snapshot.
		for ci := range states {
			cs := &states[ci]
			for slug, pos := range cs.positions {
				if !activeSlugs[slug] {
					resolvePosition(cs.result, pos, pos.lastCLPrice, pos.lastSeen)
					delete(cs.positions, slug)
				}
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

func computeVerboseSummary(result *Result) {
	s := &result.Summary

	// Unresolved count.
	for _, t := range result.Trades {
		if !t.Resolved {
			s.Unresolved++
		}
	}

	// Win rate.
	resolved := s.Wins + s.Losses
	if resolved > 0 {
		s.WinRate = float64(s.Wins) / float64(resolved)
	}

	// Entry price and PnL stats.
	if len(result.Trades) > 0 {
		var totalEntry float64
		bestPnL := -math.MaxFloat64
		worstPnL := math.MaxFloat64

		for _, t := range result.Trades {
			totalEntry += t.EntryPrice

			if t.Resolved {
				if t.PnL > bestPnL {
					bestPnL = t.PnL
					s.BestTradeSlug = t.Slug
				}
				if t.PnL < worstPnL {
					worstPnL = t.PnL
					s.WorstTradeSlug = t.Slug
				}
			}
		}

		s.AvgEntryPrice = totalEntry / float64(len(result.Trades))

		if resolved > 0 {
			s.AvgPnLPerTrade = s.SimulatedPnL / float64(resolved)
		}

		if bestPnL != -math.MaxFloat64 {
			s.BestTradePnL = bestPnL
		}
		if worstPnL != math.MaxFloat64 {
			s.WorstTradePnL = worstPnL
		}
	}

	// Gate pass rates.
	s.GatePassRates = computeGatePassRates(s)
}

// computeGatePassRates calculates what percentage of evaluations passed each gate.
// Gate N's denominator = evaluations that reached gate N (total - rejections at prior gates).
func computeGatePassRates(s *Summary) map[string]float64 {
	gates := []string{
		"gate1_time_window",
		"gate2_existing_position",
		"gate3_pm_confidence",
		"gate4_cl_binance_agreement",
		"gate5_expected_move_unavailable",
		"gate5_gap_too_small",
		"gate5_depth_too_thin",
		"gate6_liquidation_cascade",
	}

	rates := make(map[string]float64, len(gates))
	remaining := s.TotalEvaluations

	for _, gate := range gates {
		if remaining <= 0 {
			break
		}
		rejected := s.RejectionCounts[gate]
		passed := remaining - rejected
		rates[gate] = float64(passed) / float64(remaining)
		remaining = passed
	}

	return rates
}

// classifyWindow returns "5m", "15m", etc. based on the slug format (e.g. "btc-updown-5m-1773945600").
func classifyWindow(slug string) string {
	parts := strings.Split(slug, "-")
	for _, p := range parts {
		if strings.HasSuffix(p, "m") && len(p) >= 2 && p[0] >= '0' && p[0] <= '9' {
			return p
		}
	}
	return "unknown"
}

func buildRanking(configs []ConfigEntry, results map[string]*Result) []RankEntry {
	ranking := make([]RankEntry, 0, len(configs))
	for _, ce := range configs {
		r := results[ce.ID]
		resolved := r.Summary.Wins + r.Summary.Losses
		var winRate float64
		if resolved > 0 {
			winRate = float64(r.Summary.Wins) / float64(resolved)
		}
		ranking = append(ranking, RankEntry{
			ConfigID: ce.ID,
			PnL:      r.Summary.SimulatedPnL,
			WinRate:  winRate,
			Trades:   r.Summary.TotalTrades,
		})
	}
	sort.Slice(ranking, func(i, j int) bool {
		return ranking[i].PnL > ranking[j].PnL
	})
	return ranking
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

		// Support both hourly (2006-01-02-15) and 15-minute (2006-01-02-15-04) filenames.
		fileStart, err := time.Parse("2006-01-02-15-04", name)
		if err != nil {
			fileStart, err = time.Parse("2006-01-02-15", name)
			if err != nil {
				continue
			}
		}

		// Determine file duration from name format: 15 min if minute component present, else 1 hour.
		fileDuration := time.Hour
		if len(name) > len("2006-01-02-15") {
			fileDuration = 15 * time.Minute
		}

		fileEnd := fileStart.Add(fileDuration)
		if fileEnd.After(from) && fileStart.Before(to) {
			selected = append(selected, path)
		}
	}

	return selected, nil
}
