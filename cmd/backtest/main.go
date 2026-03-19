package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/backtest"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/config"
)

func main() {
	configPath := flag.String("config", "./configs/config.yaml", "path to config file")
	addr := flag.String("addr", ":8081", "listen address")
	dataDir := flag.String("data-dir", "", "override default data directory")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	defaultDataDir := cfg.Recorder.OutputDir
	if *dataDir != "" {
		defaultDataDir = *dataDir
	}

	srv := &server{
		defaultStrategy: cfg.Strategy,
		defaultDataDir:  defaultDataDir,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /data", srv.handleData)
	mux.HandleFunc("POST /run", srv.handleRun)

	httpServer := &http.Server{
		Addr:         *addr,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 120 * time.Second,
	}

	fmt.Printf("backtest server listening on %s (data: %s)\n", *addr, defaultDataDir)
	if err := httpServer.ListenAndServe(); err != nil {
		log.Fatalf("listen: %v", err)
	}
}

type server struct {
	defaultStrategy config.StrategyConfig
	defaultDataDir  string
}

type runRequest struct {
	From              string             `json:"from"`
	To                string             `json:"to"`
	DataDir           string             `json:"data_dir"`
	StrategyOverrides *strategyOverrides `json:"strategy_overrides"`
}

type strategyOverrides struct {
	EntryWindowMinSecs        *int               `json:"entry_window_min_secs"`
	EntryWindowMaxSecs        *int               `json:"entry_window_max_secs"`
	MinPMConfidence           map[string]float64 `json:"min_pm_confidence"`
	MaxCLBinanceDivergencePct map[string]float64 `json:"max_cl_binance_divergence_pct"`
	DepthSafetyMultiplier     map[string]float64 `json:"depth_safety_multiplier"`
	MinDepth                  map[string]float64 `json:"min_depth"`
	MaxSharesPerTrade         *int               `json:"max_shares_per_trade"`
}

type dataFileInfo struct {
	Name   string  `json:"name"`
	SizeMB float64 `json:"size_mb"`
	Hour   string  `json:"hour"`
}

type dataResponse struct {
	Files    []dataFileInfo `json:"files"`
	Earliest string         `json:"earliest"`
	Latest   string         `json:"latest"`
}

func (s *server) handleData(w http.ResponseWriter, r *http.Request) {
	dir := s.defaultDataDir
	if q := r.URL.Query().Get("data_dir"); q != "" {
		dir = q
	}

	pattern := filepath.Join(dir, "data-*.ndjson")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	sort.Strings(matches)

	var files []dataFileInfo
	var earliest, latest string

	for _, path := range matches {
		info, err := os.Stat(path)
		if err != nil {
			continue
		}
		name := filepath.Base(path)
		hour := strings.TrimPrefix(name, "data-")
		hour = strings.TrimSuffix(hour, ".ndjson")

		files = append(files, dataFileInfo{
			Name:   name,
			SizeMB: float64(info.Size()) / (1024 * 1024),
			Hour:   hour,
		})

		if earliest == "" || hour < earliest {
			earliest = hour
		}
		if latest == "" || hour > latest {
			latest = hour
		}
	}

	writeJSON(w, http.StatusOK, dataResponse{
		Files:    files,
		Earliest: earliest,
		Latest:   latest,
	})
}

func (s *server) handleRun(w http.ResponseWriter, r *http.Request) {
	var req runRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON: %v", err))
		return
	}

	from, err := time.Parse(time.RFC3339, req.From)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid 'from': %v", err))
		return
	}
	to, err := time.Parse(time.RFC3339, req.To)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid 'to': %v", err))
		return
	}

	dataDir := s.defaultDataDir
	if req.DataDir != "" {
		dataDir = req.DataDir
	}

	strategy := s.mergeOverrides(req.StrategyOverrides)

	result, err := backtest.Run(backtest.Request{
		DataDir:  dataDir,
		From:     from,
		To:       to,
		Strategy: strategy,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func (s *server) mergeOverrides(overrides *strategyOverrides) config.StrategyConfig {
	cfg := s.defaultStrategy

	// Deep copy maps to avoid mutating defaults.
	cfg.MinPMConfidence = cloneMap(s.defaultStrategy.MinPMConfidence)
	cfg.MaxCLBinanceDivergencePct = cloneMap(s.defaultStrategy.MaxCLBinanceDivergencePct)
	cfg.DepthSafetyMultiplier = cloneMap(s.defaultStrategy.DepthSafetyMultiplier)
	cfg.MinDepth = cloneMap(s.defaultStrategy.MinDepth)

	if overrides == nil {
		return cfg
	}

	if overrides.EntryWindowMinSecs != nil {
		cfg.EntryWindowMinSecs = *overrides.EntryWindowMinSecs
	}
	if overrides.EntryWindowMaxSecs != nil {
		cfg.EntryWindowMaxSecs = *overrides.EntryWindowMaxSecs
	}
	if overrides.MaxSharesPerTrade != nil {
		cfg.MaxSharesPerTrade = *overrides.MaxSharesPerTrade
	}

	for k, v := range overrides.MinPMConfidence {
		cfg.MinPMConfidence[k] = v
	}
	for k, v := range overrides.MaxCLBinanceDivergencePct {
		cfg.MaxCLBinanceDivergencePct[k] = v
	}
	for k, v := range overrides.DepthSafetyMultiplier {
		cfg.DepthSafetyMultiplier[k] = v
	}
	for k, v := range overrides.MinDepth {
		cfg.MinDepth[k] = v
	}

	return cfg
}

func cloneMap(m map[string]float64) map[string]float64 {
	if m == nil {
		return make(map[string]float64)
	}
	out := make(map[string]float64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
