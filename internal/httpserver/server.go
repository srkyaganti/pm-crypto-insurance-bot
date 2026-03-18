package httpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/rs/zerolog"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/config"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/connectors/chainlink"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/connectors/polymarket/tracker"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/logging"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/state"
)

type Server struct {
	cfg        *config.HTTPServerConfig
	state      *state.StateStore
	chainlink  *chainlink.Connector
	tracker    *tracker.Connector
	logger     zerolog.Logger
	httpServer *http.Server
}

type chainlinkStateResp struct {
	Symbol      string    `json:"symbol"`
	LatestPrice float64   `json:"latest_price"`
	LastUpdate  time.Time `json:"last_update"`
	IsStale     bool      `json:"is_stale"`
}

type binanceSpotResp struct {
	Symbol    string                  `json:"symbol"`
	OrderBook state.OrderBookSnapshot `json:"order_book"`
	Flow10s   state.FlowWindow        `json:"flow_10s"`
	Flow30s   state.FlowWindow        `json:"flow_30s"`
	IsStale   bool                    `json:"is_stale"`
}

type chainlinkReportResp struct {
	FeedID                string    `json:"feed_id"`
	BenchmarkPrice        float64   `json:"benchmark_price"`
	ObservationsTimestamp uint32    `json:"observations_timestamp"`
	ValidFromTimestamp    uint32    `json:"valid_from_timestamp"`
	FetchedAt             time.Time `json:"fetched_at"`
}

func New(
	cfg *config.HTTPServerConfig,
	stateStore *state.StateStore,
	chainlinkConn *chainlink.Connector,
	trackerConn *tracker.Connector,
	baseLogger *zerolog.Logger,
) *Server {
	logger := logging.Component(baseLogger, "httpserver")
	return &Server{
		cfg:       cfg,
		state:     stateStore,
		chainlink: chainlinkConn,
		tracker:   trackerConn,
		logger:    *logger,
	}
}

func (s *Server) Start(ctx context.Context) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /api/state", s.handleStateSnapshot)
	mux.HandleFunc("GET /api/state/chainlink/{symbol}", s.handleChainlinkState)
	mux.HandleFunc("GET /api/state/binance/spot/{symbol}", s.handleBinanceSpotState)
	mux.HandleFunc("GET /api/state/binance/perps/{symbol}", s.handleBinancePerpsState)
	mux.HandleFunc("GET /api/state/polymarket", s.handlePolymarketState)
	mux.HandleFunc("GET /api/state/risk", s.handleRiskState)
	mux.HandleFunc("GET /api/chainlink/report", s.handleChainlinkReport)
	mux.HandleFunc("GET /api/tracker/windows", s.handleTrackerWindows)
	mux.HandleFunc("GET /api/tracker/stats", s.handleTrackerStats)
	mux.HandleFunc("GET /api/tracker/markets", s.handleTrackerMarkets)

	s.httpServer = &http.Server{
		Addr:         s.cfg.ListenAddr,
		Handler:      mux,
		ReadTimeout:  time.Duration(s.cfg.ReadTimeoutMs) * time.Millisecond,
		WriteTimeout: time.Duration(s.cfg.WriteTimeoutMs) * time.Millisecond,
	}

	go func() {
		s.logger.Info().Str("addr", s.cfg.ListenAddr).Msg("httpserver: starting")
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error().Err(err).Msg("httpserver: listen error")
		}
	}()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.httpServer.Shutdown(shutdownCtx); err != nil {
			s.logger.Error().Err(err).Msg("httpserver: shutdown error")
		}
		s.logger.Info().Msg("httpserver: stopped")
	}()
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
		"ts":     time.Now().UTC().Format(time.RFC3339),
	})
}

func (s *Server) handleStateSnapshot(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, s.state.Snapshot())
}

func (s *Server) handleChainlinkState(w http.ResponseWriter, r *http.Request) {
	symbol := r.PathValue("symbol")
	st := s.state.GetChainlink(symbol)
	s.writeJSON(w, http.StatusOK, chainlinkStateResp{
		Symbol:      symbol,
		LatestPrice: st.LatestPrice,
		LastUpdate:  st.LastUpdate,
		IsStale:     st.IsStale,
	})
}

func (s *Server) handleBinanceSpotState(w http.ResponseWriter, r *http.Request) {
	symbol := r.PathValue("symbol")
	st := s.state.GetBinanceSpot(symbol)
	s.writeJSON(w, http.StatusOK, binanceSpotResp{
		Symbol:    symbol,
		OrderBook: st.OrderBook,
		Flow10s:   st.TradeFlow.Window10s,
		Flow30s:   st.TradeFlow.Window30s,
		IsStale:   st.IsStale,
	})
}

func (s *Server) handleBinancePerpsState(w http.ResponseWriter, r *http.Request) {
	symbol := r.PathValue("symbol")
	s.writeJSON(w, http.StatusOK, s.state.GetBinancePerps(symbol))
}

func (s *Server) handlePolymarketState(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, s.state.GetPolymarket())
}

func (s *Server) handleRiskState(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, s.state.GetRisk())
}

func (s *Server) handleChainlinkReport(w http.ResponseWriter, r *http.Request) {
	feedID := r.URL.Query().Get("feed_id")
	if feedID == "" {
		s.writeError(w, http.StatusBadRequest, "feed_id is required")
		return
	}
	tsStr := r.URL.Query().Get("timestamp")
	if tsStr == "" {
		s.writeError(w, http.StatusBadRequest, "timestamp is required")
		return
	}
	ts, err := strconv.ParseUint(tsStr, 10, 64)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid timestamp: %v", err))
		return
	}
	if s.chainlink == nil {
		s.writeError(w, http.StatusServiceUnavailable, "chainlink connector not available")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()

	rep, err := s.chainlink.GetReportAtTimestampForFeed(ctx, feedID, ts)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			s.writeError(w, http.StatusGatewayTimeout, "chainlink request timed out")
			return
		}
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("chainlink error: %v", err))
		return
	}

	s.writeJSON(w, http.StatusOK, chainlinkReportResp{
		FeedID:                rep.FeedID,
		BenchmarkPrice:        rep.BenchmarkPrice,
		ObservationsTimestamp: rep.ObservationsTimestamp,
		ValidFromTimestamp:    rep.ValidFromTimestamp,
		FetchedAt:             time.Now().UTC(),
	})
}

func (s *Server) handleTrackerWindows(w http.ResponseWriter, r *http.Request) {
	if s.tracker == nil {
		s.writeError(w, http.StatusServiceUnavailable, "tracker not available")
		return
	}
	s.writeJSON(w, http.StatusOK, s.tracker.GetActiveWindowSummaries())
}

func (s *Server) handleTrackerStats(w http.ResponseWriter, r *http.Request) {
	if s.tracker == nil {
		s.writeError(w, http.StatusServiceUnavailable, "tracker not available")
		return
	}
	s.writeJSON(w, http.StatusOK, s.tracker.GetStats())
}

func (s *Server) handleTrackerMarkets(w http.ResponseWriter, r *http.Request) {
	if s.tracker == nil {
		s.writeError(w, http.StatusServiceUnavailable, "tracker not available")
		return
	}
	s.writeJSON(w, http.StatusOK, s.tracker.GetCurrentActiveMarkets())
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		s.logger.Error().Err(err).Msg("httpserver: failed to encode response")
	}
}

func (s *Server) writeError(w http.ResponseWriter, status int, msg string) {
	s.writeJSON(w, status, map[string]string{"error": msg})
}
