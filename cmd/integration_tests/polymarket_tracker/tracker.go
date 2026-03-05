package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/connectors/chainlink"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/connectors/polymarket/gamma"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/connectors/polymarket/market"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/state"
)

type MarketTracker struct {
	gammaClient     *gamma.Client
	marketConnector *market.Connector
	chainlinkClient *chainlink.Client
	stateStore      *state.StateStore
	logger          *zerolog.Logger

	activeMarkets   map[string]*ActiveMarket
	scheduledTimers map[string]*time.Timer
	marketHistory   *MarketHistory

	config    *Config
	btcFeedID string

	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
}

func NewMarketTracker(cfg *Config, logger *zerolog.Logger) (*MarketTracker, error) {
	stateStore := state.NewStateStore()

	gammaClient := gamma.NewClient(gamma.ClientConfig{
		BaseURL: cfg.GammaURL,
	})

	chainlinkCfg := &chainlink.Config{
		RestURL:   cfg.ChainlinkRestURL,
		WsURL:     cfg.ChainlinkWsURL,
		ApiKey:    cfg.ChainlinkApiKey,
		ApiSecret: cfg.ChainlinkApiSecret,
		FeedID:    cfg.BTCFeedID,
	}

	chainlinkLogger := logger.With().Str("component", "chainlink").Logger()
	chainlinkClient, err := chainlink.NewClient(chainlinkCfg, &chainlinkLogger)
	if err != nil {
		return nil, fmt.Errorf("create chainlink client: %w", err)
	}

	if err := chainlinkClient.Initialize(); err != nil {
		return nil, fmt.Errorf("initialize chainlink client: %w", err)
	}

	marketCfg := &market.Config{
		WsURL:                cfg.MarketWsURL,
		CustomFeatureEnabled: cfg.CustomFeatures,
		InitialAssetIDs:      []string{},
	}

	marketLogger := logger.With().Str("component", "polymarket_market").Logger()
	marketConnector := market.NewConnector(marketCfg, stateStore, &marketLogger)

	marketHistory := NewMarketHistory(logger, cfg.DataDir)

	return &MarketTracker{
		gammaClient:     gammaClient,
		marketConnector: marketConnector.(*market.Connector),
		chainlinkClient: chainlinkClient,
		stateStore:      stateStore,
		logger:          logger,
		activeMarkets:   make(map[string]*ActiveMarket),
		scheduledTimers: make(map[string]*time.Timer),
		marketHistory:   marketHistory,
		config:          cfg,
		btcFeedID:       cfg.BTCFeedID,
	}, nil
}

func (mt *MarketTracker) Start(ctx context.Context) error {
	mt.ctx, mt.cancel = context.WithCancel(ctx)

	mt.logger.Info().Msg("Starting market connector")
	if err := mt.marketConnector.Start(ctx); err != nil {
		return fmt.Errorf("start market connector: %w", err)
	}

	mt.logger.Info().Msg("Waiting for connector to be healthy")
	for i := 0; i < 10; i++ {
		if err := mt.marketConnector.Health(); err == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}

	if err := mt.marketConnector.Health(); err != nil {
		return fmt.Errorf("connector not healthy: %w", err)
	}

	mt.logger.Info().Msg("Market connector is healthy")

	for _, prefix := range mt.config.MarketPrefixes {
		for _, window := range mt.config.Windows {
			currentSlug := gamma.GetCurrentSlug(prefix, window)

			mt.logger.Info().
				Str("prefix", prefix).
				Int("window", window).
				Str("slug", currentSlug).
				Msg("Subscribing to current market window")

			if err := mt.fetchAndSubscribe(currentSlug, prefix, window); err != nil {
				mt.logger.Error().
					Err(err).
					Str("slug", currentSlug).
					Msg("Failed to subscribe to current market")
			}
		}
	}

	go mt.monitoringLoop()
	go mt.strategyAnalysisLoop()

	mt.logger.Info().
		Int("active_markets", len(mt.activeMarkets)).
		Msg("Market tracker started successfully")

	return nil
}

func (mt *MarketTracker) Stop() error {
	mt.logger.Info().Msg("Stopping market tracker")

	if mt.cancel != nil {
		mt.cancel()
	}

	mt.mu.Lock()
	for slug, timer := range mt.scheduledTimers {
		timer.Stop()
		delete(mt.scheduledTimers, slug)
	}
	mt.mu.Unlock()

	mt.mu.RLock()
	for slug, market := range mt.activeMarkets {
		if market.EndTimer != nil {
			market.EndTimer.Stop()
		}
		mt.marketConnector.UnsubscribeMany([]string{
			market.YesAssetID,
			market.NoAssetID,
		})
		mt.logger.Debug().Str("slug", slug).Msg("Unsubscribed from market")
	}
	mt.mu.RUnlock()

	if err := mt.marketConnector.Stop(); err != nil {
		mt.logger.Error().Err(err).Msg("Failed to stop market connector")
	}

	stats := mt.marketHistory.GetStats()
	mt.logger.Info().
		Interface("history_stats", stats).
		Msg("Market tracker stopped")

	return nil
}

func (mt *MarketTracker) getChainlinkPriceAtTimestamp(ctx context.Context, timestamp uint64) (float64, error) {
	report, err := mt.chainlinkClient.GetReportAtTimestampForFeed(ctx, mt.btcFeedID, timestamp)
	if err != nil {
		return 0, fmt.Errorf("get chainlink report: %w", err)
	}

	if report == nil {
		return 0, fmt.Errorf("nil report returned")
	}

	price := extractPriceFromReport(report)

	mt.logger.Debug().
		Uint64("timestamp", timestamp).
		Float64("price", price).
		Str("feed_id", mt.btcFeedID).
		Msg("Fetched Chainlink price")

	return price, nil
}

func extractPriceFromReport(report interface{}) float64 {
	return 0.0
}
