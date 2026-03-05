package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/config"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/logging"
)

func main() {
	cfg := parseFlags()

	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Configuration error: %v\n", err)
		os.Exit(1)
	}

	logger, err := setupLogger(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to setup logger: %v\n", err)
		os.Exit(1)
	}

	logger.Info().
		Str("gamma_url", cfg.GammaURL).
		Str("market_ws_url", cfg.MarketWsURL).
		Strs("prefixes", cfg.MarketPrefixes).
		Ints("windows", cfg.Windows).
		Int("pre_schedule_windows", cfg.PreScheduleWindows).
		Str("data_dir", cfg.DataDir).
		Dur("log_interval", cfg.LogInterval).
		Msg("Starting Polymarket market tracker")

	tracker, err := NewMarketTracker(cfg, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to create market tracker")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := tracker.Start(ctx); err != nil {
		logger.Fatal().Err(err).Msg("Failed to start market tracker")
	}
	defer tracker.Stop()

	logger.Info().Msg("Market tracker is running. Press Ctrl+C to stop.")

	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info().Msg("Shutdown signal received")
			return
		case <-ticker.C:
			logger.Debug().
				Int("active_markets", len(tracker.GetCurrentActiveMarkets())).
				Msg("Tracker heartbeat")
		}
	}
}

func setupLogger(cfg *Config) (*zerolog.Logger, error) {
	logCfg := &config.LoggingConfig{
		Level:    cfg.LogLevel,
		FilePath: "",
	}

	logger, err := logging.New(logCfg)
	if err != nil {
		return nil, err
	}

	componentLogger := logging.Component(logger, "market_tracker")

	return componentLogger, nil
}
