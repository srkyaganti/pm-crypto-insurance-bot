package config

import (
	"fmt"
	"strings"
)

func Validate(cfg *Config) error {
	if err := validateApp(cfg); err != nil {
		return err
	}
	if err := validateLogging(cfg); err != nil {
		return err
	}
	if err := validateRisk(cfg); err != nil {
		return err
	}
	if err := validatePersistence(cfg); err != nil {
		return err
	}
	return nil
}

func validateApp(cfg *Config) error {
	if cfg.App.Mode == "" {
		cfg.App.Mode = "paper"
	}
	validModes := map[string]bool{"paper": true, "live": true}
	if !validModes[cfg.App.Mode] {
		return fmt.Errorf("invalid app.mode: %s (must be 'paper' or 'live')", cfg.App.Mode)
	}

	if cfg.App.Symbol == "" {
		return fmt.Errorf("app.symbol is required")
	}

	if cfg.App.TickIntervalMs <= 0 {
		cfg.App.TickIntervalMs = 100
	}

	return nil
}

func validateLogging(cfg *Config) error {
	if cfg.Logging.Level == "" {
		cfg.Logging.Level = "info"
	}
	validLevels := map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
	if !validLevels[strings.ToLower(cfg.Logging.Level)] {
		return fmt.Errorf("invalid logging.level: %s", cfg.Logging.Level)
	}

	if cfg.Logging.FilePath == "" {
		cfg.Logging.FilePath = "./logs/engine.log"
	}

	return nil
}

func validateRisk(cfg *Config) error {
	if cfg.Risk.MaxDailyLosses < 0 {
		return fmt.Errorf("risk.max_daily_losses cannot be negative")
	}
	if cfg.Risk.MaxOrderSize <= 0 {
		return fmt.Errorf("risk.max_order_size must be positive")
	}
	if cfg.Risk.ChainlinkStaleMs <= 0 {
		cfg.Risk.ChainlinkStaleMs = 300
	}
	if cfg.Risk.BinanceStaleMs <= 0 {
		cfg.Risk.BinanceStaleMs = 2000
	}
	if cfg.Risk.PolymarketStaleMs <= 0 {
		cfg.Risk.PolymarketStaleMs = 2000
	}
	return nil
}

func validatePersistence(cfg *Config) error {
	if cfg.Persistence.Enabled && cfg.Persistence.NdjsonPath == "" {
		cfg.Persistence.NdjsonPath = "./logs/events"
	}
	if cfg.Persistence.RotationInterval <= 0 {
		cfg.Persistence.RotationInterval = 3600
	}
	if cfg.Persistence.FlushIntervalMs <= 0 {
		cfg.Persistence.FlushIntervalMs = 1000
	}
	return nil
}

func ValidateConnectorCredentials(cfg *Config) error {
	if cfg.Chainlink.ApiKey == "" {
		return fmt.Errorf("chainlink.api_key is required")
	}
	if cfg.Chainlink.ApiSecret == "" {
		return fmt.Errorf("chainlink.api_secret is required")
	}
	if cfg.Chainlink.FeedID == "" {
		return fmt.Errorf("chainlink.feed_id is required")
	}

	if cfg.Polymarket.ApiKey == "" {
		return fmt.Errorf("polymarket.api_key is required")
	}
	if cfg.Polymarket.ApiSecret == "" {
		return fmt.Errorf("polymarket.api_secret is required")
	}
	if cfg.Polymarket.Passphrase == "" {
		return fmt.Errorf("polymarket.passphrase is required")
	}

	return nil
}
