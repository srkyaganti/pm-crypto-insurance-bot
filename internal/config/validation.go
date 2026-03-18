package config

import (
	"fmt"
	"strings"
)

func Validate(cfg *Config) error {
	if err := validateApp(cfg); err != nil {
		return err
	}
	if err := validateMarkets(cfg); err != nil {
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

	if cfg.App.TickIntervalMs <= 0 {
		cfg.App.TickIntervalMs = 100
	}

	return nil
}

func validateMarkets(cfg *Config) error {
	if len(cfg.Markets) == 0 {
		return fmt.Errorf("at least one market must be defined in markets[]")
	}

	seenSymbols := make(map[string]bool)
	seenFeedIDs := make(map[string]bool)
	seenBinance := make(map[string]bool)
	seenPrefixes := make(map[string]bool)

	for i, m := range cfg.Markets {
		if m.Symbol == "" {
			return fmt.Errorf("markets[%d].symbol is required", i)
		}
		if seenSymbols[m.Symbol] {
			return fmt.Errorf("duplicate symbol in markets: %s", m.Symbol)
		}
		seenSymbols[m.Symbol] = true

		if m.ChainlinkFeedID == "" {
			return fmt.Errorf("markets[%d].chainlink_feed_id is required (symbol: %s)", i, m.Symbol)
		}
		if !strings.HasPrefix(m.ChainlinkFeedID, "0x") {
			return fmt.Errorf("markets[%d].chainlink_feed_id must start with 0x (symbol: %s)", i, m.Symbol)
		}
		if seenFeedIDs[m.ChainlinkFeedID] {
			return fmt.Errorf("duplicate chainlink_feed_id in markets: %s", m.ChainlinkFeedID)
		}
		seenFeedIDs[m.ChainlinkFeedID] = true

		if m.BinanceSymbol == "" {
			return fmt.Errorf("markets[%d].binance_symbol is required (symbol: %s)", i, m.Symbol)
		}
		if seenBinance[m.BinanceSymbol] {
			return fmt.Errorf("duplicate binance_symbol in markets: %s", m.BinanceSymbol)
		}
		seenBinance[m.BinanceSymbol] = true

		if m.Polymarket.Prefix == "" {
			return fmt.Errorf("markets[%d].polymarket.prefix is required (symbol: %s)", i, m.Symbol)
		}
		if seenPrefixes[m.Polymarket.Prefix] {
			return fmt.Errorf("duplicate polymarket prefix in markets: %s", m.Polymarket.Prefix)
		}
		seenPrefixes[m.Polymarket.Prefix] = true

		if len(m.Polymarket.Windows) == 0 {
			return fmt.Errorf("markets[%d].polymarket.windows must have at least one entry (symbol: %s)", i, m.Symbol)
		}
		for j, w := range m.Polymarket.Windows {
			if w <= 0 {
				return fmt.Errorf("markets[%d].polymarket.windows[%d] must be positive (symbol: %s)", i, j, m.Symbol)
			}
		}
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
