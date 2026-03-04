package config

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal([]byte(os.ExpandEnv(string(data))), &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	//applyEnvOverrides(&cfg)

	if err := Validate(&cfg); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return &cfg, nil
}

func applyEnvOverrides(cfg *Config) {
	if v := os.Getenv("APP_MODE"); v != "" {
		cfg.App.Mode = v
	}
	if v := os.Getenv("APP_SYMBOL"); v != "" {
		cfg.App.Symbol = v
	}
	if v := os.Getenv("CHAINLINK_API_KEY"); v != "" {
		cfg.Chainlink.ApiKey = v
	}
	if v := os.Getenv("CHAINLINK_API_SECRET"); v != "" {
		cfg.Chainlink.ApiSecret = v
	}
	if v := os.Getenv("POLYMARKET_API_KEY"); v != "" {
		cfg.Polymarket.ApiKey = v
	}
	if v := os.Getenv("POLYMARKET_API_SECRET"); v != "" {
		cfg.Polymarket.ApiSecret = v
	}
	if v := os.Getenv("POLYMARKET_PASSPHRASE"); v != "" {
		cfg.Polymarket.Passphrase = v
	}
	if v := os.Getenv("POLYMARKET_PRIVATE_KEY"); v != "" {
		cfg.Polymarket.PrivateKey = v
	}
	if v := os.Getenv("RISK_KILL_SWITCH"); v != "" {
		cfg.Risk.KillSwitch = strings.ToLower(v) == "true"
	}
}

func DefaultConfig() *Config {
	return &Config{
		App: AppConfig{
			Mode:           "paper",
			TickIntervalMs: 100,
		},
		Logging: LoggingConfig{
			Level:      "info",
			FilePath:   "./logs/engine.log",
			MaxSizeMB:  100,
			MaxBackups: 10,
			MaxAgeDays: 30,
			Compress:   true,
		},
		BinanceSpot: BinanceSpotConfig{
			WsURL:      "wss://stream.binance.com:9443",
			RestURL:    "https://api.binance.com",
			DepthSpeed: "100ms",
			DepthLimit: 20,
		},
		BinancePerps: BinancePerpsConfig{
			WsURL: "wss://fstream.binance.com",
		},
		Chainlink: ChainlinkConfig{
			RestURL:        "https://api.dataengine.chain.link",
			WsURL:          "wss://ws.dataengine.chain.link",
			WsHA:           true,
			WsMaxReconnect: 10,
		},
		Polymarket: PolymarketConfig{
			ClobURL:  "https://clob.polymarket.com",
			WsURL:    "wss://ws-subscriptions-clob.polymarket.com/ws/",
			GammaURL: "https://gamma-api.polymarket.com",
		},
		Risk: RiskConfig{
			KillSwitch:          false,
			MaxDailyLosses:      2,
			MaxOrderSize:        100.0,
			ChainlinkStaleMs:    300,
			BinanceStaleMs:      2000,
			PolymarketStaleMs:   2000,
			MaxMove30sPct:       0.5,
			MaxMove60sPct:       1.0,
			RecoveryCooldownSec: 5,
		},
		Persistence: PersistenceConfig{
			Enabled:          true,
			NdjsonPath:       "./logs/events",
			RotationInterval: 3600,
			FlushIntervalMs:  1000,
		},
	}
}
