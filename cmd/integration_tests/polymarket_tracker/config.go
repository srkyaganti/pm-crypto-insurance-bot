package main

import (
	"flag"
	"fmt"
	"time"
)

func parseFlags() *Config {
	cfg := &Config{}

	flag.StringVar(&cfg.GammaURL, "gamma-url", "https://gamma-api.polymarket.com", "Gamma API base URL")
	flag.StringVar(&cfg.MarketWsURL, "ws-url", "wss://ws-subscriptions-clob.polymarket.com/ws/market", "Market WebSocket URL")

	flag.StringVar(&cfg.ChainlinkRestURL, "chainlink-rest-url", "", "Chainlink REST URL")
	flag.StringVar(&cfg.ChainlinkWsURL, "chainlink-ws-url", "", "Chainlink WebSocket URL")
	flag.StringVar(&cfg.ChainlinkApiKey, "chainlink-api-key", "", "Chainlink API key")
	flag.StringVar(&cfg.ChainlinkApiSecret, "chainlink-api-secret", "", "Chainlink API secret")
	flag.StringVar(&cfg.BTCFeedID, "btc-feed-id", "", "Chainlink BTC/USD feed ID")

	var prefixes stringSlice
	flag.Var(&prefixes, "prefix", "Market prefixes to track (repeatable, default: btc-updown-)")
	var windows intSlice
	flag.Var(&windows, "window", "Window sizes in minutes (repeatable, default: 5, 15)")

	flag.IntVar(&cfg.PreScheduleWindows, "pre-schedule", 3, "Number of future windows to pre-schedule")
	flag.StringVar(&cfg.DataDir, "data-dir", "./data/market_history", "Data directory for market history")

	var logIntervalStr string
	flag.StringVar(&logIntervalStr, "log-interval", "10s", "State log interval")
	flag.StringVar(&cfg.LogLevel, "log-level", "info", "Log level (debug, info, warn, error)")
	flag.BoolVar(&cfg.CustomFeatures, "custom-features", true, "Enable custom features")

	flag.Parse()

	if len(prefixes) == 0 {
		cfg.MarketPrefixes = []string{"btc-updown-"}
	} else {
		cfg.MarketPrefixes = prefixes
	}

	if len(windows) == 0 {
		cfg.Windows = []int{5, 15}
	} else {
		cfg.Windows = windows
	}

	if logInterval, err := time.ParseDuration(logIntervalStr); err == nil {
		cfg.LogInterval = logInterval
	} else {
		cfg.LogInterval = 10 * time.Second
	}

	return cfg
}

func (c *Config) Validate() error {
	if c.ChainlinkRestURL == "" {
		return fmt.Errorf("chainlink-rest-url is required")
	}
	if c.ChainlinkApiKey == "" {
		return fmt.Errorf("chainlink-api-key is required")
	}
	if c.ChainlinkApiSecret == "" {
		return fmt.Errorf("chainlink-api-secret is required")
	}
	if c.BTCFeedID == "" {
		return fmt.Errorf("btc-feed-id is required")
	}
	if len(c.MarketPrefixes) == 0 {
		return fmt.Errorf("at least one market prefix is required")
	}
	if len(c.Windows) == 0 {
		return fmt.Errorf("at least one window size is required")
	}
	return nil
}

type stringSlice []string

func (s *stringSlice) String() string {
	return fmt.Sprintf("%v", []string(*s))
}

func (s *stringSlice) Set(value string) error {
	*s = append(*s, value)
	return nil
}

type intSlice []int

func (i *intSlice) String() string {
	return fmt.Sprintf("%v", []int(*i))
}

func (i *intSlice) Set(value string) error {
	var v int
	_, err := fmt.Sscanf(value, "%d", &v)
	if err != nil {
		return err
	}
	*i = append(*i, v)
	return nil
}
