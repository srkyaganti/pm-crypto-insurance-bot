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

	// Market feed configs are built from prefix + feed-id + window flags
	var prefixes stringSlice
	flag.Var(&prefixes, "prefix", "Market prefixes to track (repeatable, default: btc-updown-)")
	var feedIDs stringSlice
	flag.Var(&feedIDs, "feed-id", "Chainlink feed IDs matching prefixes (repeatable)")
	var windows intSlice
	flag.Var(&windows, "window", "Window sizes in minutes (repeatable, default: 5, 15)")

	flag.IntVar(&cfg.PreScheduleWindows, "pre-schedule", 3, "Number of future windows to pre-schedule")
	flag.StringVar(&cfg.DataDir, "data-dir", "./data/market_history", "Data directory for market history")

	var logIntervalStr string
	flag.StringVar(&logIntervalStr, "log-interval", "10s", "State log interval")
	flag.StringVar(&cfg.LogLevel, "log-level", "info", "Log level (debug, info, warn, error)")
	flag.BoolVar(&cfg.CustomFeatures, "custom-features", true, "Enable custom features")

	flag.Parse()

	if len(windows) == 0 {
		windows = intSlice{5, 15}
	}

	if len(prefixes) == 0 {
		prefixes = stringSlice{"btc-updown-"}
	}

	// Build market feeds from flags
	cfg.MarketFeeds = make([]MarketFeedConfig, len(prefixes))
	for i, prefix := range prefixes {
		feedID := ""
		if i < len(feedIDs) {
			feedID = string(feedIDs[i])
		}
		cfg.MarketFeeds[i] = MarketFeedConfig{
			Prefix:  prefix,
			FeedID:  feedID,
			Windows: []int(windows),
		}
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
	if len(c.MarketFeeds) == 0 {
		return fmt.Errorf("at least one market feed is required")
	}
	for i, mf := range c.MarketFeeds {
		if mf.Prefix == "" {
			return fmt.Errorf("market_feeds[%d].prefix is required", i)
		}
		if mf.FeedID == "" {
			return fmt.Errorf("market_feeds[%d].feed_id is required (prefix: %s)", i, mf.Prefix)
		}
		if len(mf.Windows) == 0 {
			return fmt.Errorf("market_feeds[%d].windows must have at least one entry (prefix: %s)", i, mf.Prefix)
		}
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
