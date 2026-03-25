package config

type Config struct {
	App          AppConfig          `yaml:"app"`
	Logging      LoggingConfig      `yaml:"logging"`
	Markets      []MarketDefinition `yaml:"markets"`
	Chainlink    ChainlinkConfig    `yaml:"chainlink"`
	BinanceSpot  BinanceSpotConfig  `yaml:"binance_spot"`
	BinancePerps BinancePerpsConfig `yaml:"binance_perps"`
	Polymarket   PolymarketConfig   `yaml:"polymarket"`
	Strategy     StrategyConfig     `yaml:"strategy"`
	Execution    ExecutionConfig    `yaml:"execution"`
	Risk         RiskConfig         `yaml:"risk"`
	Persistence  PersistenceConfig  `yaml:"persistence"`
	Sampler      SamplerConfig      `yaml:"sampler"`
	Recorder     RecorderConfig     `yaml:"recorder"`
	HTTPServer   HTTPServerConfig   `yaml:"http_server"`
}

type StrategyConfig struct {
	Enabled                   bool               `yaml:"enabled" json:"enabled"`
	Mode                      string             `yaml:"mode" json:"mode"`
	EntryWindowMinSecs        int                `yaml:"entry_window_min_secs" json:"entry_window_min_secs"`
	EntryWindowMaxSecs        int                `yaml:"entry_window_max_secs" json:"entry_window_max_secs"`
	MinPMConfidence           map[string]float64 `yaml:"min_pm_confidence" json:"min_pm_confidence"`
	MaxCLBinanceDivergencePct map[string]float64 `yaml:"max_cl_binance_divergence_pct" json:"max_cl_binance_divergence_pct"`
	DepthSafetyMultiplier     map[string]float64 `yaml:"depth_safety_multiplier" json:"depth_safety_multiplier"`
	MinGapPct                 map[string]float64 `yaml:"min_gap_pct" json:"min_gap_pct"`
	MinDepth                  map[string]float64 `yaml:"min_depth" json:"min_depth"`
	MaxEntryPrice             map[string]float64 `yaml:"max_entry_price" json:"max_entry_price"`
	MaxSharesPerTrade         int                `yaml:"max_shares_per_trade" json:"max_shares_per_trade"`
	MaxConcurrentPositions    int                `yaml:"max_concurrent_positions" json:"max_concurrent_positions"`
	TradeSizeUSD              float64            `yaml:"trade_size_usd" json:"trade_size_usd"`
	StopLoss                  StopLossConfig     `yaml:"stop_loss" json:"stop_loss"`
}

type StopLossConfig struct {
	Enabled               bool               `yaml:"enabled" json:"enabled"`
	StopPrice             map[string]float64  `yaml:"stop_price" json:"stop_price"`
	ConfirmTicks          int                 `yaml:"confirm_ticks" json:"confirm_ticks"`
	MinTimeRemainingSecs  int                 `yaml:"min_time_remaining_s" json:"min_time_remaining_s"`
	EmptyBookTimeoutSecs  int                 `yaml:"empty_book_timeout_s" json:"empty_book_timeout_s"`
	EmptyBookSpreadThresh float64             `yaml:"empty_book_spread_thresh" json:"empty_book_spread_thresh"`
	MinSellPrice          float64             `yaml:"min_sell_price" json:"min_sell_price"`
}

type PolymarketMarketConfig struct {
	Prefix  string `yaml:"prefix"`
	Windows []int  `yaml:"windows"`
}

type MarketDefinition struct {
	Symbol          string                 `yaml:"symbol"`
	ChainlinkFeedID string                 `yaml:"chainlink_feed_id"`
	BinanceSymbol   string                 `yaml:"binance_symbol"`
	Polymarket      PolymarketMarketConfig `yaml:"polymarket"`
}

type AppConfig struct {
	Mode           string `yaml:"mode"`
	TickIntervalMs int    `yaml:"tick_interval_ms"`
}

type LoggingConfig struct {
	Level      string `yaml:"level"`
	FilePath   string `yaml:"file_path"`
	MaxSizeMB  int    `yaml:"max_size_mb"`
	MaxBackups int    `yaml:"max_backups"`
	MaxAgeDays int    `yaml:"max_age_days"`
	Compress   bool   `yaml:"compress"`
	S3Enabled  bool   `yaml:"s3_enabled"`
	S3Bucket   string `yaml:"s3_bucket"`
	S3Region   string `yaml:"s3_region"`
	S3Prefix   string `yaml:"s3_prefix"`
}

type ChainlinkConfig struct {
	RestURL        string `yaml:"rest_url"`
	WsURL          string `yaml:"ws_url"`
	ApiKey         string `yaml:"api_key"`
	ApiSecret      string `yaml:"api_secret"`
	WsHA           bool   `yaml:"ws_ha"`
	WsMaxReconnect int    `yaml:"ws_max_reconnect"`
}

type BinanceSpotConfig struct {
	WsURL      string `yaml:"ws_url"`
	RestURL    string `yaml:"rest_url"`
	DepthSpeed string `yaml:"depth_speed"`
	DepthLimit int    `yaml:"depth_limit"`
}

type BinancePerpsConfig struct {
	WsURL string `yaml:"ws_url"`
}

type PolymarketConfig struct {
	ClobURL    string        `yaml:"clob_url"`
	WsURL      string        `yaml:"ws_url"`
	GammaURL   string        `yaml:"gamma_url"`
	ApiKey     string        `yaml:"api_key"`
	ApiSecret  string        `yaml:"api_secret"`
	Passphrase string        `yaml:"passphrase"`
	PrivateKey string        `yaml:"private_key"`
	Tracker    TrackerConfig `yaml:"tracker"`
}

type TrackerConfig struct {
	Enabled            bool `yaml:"enabled"`
	PreScheduleWindows int  `yaml:"pre_schedule_windows"`
	CustomFeatures     bool `yaml:"custom_features"`
}

type SamplerConfig struct {
	Enabled      bool   `yaml:"enabled"`
	OutputPath   string `yaml:"output_path"`
	IntervalSecs int    `yaml:"interval_secs"`
}

type RecorderConfig struct {
	Enabled      bool   `yaml:"enabled"`
	OutputDir    string `yaml:"output_dir"`
	IntervalSecs int    `yaml:"interval_secs"`
	S3Enabled    bool   `yaml:"s3_enabled"`
	S3Bucket     string `yaml:"s3_bucket"`
	S3Region     string `yaml:"s3_region"`
	S3Prefix     string `yaml:"s3_prefix"`
}

type HTTPServerConfig struct {
	Enabled        bool   `yaml:"enabled"`
	ListenAddr     string `yaml:"listen_addr"`
	ReadTimeoutMs  int    `yaml:"read_timeout_ms"`
	WriteTimeoutMs int    `yaml:"write_timeout_ms"`
}

type ExecutionConfig struct {
	URL                    string `yaml:"url"`
	TimeoutMs              int    `yaml:"timeout_ms"`
	RedeemDelaySecs        int    `yaml:"redeem_delay_secs"`
	RedeemPollIntervalSecs int    `yaml:"redeem_poll_interval_secs"`
	RedeemTimeoutSecs      int    `yaml:"redeem_timeout_secs"`
	RedeemMaxRetries       int    `yaml:"redeem_max_retries"`
	RedeemLoopIntervalSecs int    `yaml:"redeem_loop_interval_secs"`
}

type RiskConfig struct {
	KillSwitch          bool    `yaml:"kill_switch"`
	MaxDailyLosses      int     `yaml:"max_daily_losses"`
	MaxOrderSize        float64 `yaml:"max_order_size"`
	ChainlinkStaleMs    int     `yaml:"chainlink_stale_ms"`
	BinanceStaleMs      int     `yaml:"binance_stale_ms"`
	PolymarketStaleMs   int     `yaml:"polymarket_stale_ms"`
	MaxMove30sPct       float64 `yaml:"max_move_30s_pct"`
	MaxMove60sPct       float64 `yaml:"max_move_60s_pct"`
	RecoveryCooldownSec int     `yaml:"recovery_cooldown_sec"`
}

type PersistenceConfig struct {
	Enabled          bool   `yaml:"enabled"`
	NdjsonPath       string `yaml:"ndjson_path"`
	RotationInterval int    `yaml:"rotation_interval"`
	FlushIntervalMs  int    `yaml:"flush_interval_ms"`
	S3Enabled        bool   `yaml:"s3_enabled"`
	S3Bucket         string `yaml:"s3_bucket"`
	S3Region         string `yaml:"s3_region"`
	S3Prefix         string `yaml:"s3_prefix"`
}

// Helper methods

func (c *Config) Symbols() []string {
	symbols := make([]string, len(c.Markets))
	for i, m := range c.Markets {
		symbols[i] = m.Symbol
	}
	return symbols
}

func (c *Config) AllChainlinkFeedIDs() []string {
	ids := make([]string, len(c.Markets))
	for i, m := range c.Markets {
		ids[i] = m.ChainlinkFeedID
	}
	return ids
}

func (c *Config) AllBinanceSymbols() []string {
	symbols := make([]string, len(c.Markets))
	for i, m := range c.Markets {
		symbols[i] = m.BinanceSymbol
	}
	return symbols
}

// FeedIDToSymbol returns feedID hex -> canonical symbol (e.g. "0x00..." -> "BTC")
func (c *Config) FeedIDToSymbol() map[string]string {
	m := make(map[string]string, len(c.Markets))
	for _, mkt := range c.Markets {
		m[mkt.ChainlinkFeedID] = mkt.Symbol
	}
	return m
}

// BinanceToSymbol returns lowercase binance symbol -> canonical symbol (e.g. "btcusdt" -> "BTC")
func (c *Config) BinanceToSymbol() map[string]string {
	m := make(map[string]string, len(c.Markets))
	for _, mkt := range c.Markets {
		m[mkt.BinanceSymbol] = mkt.Symbol
	}
	return m
}

// PrefixToFeedID returns polymarket prefix -> chainlink feed ID (e.g. "btc-updown-" -> "0x00...")
func (c *Config) PrefixToFeedID() map[string]string {
	m := make(map[string]string, len(c.Markets))
	for _, mkt := range c.Markets {
		m[mkt.Polymarket.Prefix] = mkt.ChainlinkFeedID
	}
	return m
}

// PrefixToWindows returns polymarket prefix -> window durations (e.g. "btc-updown-" -> [5, 15])
func (c *Config) PrefixToWindows() map[string][]int {
	m := make(map[string][]int, len(c.Markets))
	for _, mkt := range c.Markets {
		m[mkt.Polymarket.Prefix] = mkt.Polymarket.Windows
	}
	return m
}

// PrefixToSymbol returns polymarket prefix -> canonical symbol (e.g. "btc-updown-" -> "BTC")
func (c *Config) PrefixToSymbol() map[string]string {
	m := make(map[string]string, len(c.Markets))
	for _, mkt := range c.Markets {
		m[mkt.Polymarket.Prefix] = mkt.Symbol
	}
	return m
}
