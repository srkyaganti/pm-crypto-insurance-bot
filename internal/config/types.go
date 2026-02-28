package config

type Config struct {
	App          AppConfig          `yaml:"app"`
	Logging      LoggingConfig      `yaml:"logging"`
	Chainlink    ChainlinkConfig    `yaml:"chainlink"`
	BinanceSpot  BinanceSpotConfig  `yaml:"binance_spot"`
	BinancePerps BinancePerpsConfig `yaml:"binance_perps"`
	Polymarket   PolymarketConfig   `yaml:"polymarket"`
	Risk         RiskConfig         `yaml:"risk"`
	Persistence  PersistenceConfig  `yaml:"persistence"`
}

type AppConfig struct {
	Mode           string `yaml:"mode"`
	Symbol         string `yaml:"symbol"`
	TickIntervalMs int    `yaml:"tick_interval_ms"`
}

type LoggingConfig struct {
	Level      string `yaml:"level"`
	FilePath   string `yaml:"file_path"`
	MaxSizeMB  int    `yaml:"max_size_mb"`
	MaxBackups int    `yaml:"max_backups"`
	MaxAgeDays int    `yaml:"max_age_days"`
	Compress   bool   `yaml:"compress"`
}

type ChainlinkConfig struct {
	RestURL        string `yaml:"rest_url"`
	WsURL          string `yaml:"ws_url"`
	ApiKey         string `yaml:"api_key"`
	ApiSecret      string `yaml:"api_secret"`
	FeedID         string `yaml:"feed_id"`
	WsHA           bool   `yaml:"ws_ha"`
	WsMaxReconnect int    `yaml:"ws_max_reconnect"`
}

type BinanceSpotConfig struct {
	Symbol     string `yaml:"symbol"`
	WsURL      string `yaml:"ws_url"`
	RestURL    string `yaml:"rest_url"`
	DepthSpeed string `yaml:"depth_speed"`
	DepthLimit int    `yaml:"depth_limit"`
}

type BinancePerpsConfig struct {
	Symbol string `yaml:"symbol"`
	WsURL  string `yaml:"ws_url"`
}

type PolymarketConfig struct {
	ClobURL     string `yaml:"clob_url"`
	WsURL       string `yaml:"ws_url"`
	GammaURL    string `yaml:"gamma_url"`
	ApiKey      string `yaml:"api_key"`
	ApiSecret   string `yaml:"api_secret"`
	Passphrase  string `yaml:"passphrase"`
	PrivateKey  string `yaml:"private_key"`
	TokenID     string `yaml:"token_id"`
	ConditionID string `yaml:"condition_id"`
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
