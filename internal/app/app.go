package app

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/config"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/engine"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/execution"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/recorder"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/httpserver"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/connectors"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/connectors/binance/perps"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/connectors/binance/spot"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/connectors/chainlink"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/connectors/polymarket/gamma"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/connectors/polymarket/market"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/connectors/polymarket/tracker"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/lifecycle"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/logging"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/persistence"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/sampler"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/state"
)

type App struct {
	config      *config.Config
	logger      *zerolog.Logger
	supervisor  *lifecycle.Supervisor
	state       *state.StateStore
	persistence *persistence.Writer
	logWriter   *logging.RotatingWriter
	connectors  []connectors.Connector
	trackerConn   *tracker.Connector
	chainlinkConn *chainlink.Connector
	decisionEngine *engine.Engine
	recorder       *recorder.Recorder
	running       bool
}

func New(cfgPath string) (*App, error) {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	logger, logWriter, err := logging.New(&cfg.Logging)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize logger: %w", err)
	}

	appLogger := logging.Component(logger, "app")

	supervisor := lifecycle.NewSupervisor(appLogger)

	store := state.NewStateStore(cfg.Symbols())

	app := &App{
		config:     cfg,
		logger:     appLogger,
		supervisor: supervisor,
		state:      store,
		logWriter:  logWriter,
		connectors: make([]connectors.Connector, 0),
		running:    false,
	}

	if cfg.Persistence.Enabled {
		persistLogger := logging.Component(logger, "persistence")
		pw, err := persistence.NewWriter(
			cfg.Persistence.NdjsonPath,
			cfg.Persistence.FlushIntervalMs,
			persistence.S3Config{
				Enabled: cfg.Persistence.S3Enabled,
				Bucket:  cfg.Persistence.S3Bucket,
				Region:  cfg.Persistence.S3Region,
				Prefix:  cfg.Persistence.S3Prefix,
			},
			persistLogger,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize persistence writer: %w", err)
		}
		app.persistence = pw
	}

	if err := app.initializeConnectors(logger); err != nil {
		return nil, fmt.Errorf("failed to initialize connectors: %w", err)
	}

	if cfg.Strategy.Enabled && app.trackerConn != nil {
		var execClient *execution.Client
		var balanceTracker *execution.BalanceTracker

		if cfg.Strategy.Mode == "live" {
			execCfg := execution.Config{
				URL:                cfg.Execution.URL,
				Timeout:            time.Duration(cfg.Execution.TimeoutMs) * time.Millisecond,
				RedeemDelay:        time.Duration(cfg.Execution.RedeemDelaySecs) * time.Second,
				RedeemPollInterval: time.Duration(cfg.Execution.RedeemPollIntervalSecs) * time.Second,
				RedeemTimeout:      time.Duration(cfg.Execution.RedeemTimeoutSecs) * time.Second,
			}
			execLogger := logging.Component(logger, "execution")
			execClient = execution.NewClient(execCfg, execLogger)

			if err := execClient.HealthCheck(context.Background()); err != nil {
				return nil, fmt.Errorf("execution engine health check failed: %w", err)
			}

			balanceTracker = execution.NewBalanceTracker(execLogger)
			balance, err := execClient.GetCollateralBalance(context.Background())
			if err != nil {
				return nil, fmt.Errorf("failed to get initial balance: %w", err)
			}
			balanceTracker.Seed(balance)

			app.trackerConn.SetExecutionClient(execClient, balanceTracker)

			appLogger.Info().
				Float64("initial_balance", balance).
				Str("execution_url", cfg.Execution.URL).
				Msg("live trading enabled")
		}

		engineLogger := logging.Component(logger, "engine")
		app.decisionEngine = engine.NewEngine(
			app.state, app.trackerConn, &cfg.Strategy,
			execClient, balanceTracker,
			app.persistence, engineLogger,
		)
	}

	if cfg.Recorder.Enabled && app.trackerConn != nil {
		recorderLogger := logging.Component(logger, "recorder")
		getWindows := func() []tracker.ActiveWindowSummary {
			return app.trackerConn.GetActiveWindowSummaries()
		}
		symbols := cfg.Symbols()
		interval := time.Duration(cfg.Recorder.IntervalSecs) * time.Second
		s3Cfg := recorder.S3Config{
			Enabled: cfg.Recorder.S3Enabled,
			Bucket:  cfg.Recorder.S3Bucket,
			Region:  cfg.Recorder.S3Region,
			Prefix:  cfg.Recorder.S3Prefix,
		}
		rec, err := recorder.New(app.state, getWindows, symbols, interval, cfg.Recorder.OutputDir, s3Cfg, recorderLogger)
		if err != nil {
			logger.Error().Err(err).Msg("failed to initialize recorder")
		} else {
			app.recorder = rec
		}
	}

	return app, nil
}

func (a *App) initializeConnectors(logger *zerolog.Logger) error {
	// Build lookup tables from markets config
	feedIDs := make(map[string]string, len(a.config.Markets))         // symbol -> feedID
	feedIDToSymbol := make(map[string]string, len(a.config.Markets))  // feedID -> symbol
	binanceSymbols := make([]string, 0, len(a.config.Markets))
	binanceToSymbol := make(map[string]string, len(a.config.Markets)) // "BTCUSDT" -> "BTC"
	perpsSymbols := make([]string, 0, len(a.config.Markets))
	perpsToSymbol := make(map[string]string, len(a.config.Markets))

	for _, m := range a.config.Markets {
		feedIDs[m.Symbol] = m.ChainlinkFeedID
		feedIDToSymbol[m.ChainlinkFeedID] = m.Symbol
		binanceSymbols = append(binanceSymbols, strings.ToLower(m.BinanceSymbol))
		binanceToSymbol[m.BinanceSymbol] = m.Symbol
		perpsSymbols = append(perpsSymbols, strings.ToLower(m.BinanceSymbol))
		perpsToSymbol[m.BinanceSymbol] = m.Symbol
	}

	// Chainlink connector
	chainlinkCfg := &chainlink.Config{
		RestURL:        a.config.Chainlink.RestURL,
		WsURL:          a.config.Chainlink.WsURL,
		ApiKey:         a.config.Chainlink.ApiKey,
		ApiSecret:      a.config.Chainlink.ApiSecret,
		FeedIDs:        feedIDs,
		FeedIDToSymbol: feedIDToSymbol,
		WsHA:           a.config.Chainlink.WsHA,
		WsMaxReconnect: a.config.Chainlink.WsMaxReconnect,
		StaleThreshold: time.Duration(a.config.Risk.ChainlinkStaleMs) * time.Millisecond,
	}

	chainlinkConnector := chainlink.NewConnector(
		chainlinkCfg,
		a.state,
		a.persistence,
		logger,
	)
	a.connectors = append(a.connectors, chainlinkConnector)
	a.chainlinkConn = chainlinkConnector

	// Binance spot connector
	binanceSpotCfg := &spot.Config{
		Symbols:        binanceSymbols,
		SymbolToKey:    binanceToSymbol,
		WsURL:          a.config.BinanceSpot.WsURL,
		RestURL:        a.config.BinanceSpot.RestURL,
		DepthSpeed:     a.config.BinanceSpot.DepthSpeed,
		MaxDepthLevels: a.config.BinanceSpot.DepthLimit,
		StaleThreshold: time.Duration(a.config.Risk.BinanceStaleMs) * time.Millisecond,
	}

	binanceSpotConnector := spot.NewConnector(
		binanceSpotCfg,
		a.state,
		a.persistence,
		logger,
	)
	a.connectors = append(a.connectors, binanceSpotConnector)

	// Binance perps connector
	binancePerpsCfg := &perps.Config{
		Symbols:         perpsSymbols,
		SymbolToKey:     perpsToSymbol,
		WsURL:           a.config.BinancePerps.WsURL,
		StaleThreshold:  time.Duration(a.config.Risk.BinanceStaleMs) * time.Millisecond,
		MaxLiquidations: 100,
	}

	binancePerpsConnector := perps.NewConnector(
		binancePerpsCfg,
		a.state,
		a.persistence,
		logger,
	)
	a.connectors = append(a.connectors, binancePerpsConnector)

	// Polymarket market connector
	marketCfg := &market.Config{
		WsURL:                a.config.Polymarket.WsURL,
		CustomFeatureEnabled: a.config.Polymarket.Tracker.CustomFeatures,
		ReconnectMinWait:     market.DefaultReconnectMinWait,
		ReconnectMaxWait:     market.DefaultReconnectMaxWait,
		PingPeriod:           market.DefaultPingPeriod,
		PongWait:             market.DefaultPongWait,
		WriteWait:            market.DefaultWriteWait,
	}

	marketConnector := market.NewConnector(marketCfg, a.state, logger)
	a.connectors = append(a.connectors, marketConnector)

	// Polymarket tracker connector
	if a.config.Polymarket.Tracker.Enabled {
		// Create a separate chainlink client for the tracker (REST-only for historical queries)
		trackerChainlinkCfg := &chainlink.Config{
			RestURL:        a.config.Chainlink.RestURL,
			WsURL:          a.config.Chainlink.WsURL,
			ApiKey:         a.config.Chainlink.ApiKey,
			ApiSecret:      a.config.Chainlink.ApiSecret,
			FeedIDs:        feedIDs,
			FeedIDToSymbol: feedIDToSymbol,
			WsHA:           a.config.Chainlink.WsHA,
			WsMaxReconnect: a.config.Chainlink.WsMaxReconnect,
		}

		chainlinkClient, err := chainlink.NewClient(trackerChainlinkCfg, logger)
		if err != nil {
			return fmt.Errorf("create chainlink client for tracker: %w", err)
		}

		if err := chainlinkClient.Initialize(); err != nil {
			return fmt.Errorf("initialize chainlink client for tracker: %w", err)
		}

		trackerCfg := &tracker.TrackerConfig{
			PreScheduleWindows:     a.config.Polymarket.Tracker.PreScheduleWindows,
			CustomFeatures:         a.config.Polymarket.Tracker.CustomFeatures,
			RedeemDelaySecs:        a.config.Execution.RedeemDelaySecs,
			RedeemMaxRetries:       a.config.Execution.RedeemMaxRetries,
			RedeemLoopIntervalSecs: a.config.Execution.RedeemLoopIntervalSecs,
		}

		prefixToFeedID := a.config.PrefixToFeedID()
		prefixToWindows := a.config.PrefixToWindows()
		prefixToSymbol := a.config.PrefixToSymbol()

		gammaClient := gamma.NewClient(gamma.ClientConfig{
			BaseURL: a.config.Polymarket.GammaURL,
		})

		marketConn, ok := marketConnector.(*market.Connector)
		if !ok {
			return fmt.Errorf("failed to cast market connector")
		}

		trackerConnector := tracker.NewConnector(
			gammaClient,
			marketConn,
			chainlinkClient,
			trackerCfg,
			prefixToFeedID,
			prefixToWindows,
			prefixToSymbol,
			a.state,
			a.persistence,
			logger,
		)
		a.connectors = append(a.connectors, trackerConnector)
		a.trackerConn = trackerConnector.(*tracker.Connector)
	}

	a.logger.Info().
		Int("count", len(a.connectors)).
		Strs("symbols", a.config.Symbols()).
		Msg("connectors initialized")

	return nil
}

func (a *App) Run() error {
	a.logger.Info().
		Str("mode", a.config.App.Mode).
		Strs("symbols", a.config.Symbols()).
		Msg("application starting")

	a.supervisor.SetRunning(true)
	a.running = true

	a.logger.Info().
		Int("tick_interval_ms", a.config.App.TickIntervalMs).
		Bool("persistence_enabled", a.config.Persistence.Enabled).
		Bool("kill_switch", a.config.Risk.KillSwitch).
		Int("connectors", len(a.connectors)).
		Int("markets", len(a.config.Markets)).
		Msg("configuration loaded")

	a.state.UpdateMeta(func(m *state.MetaState) {
		m.IsHealthy = true
	})

	if a.logWriter != nil {
		if err := a.logWriter.Start(a.supervisor.Context()); err != nil {
			a.logger.Error().Err(err).Msg("failed to start log writer S3 archival")
			return err
		}
	}

	if a.persistence != nil {
		if err := a.persistence.Start(a.supervisor.Context(), a.supervisor.WaitGroup()); err != nil {
			a.logger.Error().Err(err).Msg("failed to start persistence writer")
			return err
		}
	}

	if err := a.startConnectors(); err != nil {
		a.logger.Error().Err(err).Msg("failed to start connectors")
		return err
	}

	if a.config.Sampler.Enabled {
		ctx := a.supervisor.Context()
		getWindows := func() []sampler.WindowSummary {
			if a.trackerConn == nil {
				return nil
			}
			sums := a.trackerConn.GetActiveWindowSummaries()
			out := make([]sampler.WindowSummary, len(sums))
			for i, s := range sums {
				out[i] = sampler.WindowSummary{
					Slug:           s.Slug,
					Symbol:         s.Symbol,
					WindowMin:      s.WindowMin,
					StartPrice:     s.StartPrice,
					EndTime:        s.EndTime,
					TimeRemainingS: s.TimeRemainingS,
					YesMid:         s.YesMid,
					YesBid:         s.YesBid,
					YesAsk:         s.YesAsk,
					NoMid:          s.NoMid,
					NoBid:          s.NoBid,
					NoAsk:          s.NoAsk,
				}
			}
			return out
		}
		interval := time.Duration(a.config.Sampler.IntervalSecs) * time.Second
		sm, err := sampler.New(a.state, getWindows, a.config.Symbols(), interval, a.config.Sampler.OutputPath, a.logger)
		if err != nil {
			return fmt.Errorf("failed to create sampler: %w", err)
		}
		sm.Start(ctx)
		defer sm.Stop()
	}

	if a.config.HTTPServer.Enabled {
		httpSrv := httpserver.New(
			&a.config.HTTPServer,
			a.state,
			a.chainlinkConn,
			a.trackerConn,
			a.logger,
		)
		httpSrv.Start(a.supervisor.Context())
	}

	a.logSystemEvent("application_started", map[string]interface{}{
		"mode":       a.config.App.Mode,
		"symbols":    a.config.Symbols(),
		"connectors": len(a.connectors),
	})

	return a.supervisor.RunWithSignal(a.runLoop)
}

func (a *App) startConnectors() error {
	ctx := a.supervisor.Context()

	for _, conn := range a.connectors {
		a.logger.Info().Str("connector", conn.Name()).Msg("starting connector")

		if err := conn.Start(ctx); err != nil {
			a.logger.Error().
				Err(err).
				Str("connector", conn.Name()).
				Msg("failed to start connector")
			return err
		}

		a.logConnectorEvent(conn.Name(), persistence.EventConnector, "started", nil)
	}

	return nil
}

func (a *App) stopConnectors() {
	for _, conn := range a.connectors {
		a.logger.Info().Str("connector", conn.Name()).Msg("stopping connector")

		if err := conn.Stop(); err != nil {
			a.logger.Error().
				Err(err).
				Str("connector", conn.Name()).
				Msg("error stopping connector")
		}

		a.logConnectorEvent(conn.Name(), persistence.EventConnector, "stopped", nil)
	}
}

func (a *App) runLoop() error {
	ctx := a.supervisor.Context()

	a.logger.Info().Msg("entering main run loop")

	if a.decisionEngine != nil {
		a.supervisor.Add(1)
		go func() {
			defer a.supervisor.Done()
			if err := a.decisionEngine.Run(ctx); err != nil {
				a.logger.Error().Err(err).Msg("decision engine error")
			}
		}()
	}

	if a.recorder != nil {
		a.supervisor.Add(1)
		go func() {
			defer a.supervisor.Done()
			if err := a.recorder.Run(ctx); err != nil {
				a.logger.Error().Err(err).Msg("recorder error")
			}
		}()
	}

	<-ctx.Done()

	a.logSystemEvent("application_shutdown_initiated", map[string]interface{}{
		"reason": "context_cancelled",
	})

	a.logger.Info().Msg("context cancelled, exiting run loop")
	return nil
}

func (a *App) Shutdown() {
	if !a.running {
		return
	}

	a.logger.Info().Msg("application shutdown initiated")

	a.state.UpdateMeta(func(m *state.MetaState) {
		m.IsHealthy = false
	})

	a.stopConnectors()

	if a.persistence != nil {
		a.logger.Info().Msg("stopping persistence writer")
		a.persistence.Stop()
	}

	if a.logWriter != nil {
		a.logWriter.Stop()
	}

	a.supervisor.Shutdown()

	a.logger.Info().Msg("application shutdown complete")
	a.running = false
}

func (a *App) Health() error {
	if !a.running {
		return fmt.Errorf("application not running")
	}

	if err := a.supervisor.Context().Err(); err != nil {
		return fmt.Errorf("supervisor context error: %w", err)
	}

	for _, conn := range a.connectors {
		if err := conn.Health(); err != nil {
			return fmt.Errorf("connector %s unhealthy: %w", conn.Name(), err)
		}
	}

	return nil
}

func (a *App) Config() *config.Config {
	return a.config
}

func (a *App) State() *state.StateStore {
	return a.state
}

func (a *App) Logger() *zerolog.Logger {
	return a.logger
}

func (a *App) Persistence() *persistence.Writer {
	return a.persistence
}

func (a *App) Connectors() []connectors.Connector {
	return a.connectors
}

func (a *App) logSystemEvent(message string, payload map[string]interface{}) {
	if a.persistence == nil {
		return
	}

	event := persistence.NewEvent("app", persistence.EventSystem, message)
	if payload != nil {
		event.WithPayloads(payload)
	}
	a.persistence.Write(event)
}

func (a *App) logConnectorEvent(connector string, eventType persistence.EventType, message string, payload map[string]interface{}) {
	if a.persistence == nil {
		return
	}

	event := persistence.NewEvent(connector, eventType, message)
	if payload != nil {
		event.WithPayloads(payload)
	}
	a.persistence.Write(event)
}

func (a *App) LogEvent(component string, eventType persistence.EventType, message string, payload map[string]interface{}) {
	if a.persistence == nil {
		return
	}

	event := persistence.NewEvent(component, eventType, message)
	if payload != nil {
		event.WithPayloads(payload)
	}
	a.persistence.Write(event)
}
