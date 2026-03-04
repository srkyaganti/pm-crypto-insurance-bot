package app

import (
	"fmt"
	"time"

	"github.com/rs/zerolog"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/config"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/connectors"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/connectors/binance/perps"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/connectors/binance/spot"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/connectors/chainlink"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/lifecycle"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/logging"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/persistence"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/state"
)

type App struct {
	config      *config.Config
	logger      *zerolog.Logger
	supervisor  *lifecycle.Supervisor
	state       *state.StateStore
	persistence *persistence.Writer
	connectors  []connectors.Connector
	running     bool
}

func New(cfgPath string) (*App, error) {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	logger, err := logging.New(&cfg.Logging)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize logger: %w", err)
	}

	appLogger := logging.Component(logger, "app")

	supervisor := lifecycle.NewSupervisor(appLogger)

	store := state.NewStateStore()

	app := &App{
		config:     cfg,
		logger:     appLogger,
		supervisor: supervisor,
		state:      store,
		connectors: make([]connectors.Connector, 0),
		running:    false,
	}

	if cfg.Persistence.Enabled {
		persistLogger := logging.Component(logger, "persistence")
		pw, err := persistence.NewWriter(
			cfg.Persistence.NdjsonPath,
			cfg.Persistence.FlushIntervalMs,
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

	return app, nil
}

func (a *App) initializeConnectors(logger *zerolog.Logger) error {
	chainlinkCfg := &chainlink.Config{
		RestURL:        a.config.Chainlink.RestURL,
		WsURL:          a.config.Chainlink.WsURL,
		ApiKey:         a.config.Chainlink.ApiKey,
		ApiSecret:      a.config.Chainlink.ApiSecret,
		FeedID:         a.config.Chainlink.FeedID,
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

	binanceSpotCfg := &spot.Config{
		Symbol:         a.config.BinanceSpot.Symbol,
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

	binancePerpsCfg := &perps.Config{
		Symbol:          a.config.BinancePerps.Symbol,
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

	a.logger.Info().
		Int("count", len(a.connectors)).
		Msg("connectors initialized")

	return nil
}

func (a *App) Run() error {
	a.logger.Info().
		Str("mode", a.config.App.Mode).
		Str("symbol", a.config.App.Symbol).
		Msg("application starting")

	a.supervisor.SetRunning(true)
	a.running = true

	a.logger.Info().
		Int("tick_interval_ms", a.config.App.TickIntervalMs).
		Bool("persistence_enabled", a.config.Persistence.Enabled).
		Bool("kill_switch", a.config.Risk.KillSwitch).
		Int("connectors", len(a.connectors)).
		Msg("configuration loaded")

	a.state.UpdateMeta(func(m *state.MetaState) {
		m.IsHealthy = true
	})

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

	a.logSystemEvent("application_started", map[string]interface{}{
		"mode":       a.config.App.Mode,
		"symbol":     a.config.App.Symbol,
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
