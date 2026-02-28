package perps

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
)

const (
	DefaultReconnectMinWait = 1 * time.Second
	DefaultReconnectMaxWait = 30 * time.Second
	PongWait                = 30 * time.Second
	PingPeriod              = 20 * time.Second
	WriteWait               = 10 * time.Second
)

type WSClient struct {
	config    *Config
	logger    *zerolog.Logger
	conn      *websocket.Conn
	connMu    sync.Mutex
	connected atomic.Bool
	stopCh    chan struct{}
	stopOnce  sync.Once

	onMarkPrice   func(data *MarkPriceData)
	onLiquidation func(data *LiquidationData)
	onConnect     func()
	onDisconnect  func(err error)

	dialer *websocket.Dialer
}

func NewWSClient(cfg *Config, logger *zerolog.Logger) *WSClient {
	return &WSClient{
		config: cfg,
		logger: logger,
		stopCh: make(chan struct{}),
		dialer: &websocket.Dialer{HandshakeTimeout: 10 * time.Second},
	}
}

func (w *WSClient) OnMarkPrice(fn func(data *MarkPriceData)) {
	w.onMarkPrice = fn
}

func (w *WSClient) OnLiquidation(fn func(data *LiquidationData)) {
	w.onLiquidation = fn
}

func (w *WSClient) OnConnect(fn func()) {
	w.onConnect = fn
}

func (w *WSClient) OnDisconnect(fn func(err error)) {
	w.onDisconnect = fn
}

func (w *WSClient) buildStreamURL() string {
	wsURL := w.config.WsURL
	if wsURL == "" {
		wsURL = "wss://fstream.binance.com"
	}

	symbolLower := strings.ToLower(w.config.Symbol)
	markPriceStream := fmt.Sprintf("%s@markPrice@1s", symbolLower)
	forceOrderStream := fmt.Sprintf("%s@forceOrder", symbolLower)

	return fmt.Sprintf("%s/stream?streams=%s/%s", wsURL, markPriceStream, forceOrderStream)
}

func (w *WSClient) Connect(ctx context.Context) error {
	w.connMu.Lock()
	defer w.connMu.Unlock()

	if w.connected.Load() {
		return nil
	}

	streamURL := w.buildStreamURL()

	w.logger.Debug().
		Str("url", streamURL).
		Str("symbol", w.config.Symbol).
		Msg("binance perps: connecting to websocket")

	headers := http.Header{}
	conn, resp, err := w.dialer.Dial(streamURL, headers)
	if err != nil {
		if resp != nil {
			w.logger.Error().
				Err(err).
				Int("status", resp.StatusCode).
				Msg("binance perps: websocket connection failed")
		}
		return fmt.Errorf("websocket dial failed: %w", err)
	}

	w.conn = conn
	w.connected.Store(true)

	w.logger.Info().
		Str("symbol", w.config.Symbol).
		Msg("binance perps: connected to futures stream")

	if w.onConnect != nil {
		w.onConnect()
	}

	return nil
}

func (w *WSClient) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			w.logger.Info().Msg("binance perps: context cancelled, stopping")
			return ctx.Err()
		case <-w.stopCh:
			w.logger.Info().Msg("binance perps: stop signal received")
			return nil
		default:
		}

		if err := w.Connect(ctx); err != nil {
			w.logger.Error().Err(err).Msg("binance perps: connection failed")
			if shouldRetry := w.handleReconnect(ctx); !shouldRetry {
				return nil
			}
			continue
		}

		if err := w.readLoop(ctx); err != nil {
			w.logger.Error().Err(err).Msg("binance perps: read loop error")
			w.setConnected(false)
			if w.onDisconnect != nil {
				w.onDisconnect(err)
			}
			if shouldRetry := w.handleReconnect(ctx); !shouldRetry {
				return nil
			}
		}
	}
}

func (w *WSClient) readLoop(ctx context.Context) error {
	defer w.conn.Close()

	w.conn.SetPongHandler(func(string) error {
		w.conn.SetReadDeadline(time.Now().Add(PongWait))
		return nil
	})

	go w.pingLoop(ctx)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-w.stopCh:
			return nil
		default:
		}

		w.conn.SetReadDeadline(time.Now().Add(PongWait))

		messageType, data, err := w.conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("read message: %w", err)
		}

		if messageType != websocket.TextMessage {
			continue
		}

		msg, err := ParseCombinedStreamMessage(data)
		if err != nil {
			w.logger.Error().Err(err).Msg("binance perps: parse combined stream")
			continue
		}

		w.handleStreamMessage(msg.Stream, msg.Data)
	}
}

func (w *WSClient) handleStreamMessage(stream string, data json.RawMessage) {
	if IsMarkPriceStream(stream) {
		markPrice, err := ParseMarkPrice(data)
		if err != nil {
			w.logger.Error().Err(err).Msg("binance perps: parse mark price")
			return
		}

		if w.onMarkPrice != nil {
			w.onMarkPrice(markPrice)
		}

	} else if IsForceOrderStream(stream) {
		liquidation, err := ParseForceOrder(data)
		if err != nil {
			w.logger.Error().Err(err).Msg("binance perps: parse force order")
			return
		}

		if w.onLiquidation != nil {
			w.onLiquidation(liquidation)
		}
	}
}

func (w *WSClient) pingLoop(ctx context.Context) {
	ticker := time.NewTicker(PingPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stopCh:
			return
		case <-ticker.C:
			w.connMu.Lock()
			if w.conn != nil {
				w.conn.SetWriteDeadline(time.Now().Add(WriteWait))
				if err := w.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					w.logger.Debug().Err(err).Msg("binance perps: ping failed")
				}
			}
			w.connMu.Unlock()
		}
	}
}

func (w *WSClient) handleReconnect(ctx context.Context) bool {
	backoff := DefaultReconnectMinWait
	w.logger.Info().
		Dur("backoff", backoff).
		Msg("binance perps: reconnecting after backoff")

	select {
	case <-ctx.Done():
		return false
	case <-w.stopCh:
		return false
	case <-time.After(backoff):
		return true
	}
}

func (w *WSClient) setConnected(connected bool) {
	w.connected.Store(connected)
}

func (w *WSClient) Stop() {
	w.stopOnce.Do(func() {
		close(w.stopCh)
		w.connMu.Lock()
		if w.conn != nil {
			w.conn.Close()
		}
		w.connMu.Unlock()
		w.connected.Store(false)
		w.logger.Info().Msg("binance perps: websocket client stopped")
	})
}

func (w *WSClient) IsConnected() bool {
	return w.connected.Load()
}
