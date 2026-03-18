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

	onMarkPrice   func(symbol string, data *MarkPriceData)
	onLiquidation func(symbol string, data *LiquidationData)
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

func (w *WSClient) OnMarkPrice(fn func(symbol string, data *MarkPriceData)) {
	w.onMarkPrice = fn
}

func (w *WSClient) OnLiquidation(fn func(symbol string, data *LiquidationData)) {
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

	var streams []string
	for _, sym := range w.config.Symbols {
		symbolLower := strings.ToLower(sym)
		streams = append(streams, fmt.Sprintf("%s@markPrice@1s", symbolLower))
		streams = append(streams, fmt.Sprintf("%s@forceOrder", symbolLower))
	}

	return fmt.Sprintf("%s/stream?streams=%s", wsURL, strings.Join(streams, "/"))
}

func (w *WSClient) resolveSymbol(streamName string) string {
	// Extract binance symbol from stream name, e.g. "btcusdt@markPrice@1s" -> "BTCUSDT"
	parts := strings.SplitN(streamName, "@", 2)
	if len(parts) == 0 {
		return ""
	}
	binanceSymbol := strings.ToUpper(parts[0])
	if key, ok := w.config.SymbolToKey[binanceSymbol]; ok {
		return key
	}
	return ""
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
		Int("symbols", len(w.config.Symbols)).
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
		Int("symbols", len(w.config.Symbols)).
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
	symbol := w.resolveSymbol(stream)
	if symbol == "" {
		w.logger.Warn().Str("stream", stream).Msg("binance perps: unknown symbol in stream")
		return
	}

	if IsMarkPriceStream(stream) {
		markPrice, err := ParseMarkPrice(data)
		if err != nil {
			w.logger.Error().Err(err).Msg("binance perps: parse mark price")
			return
		}

		if w.onMarkPrice != nil {
			w.onMarkPrice(symbol, markPrice)
		}

	} else if IsForceOrderStream(stream) {
		liquidation, err := ParseForceOrder(data)
		if err != nil {
			w.logger.Error().Err(err).Msg("binance perps: parse force order")
			return
		}

		if w.onLiquidation != nil {
			w.onLiquidation(symbol, liquidation)
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
