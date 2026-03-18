package spot

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

	onDepthUpdate func(symbol string, data *DepthData)
	onAggTrade    func(symbol string, data *AggTradeData)
	onConnect     func()
	onDisconnect  func(err error)

	dialer *websocket.Dialer
}

func NewWSClient(cfg *Config, logger *zerolog.Logger) *WSClient {
	return &WSClient{
		config: cfg,
		logger: logger,
		stopCh: make(chan struct{}),
		dialer: &websocket.Dialer{
			HandshakeTimeout: 10 * time.Second,
		},
	}
}

func (w *WSClient) OnDepthUpdate(fn func(symbol string, data *DepthData)) {
	w.onDepthUpdate = fn
}

func (w *WSClient) OnAggTrade(fn func(symbol string, data *AggTradeData)) {
	w.onAggTrade = fn
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
		wsURL = "wss://stream.binance.com:9443"
	}

	var streams []string
	for _, sym := range w.config.Symbols {
		symbolLower := strings.ToLower(sym)
		streams = append(streams, fmt.Sprintf("%s@depth20@100ms", symbolLower))
		streams = append(streams, fmt.Sprintf("%s@aggTrade", symbolLower))
	}

	return fmt.Sprintf("%s/stream?streams=%s", wsURL, strings.Join(streams, "/"))
}

func (w *WSClient) resolveSymbol(streamName string) string {
	// Extract binance symbol from stream name, e.g. "btcusdt@depth20@100ms" -> "btcusdt"
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
		Msg("binance spot: connecting to websocket")

	headers := http.Header{}
	conn, resp, err := w.dialer.Dial(streamURL, headers)
	if err != nil {
		if resp != nil {
			w.logger.Error().
				Err(err).
				Int("status", resp.StatusCode).
				Msg("binance spot: websocket connection failed")
		}
		return fmt.Errorf("websocket dial failed: %w", err)
	}

	w.conn = conn
	w.connected.Store(true)

	w.logger.Info().
		Int("symbols", len(w.config.Symbols)).
		Msg("binance spot: connected to data stream")

	if w.onConnect != nil {
		w.onConnect()
	}

	return nil
}

func (w *WSClient) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			w.logger.Info().Msg("binance spot: context cancelled, stopping")
			return ctx.Err()
		case <-w.stopCh:
			w.logger.Info().Msg("binance spot: stop signal received")
			return nil
		default:
		}

		if err := w.Connect(ctx); err != nil {
			w.logger.Error().Err(err).Msg("binance spot: connection failed")
			if shouldRetry := w.handleReconnect(ctx); !shouldRetry {
				return nil
			}
			continue
		}

		if err := w.readLoop(ctx); err != nil {
			w.logger.Error().Err(err).Msg("binance spot: read loop error")
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

		var msg CombinedStreamMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			w.logger.Error().Err(err).Bytes("data", data).Msg("binance spot: failed to unmarshal message")
			continue
		}

		w.handleStreamMessage(msg.Stream, msg.Data)
	}
}

func (w *WSClient) handleStreamMessage(stream string, data json.RawMessage) {
	symbol := w.resolveSymbol(stream)
	if symbol == "" {
		w.logger.Warn().Str("stream", stream).Msg("binance spot: unknown symbol in stream")
		return
	}

	if strings.Contains(stream, "@depth20@100ms") {
		depthData, err := ParsePartialDepth(data)
		if err != nil {
			w.logger.Error().Err(err).Msg("binance spot: parse partial depth")
			return
		}

		if w.onDepthUpdate != nil {
			w.onDepthUpdate(symbol, depthData)
		}

	} else if strings.Contains(stream, "@aggTrade") {
		tradeData, err := ParseAggTrade(data)
		if err != nil {
			w.logger.Error().Err(err).Msg("binance spot: parse aggTrade")
			return
		}

		if w.onAggTrade != nil {
			w.onAggTrade(symbol, tradeData)
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
					w.logger.Debug().Err(err).Msg("binance spot: ping failed")
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
		Msg("binance spot: reconnecting after backoff")

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
		w.logger.Info().Msg("binance spot: websocket client stopped")
	})
}

func (w *WSClient) IsConnected() bool {
	return w.connected.Load()
}
