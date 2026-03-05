package market

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
)

type WSClient struct {
	config    *Config
	logger    *zerolog.Logger
	conn      *websocket.Conn
	connMu    sync.Mutex
	connected atomic.Bool
	stopCh    chan struct{}
	stopOnce  sync.Once

	subManager *SubscriptionManager

	onBook           func(*BookEvent)
	onPriceChange    func(*PriceChangeEvent)
	onTickSizeChange func(*TickSizeChangeEvent)
	onLastTradePrice func(*LastTradePriceEvent)
	onBestBidAsk     func(*BestBidAskEvent)
	onNewMarket      func(*NewMarketEvent)
	onMarketResolved func(*MarketResolvedEvent)
	onConnect        func()
	onDisconnect     func(error)

	dialer *websocket.Dialer
}

func NewWSClient(cfg *Config, logger *zerolog.Logger) *WSClient {
	if cfg.WsURL == "" {
		cfg.WsURL = DefaultWsURL
	}
	if cfg.ReconnectMinWait == 0 {
		cfg.ReconnectMinWait = DefaultReconnectMinWait
	}
	if cfg.ReconnectMaxWait == 0 {
		cfg.ReconnectMaxWait = DefaultReconnectMaxWait
	}
	if cfg.PingPeriod == 0 {
		cfg.PingPeriod = DefaultPingPeriod
	}
	if cfg.PongWait == 0 {
		cfg.PongWait = DefaultPongWait
	}
	if cfg.WriteWait == 0 {
		cfg.WriteWait = DefaultWriteWait
	}

	client := &WSClient{
		config: cfg,
		logger: logger,
		stopCh: make(chan struct{}),
		dialer: &websocket.Dialer{HandshakeTimeout: 10 * time.Second},
	}

	client.subManager = newSubscriptionManager(client)
	return client
}

func (w *WSClient) OnBook(fn func(*BookEvent)) {
	w.onBook = fn
}

func (w *WSClient) OnPriceChange(fn func(*PriceChangeEvent)) {
	w.onPriceChange = fn
}

func (w *WSClient) OnTickSizeChange(fn func(*TickSizeChangeEvent)) {
	w.onTickSizeChange = fn
}

func (w *WSClient) OnLastTradePrice(fn func(*LastTradePriceEvent)) {
	w.onLastTradePrice = fn
}

func (w *WSClient) OnBestBidAsk(fn func(*BestBidAskEvent)) {
	w.onBestBidAsk = fn
}

func (w *WSClient) OnNewMarket(fn func(*NewMarketEvent)) {
	w.onNewMarket = fn
}

func (w *WSClient) OnMarketResolved(fn func(*MarketResolvedEvent)) {
	w.onMarketResolved = fn
}

func (w *WSClient) OnConnect(fn func()) {
	w.onConnect = fn
}

func (w *WSClient) OnDisconnect(fn func(error)) {
	w.onDisconnect = fn
}

func (w *WSClient) Connect(ctx context.Context) error {
	w.connMu.Lock()
	defer w.connMu.Unlock()

	if w.connected.Load() {
		return nil
	}

	w.logger.Debug().
		Str("url", w.config.WsURL).
		Msg("polymarket market: connecting to websocket")

	headers := http.Header{}
	conn, resp, err := w.dialer.Dial(w.config.WsURL, headers)
	if err != nil {
		if resp != nil {
			w.logger.Error().
				Err(err).
				Int("status", resp.StatusCode).
				Msg("polymarket market: websocket connection failed")
		}
		return fmt.Errorf("websocket dial failed: %w", err)
	}

	w.conn = conn
	w.connected.Store(true)

	w.logger.Info().Msg("polymarket market: connected to data stream")

	if w.onConnect != nil {
		w.onConnect()
	}

	return nil
}

func (w *WSClient) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			w.logger.Info().Msg("polymarket market: context cancelled, stopping")
			return ctx.Err()
		case <-w.stopCh:
			w.logger.Info().Msg("polymarket market: stop signal received")
			return nil
		default:
		}

		if err := w.Connect(ctx); err != nil {
			w.logger.Error().Err(err).Msg("polymarket market: connection failed")
			if shouldRetry := w.handleReconnect(ctx); !shouldRetry {
				return nil
			}
			continue
		}

		if err := w.readLoop(ctx); err != nil {
			w.logger.Error().Err(err).Msg("polymarket market: read loop error")
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
		w.conn.SetReadDeadline(time.Now().Add(w.config.PongWait))
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

		w.conn.SetReadDeadline(time.Now().Add(w.config.PongWait))

		messageType, data, err := w.conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("read message: %w", err)
		}

		if messageType != websocket.TextMessage {
			continue
		}

		w.handleMessage(data)
	}
}

func (w *WSClient) handleMessage(data []byte) {
	if len(data) == 0 {
		return
	}

	dataStr := string(data)
	if !json.Valid(data) {
		if dataStr == "INVALID OPERATION" || dataStr == "INVALID" {
			w.logger.Error().Str("message", dataStr).Msg("polymarket market: received protocol error from server")
		} else {
			w.logger.Warn().Str("message", dataStr).Msg("polymarket market: received non-JSON message")
		}
		return
	}

	if data[0] == '[' {
		var events []json.RawMessage
		if err := json.Unmarshal(data, &events); err != nil {
			w.logger.Error().Err(err).Bytes("data", data).Msg("polymarket market: failed to parse event array")
			return
		}
		for _, event := range events {
			w.handleSingleMessage(event)
		}
	} else {
		w.handleSingleMessage(data)
	}
}

func (w *WSClient) handleSingleMessage(data []byte) {
	var baseMsg struct {
		EventType string `json:"event_type"`
	}
	if err := json.Unmarshal(data, &baseMsg); err != nil {
		w.logger.Error().Err(err).Bytes("data", data).Msg("polymarket market: failed to parse event type")
		return
	}

	switch baseMsg.EventType {
	case "book":
		event, err := ParseBookEvent(data)
		if err != nil {
			w.logger.Error().Err(err).Msg("polymarket market: parse book event")
			return
		}
		if w.onBook != nil {
			w.onBook(event)
		}

	case "price_change":
		event, err := ParsePriceChangeEvent(data)
		if err != nil {
			w.logger.Error().Err(err).Msg("polymarket market: parse price_change event")
			return
		}
		if w.onPriceChange != nil {
			w.onPriceChange(event)
		}

	case "tick_size_change":
		event, err := ParseTickSizeChangeEvent(data)
		if err != nil {
			w.logger.Error().Err(err).Msg("polymarket market: parse tick_size_change event")
			return
		}
		if w.onTickSizeChange != nil {
			w.onTickSizeChange(event)
		}

	case "last_trade_price":
		event, err := ParseLastTradePriceEvent(data)
		if err != nil {
			w.logger.Error().Err(err).Msg("polymarket market: parse last_trade_price event")
			return
		}
		if w.onLastTradePrice != nil {
			w.onLastTradePrice(event)
		}

	case "best_bid_ask":
		event, err := ParseBestBidAskEvent(data)
		if err != nil {
			w.logger.Error().Err(err).Msg("polymarket market: parse best_bid_ask event")
			return
		}
		if w.onBestBidAsk != nil {
			w.onBestBidAsk(event)
		}

	case "new_market":
		event, err := ParseNewMarketEvent(data)
		if err != nil {
			w.logger.Error().Err(err).Msg("polymarket market: parse new_market event")
			return
		}
		if w.onNewMarket != nil {
			w.onNewMarket(event)
		}

	case "market_resolved":
		event, err := ParseMarketResolvedEvent(data)
		if err != nil {
			w.logger.Error().Err(err).Msg("polymarket market: parse market_resolved event")
			return
		}
		if w.onMarketResolved != nil {
			w.onMarketResolved(event)
		}

	default:
		w.logger.Debug().Str("event_type", baseMsg.EventType).Msg("polymarket market: unknown event type")
	}
}

func (w *WSClient) sendSubscription(assetIDs []string) error {
	w.connMu.Lock()
	defer w.connMu.Unlock()

	if w.conn == nil || !w.connected.Load() {
		return nil
	}

	msg := map[string]interface{}{
		"assets_ids":             assetIDs,
		"type":                   "market",
		"custom_feature_enabled": w.config.CustomFeatureEnabled,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal subscription: %w", err)
	}

	w.conn.SetWriteDeadline(time.Now().Add(w.config.WriteWait))
	if err := w.conn.WriteMessage(websocket.TextMessage, data); err != nil {
		return fmt.Errorf("write subscription: %w", err)
	}

	w.logger.Debug().
		Int("asset_count", len(assetIDs)).
		Bool("custom_features", w.config.CustomFeatureEnabled).
		Msg("polymarket market: sent subscription")

	return nil
}

func (w *WSClient) pingLoop(ctx context.Context) {
	ticker := time.NewTicker(w.config.PingPeriod)
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
				w.conn.SetWriteDeadline(time.Now().Add(w.config.WriteWait))
				if err := w.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					w.logger.Debug().Err(err).Msg("polymarket market: ping failed")
				}
			}
			w.connMu.Unlock()
		}
	}
}

func (w *WSClient) handleReconnect(ctx context.Context) bool {
	backoff := w.config.ReconnectMinWait
	w.logger.Info().
		Dur("backoff", backoff).
		Msg("polymarket market: reconnecting after backoff")

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
		w.logger.Info().Msg("polymarket market: websocket client stopped")
	})
}

func (w *WSClient) IsConnected() bool {
	return w.connected.Load()
}

func (w *WSClient) Subscribe(assetID string) error {
	return w.subManager.Subscribe(assetID)
}

func (w *WSClient) Unsubscribe(assetID string) error {
	return w.subManager.Unsubscribe(assetID)
}

func (w *WSClient) SubscribeMany(assetIDs []string) error {
	return w.subManager.SubscribeMany(assetIDs)
}

func (w *WSClient) UnsubscribeMany(assetIDs []string) error {
	return w.subManager.UnsubscribeMany(assetIDs)
}

func (w *WSClient) GetCurrentSubscriptions() []string {
	return w.subManager.GetCurrentSubscriptions()
}
