package chainlink

import (
	"context"
	"fmt"

	"github.com/rs/zerolog"
	streams "github.com/smartcontractkit/data-streams-sdk/go"
	"github.com/smartcontractkit/data-streams-sdk/go/feed"
)

type Client struct {
	config    *Config
	logger    *zerolog.Logger
	sdkClient streams.Client
	stream    streams.Stream
	feedIDs   []feed.ID
	feedIDMap map[string]string // feedID string -> symbol
}

func NewClient(cfg *Config, logger *zerolog.Logger) (*Client, error) {
	feedIDs := make([]feed.ID, 0, len(cfg.FeedIDs))
	feedIDMap := make(map[string]string, len(cfg.FeedIDs))

	for symbol, hexID := range cfg.FeedIDs {
		var fid feed.ID
		if err := fid.FromString(hexID); err != nil {
			return nil, fmt.Errorf("invalid feed ID for %s: %w", symbol, err)
		}
		feedIDs = append(feedIDs, fid)
		feedIDMap[fid.String()] = symbol
	}

	return &Client{
		config:    cfg,
		logger:    logger,
		feedIDs:   feedIDs,
		feedIDMap: feedIDMap,
	}, nil
}

func (c *Client) Initialize() error {
	sdkConfig := streams.Config{
		ApiKey:         c.config.ApiKey,
		ApiSecret:      c.config.ApiSecret,
		RestURL:        c.config.RestURL,
		WsURL:          c.config.WsURL,
		WsHA:           c.config.WsHA,
		WsMaxReconnect: c.config.WsMaxReconnect,
		Logger:         c.logAdapter(),
	}

	client, err := streams.New(sdkConfig)
	if err != nil {
		return fmt.Errorf("failed to create SDK client: %w", err)
	}

	c.sdkClient = client
	c.logger.Info().
		Str("rest_url", c.config.RestURL).
		Str("ws_url", c.config.WsURL).
		Bool("ha_mode", c.config.WsHA).
		Int("feed_count", len(c.feedIDs)).
		Msg("chainlink: SDK client initialized")

	return nil
}

func (c *Client) logAdapter() func(format string, a ...any) {
	return func(format string, a ...any) {
		c.logger.Debug().Msgf(format, a...)
	}
}

func (c *Client) GetLatestReport(ctx context.Context, feedID feed.ID) (*streams.ReportResponse, error) {
	if c.sdkClient == nil {
		return nil, fmt.Errorf("client not initialized")
	}

	report, err := c.sdkClient.GetLatestReport(ctx, feedID)
	if err != nil {
		return nil, fmt.Errorf("get latest report: %w", err)
	}

	return report, nil
}

func (c *Client) Stream(ctx context.Context, statusCallback func(isConnected bool)) error {
	if c.sdkClient == nil {
		return fmt.Errorf("client not initialized")
	}

	connStatusCallback := func(isConnected bool, host string, origin string) {
		c.logger.Info().
			Bool("connected", isConnected).
			Str("host", host).
			Msg("chainlink: connection status changed")

		if statusCallback != nil {
			statusCallback(isConnected)
		}
	}

	stream, err := c.sdkClient.StreamWithStatusCallback(ctx, c.feedIDs, connStatusCallback)
	if err != nil {
		return fmt.Errorf("create stream: %w", err)
	}

	c.stream = stream
	c.logger.Info().
		Int("feed_count", len(c.feedIDs)).
		Msg("chainlink: stream started for all feeds")

	return nil
}

// SymbolForFeedID looks up the canonical symbol for a decoded feed ID string.
func (c *Client) SymbolForFeedID(feedIDStr string) string {
	return c.feedIDMap[feedIDStr]
}

func (c *Client) Read(ctx context.Context) (*streams.ReportResponse, error) {
	if c.stream == nil {
		return nil, fmt.Errorf("stream not initialized")
	}

	report, err := c.stream.Read(ctx)
	if err != nil {
		return nil, err
	}

	return report, nil
}

func (c *Client) Close() error {
	if c.stream != nil {
		if err := c.stream.Close(); err != nil {
			c.logger.Error().Err(err).Msg("chainlink: error closing stream")
			return err
		}
		c.stream = nil
	}
	c.logger.Info().Msg("chainlink: stream closed")
	return nil
}

func (c *Client) Stats() streams.Stats {
	if c.stream == nil {
		return streams.Stats{}
	}
	return c.stream.Stats()
}

func (c *Client) GetReportAtTimestamp(ctx context.Context, feedID feed.ID, timestamp uint64) (*streams.ReportResponse, error) {
	if c.sdkClient == nil {
		return nil, fmt.Errorf("client not initialized")
	}

	reports, err := c.sdkClient.GetReports(ctx, []feed.ID{feedID}, timestamp)
	if err != nil {
		return nil, fmt.Errorf("get report at timestamp %d: %w", timestamp, err)
	}

	if len(reports) == 0 {
		return nil, fmt.Errorf("no report available for timestamp %d", timestamp)
	}

	return reports[0], nil
}

func (c *Client) GetReportAtTimestampForFeed(ctx context.Context, feedIDHex string, timestamp uint64) (*streams.ReportResponse, error) {
	if c.sdkClient == nil {
		return nil, fmt.Errorf("client not initialized")
	}

	var fid feed.ID
	if err := fid.FromString(feedIDHex); err != nil {
		return nil, fmt.Errorf("invalid feed ID: %w", err)
	}

	reports, err := c.sdkClient.GetReports(ctx, []feed.ID{fid}, timestamp)
	if err != nil {
		return nil, fmt.Errorf("get report at timestamp %d: %w", timestamp, err)
	}

	if len(reports) == 0 {
		return nil, fmt.Errorf("no report available for timestamp %d", timestamp)
	}

	return reports[0], nil
}
