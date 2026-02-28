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
	feedID    feed.ID
}

func NewClient(cfg *Config, logger *zerolog.Logger) (*Client, error) {
	var feedID feed.ID
	if err := feedID.FromString(cfg.FeedID); err != nil {
		return nil, fmt.Errorf("invalid feed ID: %w", err)
	}

	return &Client{
		config: cfg,
		logger: logger,
		feedID: feedID,
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
		Msg("chainlink: SDK client initialized")

	return nil
}

func (c *Client) logAdapter() func(format string, a ...any) {
	return func(format string, a ...any) {
		c.logger.Debug().Msgf(format, a...)
	}
}

func (c *Client) GetLatestReport(ctx context.Context) (*streams.ReportResponse, error) {
	if c.sdkClient == nil {
		return nil, fmt.Errorf("client not initialized")
	}

	report, err := c.sdkClient.GetLatestReport(ctx, c.feedID)
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

	stream, err := c.sdkClient.StreamWithStatusCallback(ctx, []feed.ID{c.feedID}, connStatusCallback)
	if err != nil {
		return fmt.Errorf("create stream: %w", err)
	}

	c.stream = stream
	c.logger.Info().Str("feed_id", c.feedID.String()).Msg("chainlink: stream started")

	return nil
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
