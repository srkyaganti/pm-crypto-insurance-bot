package gamma

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	baseURL    string
	httpClient *http.Client
}

func NewClient(config ClientConfig) *Client {
	httpClient := config.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{
			Timeout: 10 * time.Second,
		}
	}

	return &Client{
		baseURL:    config.BaseURL,
		httpClient: httpClient,
	}
}

func (c *Client) GetMarketBySlug(ctx context.Context, slug string) (*MarketData, error) {
	url := fmt.Sprintf("%s/markets/slug/%s", c.baseURL, slug)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("market not found: %s", slug)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var marketResp MarketResponse
	if err := json.NewDecoder(resp.Body).Decode(&marketResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	tokens, err := parseTokenIDs(marketResp.ClobTokenIDs)
	if err != nil {
		return nil, fmt.Errorf("parse token IDs: %w", err)
	}

	timestamp, err := extractTimestampFromSlug(marketResp.Slug)
	if err != nil {
		return nil, fmt.Errorf("extract timestamp from slug: %w", err)
	}

	return &MarketData{
		ID:          marketResp.ID,
		Question:    marketResp.Question,
		ConditionID: marketResp.ConditionID,
		Slug:        marketResp.Slug,
		QuestionID:  marketResp.QuestionID,
		Tokens:      *tokens,
		WindowStart: time.Unix(timestamp, 0),
	}, nil
}

func parseTokenIDs(clobTokenIDs string) (*TokenIDs, error) {
	var ids []string
	if err := json.Unmarshal([]byte(clobTokenIDs), &ids); err != nil {
		return nil, fmt.Errorf("unmarshal token IDs: %w", err)
	}

	if len(ids) < 2 {
		return nil, fmt.Errorf("expected at least 2 token IDs, got %d", len(ids))
	}

	return &TokenIDs{
		UpTokenID:   ids[0],
		DownTokenID: ids[1],
	}, nil
}

func extractTimestampFromSlug(slug string) (int64, error) {
	parts := strings.Split(slug, "-")
	if len(parts) < 3 {
		return 0, fmt.Errorf("invalid slug format: %s", slug)
	}

	timestampStr := parts[len(parts)-1]
	timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse timestamp: %w", err)
	}

	return timestamp, nil
}
