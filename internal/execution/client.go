package execution

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/rs/zerolog"
)

// Config holds execution engine connection parameters.
type Config struct {
	URL                string
	Timeout            time.Duration
	RedeemDelay        time.Duration
	RedeemPollInterval time.Duration
	RedeemTimeout      time.Duration
}

// Client communicates with the Node.js execution engine at localhost:3000.
type Client struct {
	cfg        Config
	httpClient *http.Client
	logger     *zerolog.Logger
}

func NewClient(cfg Config, logger *zerolog.Logger) *Client {
	return &Client{
		cfg: cfg,
		httpClient: &http.Client{
			Timeout: cfg.Timeout,
		},
		logger: logger,
	}
}

// PlaceMarketOrder posts a FOK market order to the execution engine.
func (c *Client) PlaceMarketOrder(ctx context.Context, requestID string, req MarketOrderRequest) (*OrderResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal order request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.URL+"/orders/market", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("x-request-id", requestID)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var orderResp OrderResponse
	if err := json.NewDecoder(resp.Body).Decode(&orderResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &orderResp, nil
}

// Redeem posts a redeem request to the execution engine.
func (c *Client) Redeem(ctx context.Context, requestID string, req RedeemRequest) (*RedeemResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal redeem request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.URL+"/redeem", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("x-request-id", requestID)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var redeemResp RedeemResponse
	if err := json.NewDecoder(resp.Body).Decode(&redeemResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &redeemResp, nil
}

// GetRedeemStatus polls GET /redeem/:transactionId. The endpoint returns an array; we take the first element.
func (c *Client) GetRedeemStatus(ctx context.Context, transactionID string) (*RedeemStatusResponse, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, c.cfg.URL+"/redeem/"+transactionID, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var statuses []RedeemStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&statuses); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	if len(statuses) == 0 {
		return nil, fmt.Errorf("empty redeem status array")
	}
	return &statuses[0], nil
}

// GetCollateralBalance fetches the COLLATERAL balance from the execution engine.
func (c *Client) GetCollateralBalance(ctx context.Context) (float64, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, c.cfg.URL+"/balance?assetType=COLLATERAL", nil)
	if err != nil {
		return 0, fmt.Errorf("create request: %w", err)
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return 0, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var balResp BalanceResponse
	if err := json.NewDecoder(resp.Body).Decode(&balResp); err != nil {
		return 0, fmt.Errorf("decode response: %w", err)
	}

	balance, err := strconv.ParseFloat(balResp.Balance, 64)
	if err != nil {
		return 0, fmt.Errorf("parse balance %q: %w", balResp.Balance, err)
	}
	return balance, nil
}

// RedeemAndWait submits a redeem and polls until terminal state or timeout.
func (c *Client) RedeemAndWait(ctx context.Context, requestID, conditionID string) error {
	redeemResp, err := c.Redeem(ctx, requestID, RedeemRequest{
		ConditionID: conditionID,
		NegRisk:     false,
	})
	if err != nil {
		return fmt.Errorf("submit redeem: %w", err)
	}

	c.logger.Info().
		Str("transaction_id", redeemResp.TransactionID).
		Str("request_id", requestID).
		Msg("execution: redeem submitted, polling for confirmation")

	deadline := time.After(c.cfg.RedeemTimeout)
	ticker := time.NewTicker(c.cfg.RedeemPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			return fmt.Errorf("redeem timed out after %v", c.cfg.RedeemTimeout)
		case <-ticker.C:
			status, err := c.GetRedeemStatus(ctx, redeemResp.TransactionID)
			if err != nil {
				c.logger.Warn().Err(err).Str("transaction_id", redeemResp.TransactionID).Msg("execution: redeem poll error")
				continue
			}

			c.logger.Debug().
				Str("transaction_id", redeemResp.TransactionID).
				Str("state", status.State).
				Msg("execution: redeem poll")

			switch status.State {
			case "STATE_CONFIRMED":
				return nil
			case "STATE_FAILED", "STATE_INVALID":
				return fmt.Errorf("redeem terminal failure: %s", status.State)
			}
		}
	}
}

// HealthCheck verifies the execution engine is reachable.
func (c *Client) HealthCheck(ctx context.Context) error {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, c.cfg.URL+"/health", nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("execution engine unreachable: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("execution engine unhealthy: status %d", resp.StatusCode)
	}
	return nil
}
