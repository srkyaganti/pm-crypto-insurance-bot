package execution

// MarketOrderRequest is sent to POST /orders/market on the execution engine.
type MarketOrderRequest struct {
	TokenID   string  `json:"tokenId"`
	Side      string  `json:"side"`
	Amount    float64 `json:"amount"`
	TickSize  string  `json:"tickSize"`
	NegRisk   bool    `json:"negRisk"`
	OrderType string  `json:"orderType"`
	Price     float64 `json:"price,omitempty"`
}

// OrderResponse is returned from POST /orders/market.
type OrderResponse struct {
	RequestID         string   `json:"requestId"`
	Success           bool     `json:"success"`
	ErrorMsg          string   `json:"errorMsg"`
	OrderID           string   `json:"orderID"`
	TakingAmount      string   `json:"takingAmount"`
	MakingAmount      string   `json:"makingAmount"`
	Status            string   `json:"status"`
	TransactionHashes []string `json:"transactionsHashes"`
}

// RedeemRequest is sent to POST /redeem.
type RedeemRequest struct {
	ConditionID string `json:"conditionId"`
	NegRisk     bool   `json:"negRisk"`
}

// RedeemResponse is returned from POST /redeem.
type RedeemResponse struct {
	RequestID       string `json:"requestId"`
	TransactionID   string `json:"transactionID"`
	TransactionHash string `json:"transactionHash"`
	State           string `json:"state"`
}

// RedeemStatusResponse is returned from GET /redeem/:transactionId.
type RedeemStatusResponse struct {
	TransactionID   string `json:"transactionID"`
	TransactionHash string `json:"transactionHash"`
	State           string `json:"state"`
}

// BalanceResponse is returned from GET /balance.
type BalanceResponse struct {
	Balance   string `json:"balance"`
	Allowance string `json:"allowance"`
}

// Trade represents a trade returned from GET /trades.
type Trade struct {
	Market  string `json:"market"`
	AssetID string `json:"asset_id"`
	Side    string `json:"side"`
	Size    string `json:"size"`
	Price   string `json:"price"`
	Status  string `json:"status"`
	Outcome string `json:"outcome"`
}
