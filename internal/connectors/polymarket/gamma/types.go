package gamma

import (
	"net/http"
	"time"
)

type MarketResponse struct {
	ID           string `json:"id"`
	Question     string `json:"question"`
	ConditionID  string `json:"conditionId"`
	Slug         string `json:"slug"`
	QuestionID   string `json:"questionID"`
	ClobTokenIDs string `json:"clobTokenIds"`
}

type TokenIDs struct {
	UpTokenID   string
	DownTokenID string
}

type MarketData struct {
	ID          string
	Question    string
	ConditionID string
	Slug        string
	QuestionID  string
	Tokens      TokenIDs
	WindowStart time.Time
}

type ClientConfig struct {
	BaseURL    string
	HTTPClient *http.Client
}
