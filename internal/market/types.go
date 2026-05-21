package market

import (
	"encoding/json"
	"time"
)

type Tick struct {
	Exchange  string          `json:"exchange"`
	Symbol    string          `json:"symbol"`
	TsMS      int64           `json:"ts_ms"`
	Price     float64         `json:"price"`
	Size      float64         `json:"size"`
	Side      string          `json:"side,omitempty"`
	TradeID   string          `json:"trade_id,omitempty"`
	EventType string          `json:"event_type"`
	Source    string          `json:"source"`
	RecvMS    int64           `json:"recv_ms,omitempty"`
	Raw       json.RawMessage `json:"raw,omitempty"`
}

type Bar struct {
	Exchange    string  `json:"exchange"`
	Symbol      string  `json:"symbol"`
	Timeframe   string  `json:"timeframe"`
	StartMS     int64   `json:"start_ms"`
	EndMS       int64   `json:"end_ms"`
	OpenPrice   float64 `json:"open_price"`
	HighPrice   float64 `json:"high_price"`
	LowPrice    float64 `json:"low_price"`
	ClosePrice  float64 `json:"close_price"`
	Volume      float64 `json:"volume"`
	QuoteVolume float64 `json:"quote_volume"`
	TradeCount  int64   `json:"trade_count"`
	LastTickMS  int64   `json:"last_tick_ms"`
	IsFinal     bool    `json:"is_final"`
	Source      string  `json:"source"`
	Reason      string  `json:"reason"`
	UpdatedAtMS int64   `json:"updated_at_ms"`
}

type SymbolInfo struct {
	Exchange      string `json:"exchange"`
	Symbol        string `json:"symbol"`
	MarketType    string `json:"market_type"`
	Status        string `json:"status"`
	IsActive      bool   `json:"is_active"`
	FirstSeenAtMS int64  `json:"first_seen_at_ms"`
	LastSeenAtMS  int64  `json:"last_seen_at_ms"`
	UpdatedAtMS   int64  `json:"updated_at_ms"`
}

type KlineQuery struct {
	Exchange    string
	Symbol      string
	Timeframe   string
	Limit       int
	IncludeLive bool
}

type Event struct {
	Type      string `json:"type"`
	Seq       int64  `json:"seq"`
	Exchange  string `json:"exchange"`
	Symbol    string `json:"symbol"`
	Timeframe string `json:"timeframe,omitempty"`
	Tick      *Tick  `json:"tick,omitempty"`
	Bar       *Bar   `json:"bar,omitempty"`
}

func NowMS() int64 {
	return time.Now().UnixMilli()
}
