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
	Exchange       string  `json:"exchange"`
	Symbol         string  `json:"symbol"`
	MarginType     string  `json:"margin_type,omitempty"`
	Timeframe      string  `json:"timeframe"`
	StartMS        int64   `json:"start_ms"`
	EndMS          int64   `json:"end_ms"`
	StartTS        int64   `json:"startts,omitempty"`
	EndTS          int64   `json:"endts,omitempty"`
	OpenPrice      float64 `json:"open_price"`
	HighPrice      float64 `json:"high_price"`
	LowPrice       float64 `json:"low_price"`
	ClosePrice     float64 `json:"close_price"`
	Open           float64 `json:"open,omitempty"`
	High           float64 `json:"high,omitempty"`
	Low            float64 `json:"low,omitempty"`
	Close          float64 `json:"close,omitempty"`
	PrevClose      float64 `json:"prev_close,omitempty"`
	Chg            float64 `json:"chg,omitempty"`
	Amp            float64 `json:"amp,omitempty"`
	Volume         float64 `json:"volume"`
	VolumeUnit     string  `json:"volume_unit,omitempty"`
	QuoteVolume    float64 `json:"quote_volume"`
	Quote          float64 `json:"quote,omitempty"`
	QuoteUnit      string  `json:"quote_unit,omitempty"`
	ContractVolume float64 `json:"contract_volume,omitempty"`
	TradeCount     int64   `json:"trade_count"`
	LastTickMS     int64   `json:"last_tick_ms"`
	IsFinal        bool    `json:"is_final"`
	Source         string  `json:"source"`
	Reason         string  `json:"reason"`
	UpdatedAtMS    int64   `json:"updated_at_ms"`
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

func DecorateBar(bar Bar) Bar {
	bar.StartTS = bar.StartMS
	bar.EndTS = bar.EndMS
	bar.Open = bar.OpenPrice
	bar.High = bar.HighPrice
	bar.Low = bar.LowPrice
	bar.Close = bar.ClosePrice
	bar.Quote = bar.QuoteVolume
	return bar
}
