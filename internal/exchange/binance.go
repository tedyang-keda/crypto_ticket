package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"crypto-ticket/internal/market"
)

type BinanceFuturesAdapter struct {
	marketType string
	restURL    string
	wsURL      string
}

func NewBinanceFuturesAdapter(marketType string, restURL string, wsURL string) *BinanceFuturesAdapter {
	return &BinanceFuturesAdapter{
		marketType: strings.TrimSpace(marketType),
		restURL:    strings.TrimRight(strings.TrimSpace(restURL), "/"),
		wsURL:      strings.TrimSpace(wsURL),
	}
}

func (a *BinanceFuturesAdapter) Name() string {
	return "binance"
}

func (a *BinanceFuturesAdapter) MarketType() string {
	return a.marketType
}

func (a *BinanceFuturesAdapter) RestURL() string {
	return a.restURL
}

func (a *BinanceFuturesAdapter) WSURL() string {
	return a.wsURL
}

func (a *BinanceFuturesAdapter) FetchSymbols(ctx context.Context, client *http.Client) ([]market.SymbolInfo, error) {
	path := "/api/v3/exchangeInfo"
	if a.marketType == "" || strings.EqualFold(a.marketType, "um_futures") {
		path = "/fapi/v1/exchangeInfo"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.restURL+path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("binance exchangeInfo status %s", resp.Status)
	}
	var payload struct {
		Symbols []struct {
			Symbol string `json:"symbol"`
			Status string `json:"status"`
		} `json:"symbols"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	now := market.NowMS()
	symbols := make([]market.SymbolInfo, 0, len(payload.Symbols))
	for _, item := range payload.Symbols {
		symbol := strings.ToUpper(strings.TrimSpace(item.Symbol))
		if symbol == "" {
			continue
		}
		status := strings.ToUpper(item.Status)
		symbols = append(symbols, market.SymbolInfo{
			Exchange:      a.Name(),
			Symbol:        symbol,
			MarketType:    a.marketType,
			Status:        status,
			IsActive:      status == "TRADING",
			FirstSeenAtMS: now,
			LastSeenAtMS:  now,
			UpdatedAtMS:   now,
		})
	}
	return symbols, nil
}

func (a *BinanceFuturesAdapter) BuildSubscribePayload(symbols []string, requestID int64) ([]byte, error) {
	return a.buildSubscriptionPayload("SUBSCRIBE", symbols, requestID)
}

func (a *BinanceFuturesAdapter) BuildUnsubscribePayload(symbols []string, requestID int64) ([]byte, error) {
	return a.buildSubscriptionPayload("UNSUBSCRIBE", symbols, requestID)
}

func (a *BinanceFuturesAdapter) buildSubscriptionPayload(method string, symbols []string, requestID int64) ([]byte, error) {
	params := make([]string, 0, len(symbols))
	for _, symbol := range symbols {
		symbol = strings.ToLower(strings.TrimSpace(symbol))
		if symbol == "" {
			continue
		}
		params = append(params, symbol+"@trade")
	}
	return json.Marshal(map[string]any{
		"method": method,
		"params": params,
		"id":     requestID,
	})
}

func (a *BinanceFuturesAdapter) ParseMessage(payload []byte) ([]market.Tick, error) {
	var data map[string]any
	if err := json.Unmarshal(payload, &data); err != nil {
		return nil, err
	}
	eventType := strings.ToLower(stringValue(data["e"]))
	if eventType != "trade" && eventType != "aggtrade" {
		return nil, nil
	}
	symbol := strings.ToUpper(stringValue(data["s"]))
	price := floatValue(data["p"])
	size := floatValue(data["q"])
	tsMS := intValue(data["T"])
	if tsMS == 0 {
		tsMS = intValue(data["E"])
	}
	if symbol == "" || price <= 0 || tsMS <= 0 {
		return nil, nil
	}
	side := "buy"
	if maker, ok := data["m"].(bool); ok && maker {
		side = "sell"
	}
	tick := market.Tick{
		Exchange:  a.Name(),
		Symbol:    symbol,
		TsMS:      tsMS,
		Price:     price,
		Size:      size,
		Side:      side,
		TradeID:   stringValue(firstNonEmpty(data["a"], data["t"])),
		EventType: eventType,
		Source:    "ws",
		RecvMS:    market.NowMS(),
		Raw:       append([]byte(nil), payload...),
	}
	return []market.Tick{tick}, nil
}
