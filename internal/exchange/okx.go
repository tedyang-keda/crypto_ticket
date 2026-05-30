package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"crypto-ticket/internal/market"
)

type OKXAdapter struct {
	instType string
	restURL  string
	wsURL    string
	mu       sync.RWMutex
	specs    map[string]okxInstrumentSpec
}

type okxInstrumentSpec struct {
	baseCcy   string
	quoteCcy  string
	settleCcy string
	ctVal     float64
	ctValCcy  string
}

func NewOKXAdapter(instType string, restURL string, wsURL string) *OKXAdapter {
	return &OKXAdapter{
		instType: strings.ToUpper(strings.TrimSpace(instType)),
		restURL:  strings.TrimRight(strings.TrimSpace(restURL), "/"),
		wsURL:    strings.TrimSpace(wsURL),
		specs:    make(map[string]okxInstrumentSpec),
	}
}

func (a *OKXAdapter) Name() string {
	return "okx"
}

func (a *OKXAdapter) MarketType() string {
	return a.instType
}

func (a *OKXAdapter) RestURL() string {
	return a.restURL
}

func (a *OKXAdapter) WSURL() string {
	return a.wsURL
}

func (a *OKXAdapter) FetchSymbols(ctx context.Context, client *http.Client) ([]market.SymbolInfo, error) {
	endpoint, err := url.Parse(a.restURL + "/api/v5/public/instruments")
	if err != nil {
		return nil, err
	}
	query := endpoint.Query()
	query.Set("instType", a.instType)
	endpoint.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("okx instruments status %s", resp.Status)
	}
	var payload struct {
		Data []struct {
			InstID    string `json:"instId"`
			State     string `json:"state"`
			BaseCcy   string `json:"baseCcy"`
			QuoteCcy  string `json:"quoteCcy"`
			SettleCcy string `json:"settleCcy"`
			CtVal     string `json:"ctVal"`
			CtValCcy  string `json:"ctValCcy"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	now := market.NowMS()
	symbols := make([]market.SymbolInfo, 0, len(payload.Data))
	specs := make(map[string]okxInstrumentSpec, len(payload.Data))
	for _, item := range payload.Data {
		symbol := strings.ToUpper(strings.TrimSpace(item.InstID))
		if symbol == "" {
			continue
		}
		baseCcy := strings.ToUpper(strings.TrimSpace(item.BaseCcy))
		quoteCcy := strings.ToUpper(strings.TrimSpace(item.QuoteCcy))
		if baseCcy == "" || quoteCcy == "" {
			baseCcy, quoteCcy = inferOKXSymbolCurrencies(symbol)
		}
		specs[symbol] = okxInstrumentSpec{
			baseCcy:   baseCcy,
			quoteCcy:  quoteCcy,
			settleCcy: strings.ToUpper(strings.TrimSpace(item.SettleCcy)),
			ctVal:     parseFloat(item.CtVal),
			ctValCcy:  strings.ToUpper(strings.TrimSpace(item.CtValCcy)),
		}
		status := strings.ToLower(item.State)
		symbols = append(symbols, market.SymbolInfo{
			Exchange:      a.Name(),
			Symbol:        symbol,
			MarketType:    a.instType,
			Status:        status,
			IsActive:      status == "live",
			FirstSeenAtMS: now,
			LastSeenAtMS:  now,
			UpdatedAtMS:   now,
		})
	}
	a.replaceInstrumentSpecs(specs)
	return symbols, nil
}

func (a *OKXAdapter) BuildSubscribePayload(symbols []string, requestID int64) ([]byte, error) {
	return a.buildSubscriptionPayload("subscribe", symbols, requestID)
}

func (a *OKXAdapter) BuildUnsubscribePayload(symbols []string, requestID int64) ([]byte, error) {
	return a.buildSubscriptionPayload("unsubscribe", symbols, requestID)
}

func (a *OKXAdapter) buildSubscriptionPayload(op string, symbols []string, requestID int64) ([]byte, error) {
	args := make([]map[string]string, 0, len(symbols))
	for _, symbol := range symbols {
		symbol = strings.ToUpper(strings.TrimSpace(symbol))
		if symbol == "" {
			continue
		}
		args = append(args, map[string]string{"channel": "trades", "instId": symbol})
	}
	return json.Marshal(map[string]any{
		"op":   op,
		"args": args,
		"id":   requestID,
	})
}

func (a *OKXAdapter) ParseMessage(payload []byte) ([]market.Tick, error) {
	var data struct {
		Event string `json:"event"`
		Arg   struct {
			Channel string `json:"channel"`
		} `json:"arg"`
		Data []struct {
			InstID  string `json:"instId"`
			Price   string `json:"px"`
			Size    string `json:"sz"`
			Side    string `json:"side"`
			TradeID string `json:"tradeId"`
			TsMS    string `json:"ts"`
		} `json:"data"`
	}
	if err := json.Unmarshal(payload, &data); err != nil {
		return nil, err
	}
	if data.Event != "" || strings.ToLower(data.Arg.Channel) != "trades" {
		return nil, nil
	}
	ticks := make([]market.Tick, 0, len(data.Data))
	now := market.NowMS()
	for _, item := range data.Data {
		symbol := strings.ToUpper(strings.TrimSpace(item.InstID))
		price := parseFloat(item.Price)
		size := a.normalizeTradeSize(symbol, price, parseFloat(item.Size))
		tsMS := parseInt(item.TsMS)
		if symbol == "" || price <= 0 || tsMS <= 0 {
			continue
		}
		ticks = append(ticks, market.Tick{
			Exchange:  a.Name(),
			Symbol:    symbol,
			TsMS:      tsMS,
			Price:     price,
			Size:      size,
			Side:      item.Side,
			TradeID:   item.TradeID,
			EventType: "trades",
			Source:    "ws",
			RecvMS:    now,
			Raw:       append([]byte(nil), payload...),
		})
	}
	return ticks, nil
}

func (a *OKXAdapter) replaceInstrumentSpecs(specs map[string]okxInstrumentSpec) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.specs = specs
}

func (a *OKXAdapter) instrumentSpec(symbol string) (okxInstrumentSpec, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	spec, ok := a.specs[strings.ToUpper(strings.TrimSpace(symbol))]
	return spec, ok
}

func (a *OKXAdapter) normalizeTradeSize(symbol string, price float64, size float64) float64 {
	if size <= 0 {
		return size
	}
	spec, ok := a.instrumentSpec(symbol)
	if !ok || spec.ctVal <= 0 || spec.ctValCcy == "" {
		return size
	}
	baseCcy := spec.baseCcy
	quoteCcy := spec.quoteCcy
	if baseCcy == "" || quoteCcy == "" {
		baseCcy, quoteCcy = inferOKXSymbolCurrencies(symbol)
	}
	contractValue := size * spec.ctVal
	if spec.ctValCcy == baseCcy {
		return contractValue
	}
	if price > 0 && (spec.ctValCcy == quoteCcy || spec.ctValCcy == spec.settleCcy) {
		return contractValue / price
	}
	return size
}

func inferOKXSymbolCurrencies(symbol string) (string, string) {
	parts := strings.Split(strings.ToUpper(strings.TrimSpace(symbol)), "-")
	if len(parts) < 2 {
		return "", ""
	}
	return parts[0], parts[1]
}
