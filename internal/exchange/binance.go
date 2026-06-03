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
	} else if a.marginType() == "coinmargin" {
		path = "/dapi/v1/exchangeInfo"
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
			Symbol         string `json:"symbol"`
			Status         string `json:"status"`
			ContractStatus string `json:"contractStatus"`
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
		status := strings.ToUpper(stringValue(firstNonEmpty(item.Status, item.ContractStatus)))
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

func (a *BinanceFuturesAdapter) StaticStreamURL(symbols []string) string {
	streams := make([]string, 0, len(symbols))
	for _, symbol := range symbols {
		symbol = strings.ToLower(strings.TrimSpace(symbol))
		if symbol != "" {
			streams = append(streams, symbol+"@kline_1m")
		}
	}
	base := a.staticKlineStreamBaseURL()
	return base + "?streams=" + strings.Join(streams, "/")
}

func (a *BinanceFuturesAdapter) staticKlineStreamBaseURL() string {
	base := strings.TrimRight(strings.TrimSpace(a.wsURL), "/")
	if base == "" {
		if a.marginType() == "coinmargin" {
			base = "wss://dstream.binance.com/ws"
		} else {
			base = "wss://fstream.binance.com/market"
		}
	}
	if a.marginType() == "umargin" {
		base = strings.TrimSuffix(base, "/ws")
		base = strings.TrimSuffix(base, "/stream")
		base = strings.TrimSuffix(base, "/public")
		base = strings.TrimSuffix(base, "/public/ws")
		base = strings.TrimSuffix(base, "/public/stream")
		if !strings.HasSuffix(base, "/market") {
			base += "/market"
		}
		return base + "/stream"
	}
	if strings.HasSuffix(base, "/ws") {
		return strings.TrimSuffix(base, "/ws") + "/stream"
	}
	if strings.HasSuffix(base, "/stream") {
		return base
	}
	return base + "/stream"
}

func (a *BinanceFuturesAdapter) buildSubscriptionPayload(method string, symbols []string, requestID int64) ([]byte, error) {
	params := make([]string, 0, len(symbols))
	for _, symbol := range symbols {
		symbol = strings.ToLower(strings.TrimSpace(symbol))
		if symbol == "" {
			continue
		}
		params = append(params, symbol+"@kline_1m")
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

func (a *BinanceFuturesAdapter) ParseKlineMessage(payload []byte) ([]market.Bar, error) {
	var root map[string]any
	if err := json.Unmarshal(payload, &root); err != nil {
		return nil, err
	}
	if code := stringValue(root["code"]); code != "" {
		return nil, fmt.Errorf("binance websocket error code=%s msg=%s", code, stringValue(root["msg"]))
	}
	data := root
	if nested, ok := root["data"].(map[string]any); ok {
		data = nested
	}
	if strings.ToLower(stringValue(data["e"])) != "kline" {
		return nil, nil
	}
	kline, ok := data["k"].(map[string]any)
	if !ok {
		return nil, nil
	}
	symbol := strings.ToUpper(stringValue(firstNonEmpty(kline["s"], data["s"])))
	startMS := intValue(kline["t"])
	endMS := intValue(kline["T"])
	open := floatValue(kline["o"])
	high := floatValue(kline["h"])
	low := floatValue(kline["l"])
	closePrice := floatValue(kline["c"])
	if symbol == "" || startMS <= 0 || endMS <= 0 || open <= 0 || high <= 0 || low <= 0 || closePrice <= 0 {
		return nil, nil
	}
	marginType := a.marginType()
	base, quote := binanceSymbolCurrencies(symbol, marginType)
	volume := floatValue(kline["v"])
	quoteVolume := floatValue(kline["q"])
	volumeUnit := base
	quoteUnit := quote
	contractVolume := float64(0)
	if marginType == "coinmargin" {
		contractVolume = volume
		volumeUnit = "contract"
		quoteUnit = base
	}
	isFinal := false
	if closed, ok := kline["x"].(bool); ok {
		isFinal = closed
	}
	now := market.NowMS()
	reason := "update"
	if isFinal {
		reason = "final"
	}
	bar := market.Bar{
		Exchange:       a.Name(),
		Symbol:         symbol,
		MarginType:     marginType,
		Timeframe:      "1m",
		StartMS:        startMS,
		EndMS:          endMS,
		OpenPrice:      open,
		HighPrice:      high,
		LowPrice:       low,
		ClosePrice:     closePrice,
		Volume:         volume,
		VolumeUnit:     volumeUnit,
		QuoteVolume:    quoteVolume,
		QuoteUnit:      quoteUnit,
		ContractVolume: contractVolume,
		TradeCount:     intValue(kline["n"]),
		LastTickMS:     intValue(firstNonEmpty(data["E"], endMS)),
		IsFinal:        isFinal,
		Source:         "exchange_kline",
		Reason:         reason,
		UpdatedAtMS:    now,
	}
	return []market.Bar{market.DecorateBar(bar)}, nil
}

func (a *BinanceFuturesAdapter) marginType() string {
	value := strings.ToLower(strings.TrimSpace(a.marketType))
	if value == "coin_futures" || value == "coin_margined" || value == "coinmargin" || value == "coin-m" || value == "cm_futures" {
		return "coinmargin"
	}
	return "umargin"
}

func binanceSymbolCurrencies(symbol string, marginType string) (string, string) {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	if marginType == "coinmargin" {
		if index := strings.Index(symbol, "USD"); index > 0 {
			return symbol[:index], "USD"
		}
		return symbol, "USD"
	}
	for _, quote := range []string{"USDT", "USDC", "BUSD", "FDUSD", "USD"} {
		if strings.HasSuffix(symbol, quote) && len(symbol) > len(quote) {
			return strings.TrimSuffix(symbol, quote), quote
		}
	}
	return symbol, ""
}
