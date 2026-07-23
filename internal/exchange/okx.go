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
	baseCcy        string
	quoteCcy       string
	settleCcy      string
	instFamily     string
	ctVal          float64
	ctMult         float64
	ctValCcy       string
	classification market.InstrumentClassification
}

// okxInstrumentItem mirrors the fields OKX returns from both the REST
// GET /api/v5/public/instruments endpoint and the public "instruments"
// WebSocket channel. The two share an identical data shape, so the same
// decoding path serves symbol refresh and real-time corporate-action
// detection.
type okxInstrumentItem struct {
	InstID       string `json:"instId"`
	InstType     string `json:"instType"`
	InstFamily   string `json:"instFamily"`
	InstCategory string `json:"instCategory"`
	RuleType     string `json:"ruleType"`
	State        string `json:"state"`
	BaseCcy      string `json:"baseCcy"`
	QuoteCcy     string `json:"quoteCcy"`
	SettleCcy    string `json:"settleCcy"`
	CtVal        string `json:"ctVal"`
	CtMult       string `json:"ctMult"`
	CtValCcy     string `json:"ctValCcy"`
	ListTime     string `json:"listTime"`
	ExpTime      string `json:"expTime"`
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
		Data []okxInstrumentItem `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	now := market.NowMS()
	symbols := make([]market.SymbolInfo, 0, len(payload.Data))
	specs := make(map[string]okxInstrumentSpec, len(payload.Data))
	for _, item := range payload.Data {
		info, spec, symbol := a.buildInstrument(item, now)
		if symbol == "" {
			continue
		}
		specs[symbol] = spec
		symbols = append(symbols, info)
	}
	a.replaceInstrumentSpecs(specs)
	return symbols, nil
}

// buildInstrument maps a single OKX instrument record to a classified
// SymbolInfo plus the spec used for kline normalization. Shared by the REST
// symbol refresh and the WebSocket instruments channel so both stay in sync.
func (a *OKXAdapter) buildInstrument(item okxInstrumentItem, now int64) (market.SymbolInfo, okxInstrumentSpec, string) {
	symbol := strings.ToUpper(strings.TrimSpace(item.InstID))
	if symbol == "" {
		return market.SymbolInfo{}, okxInstrumentSpec{}, ""
	}
	baseCcy := strings.ToUpper(strings.TrimSpace(item.BaseCcy))
	quoteCcy := strings.ToUpper(strings.TrimSpace(item.QuoteCcy))
	if baseCcy == "" || quoteCcy == "" {
		baseCcy, quoteCcy = inferOKXSymbolCurrencies(symbol)
	}
	raw := map[string]any{
		"instId":       item.InstID,
		"instType":     item.InstType,
		"instFamily":   item.InstFamily,
		"instCategory": item.InstCategory,
		"ruleType":     item.RuleType,
		"state":        item.State,
		"baseCcy":      item.BaseCcy,
		"quoteCcy":     item.QuoteCcy,
		"settleCcy":    item.SettleCcy,
		"ctVal":        item.CtVal,
		"ctMult":       item.CtMult,
		"ctValCcy":     item.CtValCcy,
		"listTime":     item.ListTime,
		"expTime":      item.ExpTime,
	}
	classification := market.ClassifyOKXSymbol(a.instType, raw)
	spec := okxInstrumentSpec{
		baseCcy:        baseCcy,
		quoteCcy:       quoteCcy,
		settleCcy:      strings.ToUpper(strings.TrimSpace(item.SettleCcy)),
		instFamily:     strings.ToUpper(strings.TrimSpace(item.InstFamily)),
		ctVal:          parseFloat(item.CtVal),
		ctMult:         parseFloat(item.CtMult),
		ctValCcy:       strings.ToUpper(strings.TrimSpace(item.CtValCcy)),
		classification: classification,
	}
	status := strings.ToLower(item.State)
	info := market.SymbolInfo{
		Exchange:      a.Name(),
		Symbol:        symbol,
		MarketType:    a.instType,
		Status:        status,
		IsActive:      status == "live",
		FirstSeenAtMS: now,
		LastSeenAtMS:  now,
		UpdatedAtMS:   now,
		Raw:           rawJSON(raw),
	}
	return market.ApplyClassificationFieldsToSymbol(info, classification), spec, symbol
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
		args = append(args, map[string]string{"channel": "candle1m", "instId": symbol})
	}
	return json.Marshal(map[string]any{
		"op":   op,
		"args": args,
		"id":   requestID,
	})
}

// BuildInstrumentsSubscribePayload builds the subscription frame for the OKX
// public "instruments" channel. After subscribing, OKX pushes the full
// instrument list once, then pushes only the records whose state changes
// (listing, suspension, expiry, rebase) — the real-time signal used to detect
// corporate actions on equity/pre-market perpetuals.
func (a *OKXAdapter) BuildInstrumentsSubscribePayload() ([]byte, error) {
	return json.Marshal(map[string]any{
		"op": "subscribe",
		"args": []map[string]string{
			{"channel": "instruments", "instType": a.instType},
		},
	})
}

// ParseInstrumentsMessage decodes a push from the "instruments" channel into
// classified SymbolInfo records. Event acks (subscribe/error) and messages for
// other channels decode to a nil slice. It also refreshes the adapter's local
// instrument specs so kline normalization tracks live metadata changes.
func (a *OKXAdapter) ParseInstrumentsMessage(payload []byte) ([]market.SymbolInfo, error) {
	// OKX answers our keepalive "ping" with a plain-text "pong" and may send
	// other non-JSON control frames; treat anything that is not a JSON object
	// as a no-op rather than a decode error.
	if trimmed := strings.TrimSpace(string(payload)); trimmed == "" || trimmed[0] != '{' {
		return nil, nil
	}
	var data struct {
		Event string `json:"event"`
		Arg   struct {
			Channel string `json:"channel"`
		} `json:"arg"`
		Data []okxInstrumentItem `json:"data"`
	}
	if err := json.Unmarshal(payload, &data); err != nil {
		return nil, err
	}
	if data.Event != "" || strings.ToLower(strings.TrimSpace(data.Arg.Channel)) != "instruments" {
		return nil, nil
	}
	now := market.NowMS()
	symbols := make([]market.SymbolInfo, 0, len(data.Data))
	updated := make(map[string]okxInstrumentSpec, len(data.Data))
	for _, item := range data.Data {
		info, spec, symbol := a.buildInstrument(item, now)
		if symbol == "" {
			continue
		}
		updated[symbol] = spec
		symbols = append(symbols, info)
	}
	a.mergeInstrumentSpecs(updated)
	return symbols, nil
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

func (a *OKXAdapter) ParseKlineMessage(payload []byte) ([]market.Bar, error) {
	var data struct {
		Event string `json:"event"`
		Arg   struct {
			Channel string `json:"channel"`
			InstID  string `json:"instId"`
		} `json:"arg"`
		Data [][]string `json:"data"`
	}
	if err := json.Unmarshal(payload, &data); err != nil {
		return nil, err
	}
	channel := strings.ToLower(strings.TrimSpace(data.Arg.Channel))
	if data.Event != "" || !strings.HasPrefix(channel, "candle") {
		return nil, nil
	}
	symbol := strings.ToUpper(strings.TrimSpace(data.Arg.InstID))
	if symbol == "" {
		return nil, nil
	}
	base, quote := inferOKXSymbolCurrencies(symbol)
	spec, ok := a.instrumentSpec(symbol)
	if ok {
		if spec.baseCcy != "" {
			base = spec.baseCcy
		}
		if spec.quoteCcy != "" {
			quote = spec.quoteCcy
		}
	}
	marginType := okxMarginType(symbol, spec)
	now := market.NowMS()
	bars := make([]market.Bar, 0, len(data.Data))
	for _, row := range data.Data {
		if len(row) < 9 {
			continue
		}
		startMS := parseInt(row[0])
		open := parseFloat(row[1])
		high := parseFloat(row[2])
		low := parseFloat(row[3])
		closePrice := parseFloat(row[4])
		if startMS <= 0 || open <= 0 || high <= 0 || low <= 0 || closePrice <= 0 {
			continue
		}
		contractVolume := parseFloat(row[5])
		volumeBase := parseFloat(row[6])
		quoteVolume := parseFloat(row[7])
		volume := volumeBase
		volumeUnit := base
		quoteUnit := quote
		if marginType == "coinmargin" {
			volume = contractVolume
			volumeUnit = "contract"
			quoteVolume = volumeBase
			quoteUnit = base
		}
		isFinal := strings.TrimSpace(row[8]) == "1"
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
			EndMS:          startMS + 60_000 - 1,
			OpenPrice:      open,
			HighPrice:      high,
			LowPrice:       low,
			ClosePrice:     closePrice,
			Volume:         volume,
			VolumeUnit:     volumeUnit,
			QuoteVolume:    quoteVolume,
			QuoteUnit:      quoteUnit,
			ContractVolume: contractVolume,
			LastTickMS:     startMS + 60_000 - 1,
			IsFinal:        isFinal,
			Source:         "exchange_kline",
			Reason:         reason,
			UpdatedAtMS:    now,
		}
		if ok {
			bar = market.ApplyClassificationFieldsToBar(bar, spec.classification)
		} else {
			bar = market.ApplyClassificationFieldsToBar(bar, okxDefaultClassification(a.instType))
		}
		bars = append(bars, market.DecorateBar(bar))
	}
	return bars, nil
}

func (a *OKXAdapter) replaceInstrumentSpecs(specs map[string]okxInstrumentSpec) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.specs = specs
}

// mergeInstrumentSpecs applies incremental instrument updates from the
// WebSocket instruments channel without discarding specs for symbols absent
// from a partial push.
func (a *OKXAdapter) mergeInstrumentSpecs(specs map[string]okxInstrumentSpec) {
	if len(specs) == 0 {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.specs == nil {
		a.specs = make(map[string]okxInstrumentSpec, len(specs))
	}
	for symbol, spec := range specs {
		a.specs[symbol] = spec
	}
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

func okxMarginType(symbol string, spec okxInstrumentSpec) string {
	base, quote := inferOKXSymbolCurrencies(symbol)
	if spec.baseCcy != "" {
		base = spec.baseCcy
	}
	if spec.quoteCcy != "" {
		quote = spec.quoteCcy
	}
	settle := spec.settleCcy
	if settle == "" {
		parts := strings.Split(strings.ToUpper(strings.TrimSpace(symbol)), "-")
		if len(parts) >= 2 {
			settle = parts[1]
		}
	}
	if quote == "USDT" || quote == "USDC" || settle == "USDT" || settle == "USDC" {
		return "umargin"
	}
	if quote == "USD" || settle == base {
		return "coinmargin"
	}
	return "umargin"
}
