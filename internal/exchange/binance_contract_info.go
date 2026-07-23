package exchange

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"

	"crypto-ticket/internal/market"
)

// BinanceContractInfoSource adapts Binance USD-M's all-market contractInfo
// stream to the instrument monitor. The stream is server-pushed and needs no
// subscribe frame.
type BinanceContractInfoSource struct {
	marketType string
	wsURL      string
	adapter    *BinanceFuturesAdapter
	mu         sync.RWMutex
	metadata   map[string]market.SymbolInfo
}

func NewBinanceContractInfoSource(marketType string, restURL string, wsURL string) *BinanceContractInfoSource {
	wsURL = strings.TrimRight(strings.TrimSpace(wsURL), "/")
	if wsURL == "" {
		wsURL = "wss://fstream.binance.com/market/ws/!contractInfo"
	} else if !strings.Contains(strings.ToLower(wsURL), "!contractinfo") {
		wsURL = strings.TrimSuffix(wsURL, "/ws")
		wsURL = strings.TrimSuffix(wsURL, "/stream")
		wsURL = strings.TrimSuffix(wsURL, "/market")
		wsURL += "/market/ws/!contractInfo"
	}
	marketType = strings.TrimSpace(marketType)
	return &BinanceContractInfoSource{
		marketType: marketType,
		wsURL:      wsURL,
		adapter:    NewBinanceFuturesAdapter(marketType, restURL, wsURL),
		metadata:   make(map[string]market.SymbolInfo),
	}
}

func (s *BinanceContractInfoSource) Name() string                  { return "binance" }
func (s *BinanceContractInfoSource) MarketType() string            { return s.marketType }
func (s *BinanceContractInfoSource) WSURL() string                 { return s.wsURL }
func (s *BinanceContractInfoSource) UseWebSocketControlPing() bool { return true }

func (s *BinanceContractInfoSource) BuildInstrumentsSubscribePayload() ([]byte, error) {
	return nil, nil
}

// FetchSymbols bootstraps and reconciles full metadata through exchangeInfo.
// It also populates the cache used to classify sparse contractInfo frames.
func (s *BinanceContractInfoSource) FetchSymbols(ctx context.Context, client *http.Client) ([]market.SymbolInfo, error) {
	symbols, err := s.adapter.FetchSymbols(ctx, client)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	for _, symbol := range symbols {
		s.metadata[strings.ToUpper(symbol.Symbol)] = symbol
	}
	s.mu.Unlock()
	return symbols, nil
}

func (s *BinanceContractInfoSource) ParseInstrumentsMessage(payload []byte) ([]market.SymbolInfo, error) {
	var root map[string]any
	if err := json.Unmarshal(payload, &root); err != nil {
		return nil, err
	}
	data := root
	if nested, ok := root["data"].(map[string]any); ok {
		data = nested
	}
	if event := strings.ToLower(strings.TrimSpace(stringValue(data["e"]))); event != "contractinfo" {
		return nil, nil
	}
	symbol := strings.ToUpper(strings.TrimSpace(stringValue(data["s"])))
	if symbol == "" {
		return nil, nil
	}
	status := strings.ToUpper(strings.TrimSpace(stringValue(firstNonEmpty(data["st"], data["cs"], data["status"]))))
	contractType := strings.ToUpper(strings.TrimSpace(stringValue(firstNonEmpty(data["ct"], data["contractType"]))))
	raw := make(map[string]any, len(data)+8)
	s.mu.RLock()
	cached, cachedOK := s.metadata[symbol]
	s.mu.RUnlock()
	if cachedOK && len(cached.Raw) > 0 {
		_ = json.Unmarshal(cached.Raw, &raw)
	}
	for key, value := range data {
		raw[key] = value
	}
	if contractType == "" && cachedOK {
		contractType = cached.InstrumentType
	}
	raw["symbol"] = symbol
	raw["status"] = status
	raw["contractType"] = contractType
	classification := market.ClassifyBinanceSymbol(s.marketType, raw)
	nowMS := market.NowMS()
	if eventMS := intValue(data["E"]); eventMS > 0 {
		nowMS = eventMS
	}
	info := market.SymbolInfo{
		Exchange: "binance", Symbol: symbol, MarketType: s.marketType,
		Status: status, IsActive: status == "TRADING",
		FirstSeenAtMS: nowMS, LastSeenAtMS: nowMS, UpdatedAtMS: nowMS,
		Raw: rawJSON(raw),
	}
	if cachedOK && cached.FirstSeenAtMS > 0 {
		info.FirstSeenAtMS = cached.FirstSeenAtMS
	}
	if classification.AssetClass == market.AssetClassUnknown && contractType != "TRADIFI_PERPETUAL" {
		return nil, nil
	}
	return []market.SymbolInfo{market.ApplyClassificationFieldsToSymbol(info, classification)}, nil
}
