package exchange

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"crypto-ticket/internal/market"
)

func TestBinanceContractInfoSourceParsesLifecycle(t *testing.T) {
	source := NewBinanceContractInfoSource("um_futures", "https://fapi.binance.com", "wss://fstream.binance.com")
	cases := []struct {
		status string
		phase  string
	}{
		{"TRADING_HALT", market.PhaseHalt},
		{"TRADING_CANCEL_ONLY", market.PhaseCancelOnly},
		{"TRADING", market.PhaseContinuous},
	}
	for _, tc := range cases {
		payload := []byte(`{"e":"contractInfo","E":1784074500000,"s":"KORUUSDT","ct":"TRADIFI_PERPETUAL","cs":"` + tc.status + `"}`)
		symbols, err := source.ParseInstrumentsMessage(payload)
		if err != nil || len(symbols) != 1 {
			t.Fatalf("status=%s parse err=%v symbols=%d", tc.status, err, len(symbols))
		}
		got := symbols[0]
		if got.AssetClass != market.AssetClassEquity || got.InstrumentType != "TRADIFI_PERPETUAL" || got.LifecyclePhase != tc.phase {
			t.Fatalf("status=%s unexpected classification: %+v", tc.status, got)
		}
	}
}

func TestBinanceContractInfoSourceParsesCombinedFrame(t *testing.T) {
	source := NewBinanceContractInfoSource("um_futures", "https://fapi.binance.com", "")
	symbols, err := source.ParseInstrumentsMessage([]byte(`{"stream":"!contractInfo","data":{"e":"contractInfo","E":1,"s":"KORUUSDT","ct":"TRADIFI_PERPETUAL","st":"TRADING"}}`))
	if err != nil || len(symbols) != 1 || !symbols[0].IsActive {
		t.Fatalf("unexpected combined parse err=%v symbols=%+v", err, symbols)
	}
	if payload, err := source.BuildInstrumentsSubscribePayload(); err != nil || len(payload) != 0 {
		t.Fatalf("direct stream must not send subscription payload: %q err=%v", payload, err)
	}
}

func TestBinanceContractInfoSourceMergesExchangeInfoMetadata(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"symbols":[{"symbol":"KORUUSDT","status":"TRADING","contractType":"TRADIFI_PERPETUAL","underlyingType":"EQUITY"}]}`))
	}))
	defer server.Close()
	source := NewBinanceContractInfoSource("um_futures", server.URL, "")
	if _, err := source.FetchSymbols(context.Background(), server.Client()); err != nil {
		t.Fatal(err)
	}
	symbols, err := source.ParseInstrumentsMessage([]byte(`{"e":"contractInfo","E":2,"s":"KORUUSDT","cs":"TRADING_HALT"}`))
	if err != nil || len(symbols) != 1 {
		t.Fatalf("sparse frame parse err=%v symbols=%+v", err, symbols)
	}
	if symbols[0].AssetClass != market.AssetClassEquity || symbols[0].InstrumentType != "TRADIFI_PERPETUAL" || symbols[0].LifecyclePhase != market.PhaseHalt {
		t.Fatalf("cached metadata was not merged: %+v", symbols[0])
	}
}
