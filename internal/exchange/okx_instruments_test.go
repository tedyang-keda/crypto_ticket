package exchange

import (
	"encoding/json"
	"strings"
	"testing"

	"crypto-ticket/internal/market"
)

func TestOKXBuildInstrumentsSubscribePayload(t *testing.T) {
	adapter := NewOKXAdapter("SWAP", "https://www.okx.com", "wss://example")
	payload, err := adapter.BuildInstrumentsSubscribePayload()
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Op   string `json:"op"`
		Args []struct {
			Channel  string `json:"channel"`
			InstType string `json:"instType"`
		} `json:"args"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Op != "subscribe" || len(decoded.Args) != 1 {
		t.Fatalf("unexpected payload: %s", payload)
	}
	if decoded.Args[0].Channel != "instruments" || decoded.Args[0].InstType != "SWAP" {
		t.Fatalf("unexpected args: %s", payload)
	}
}

func TestOKXParseInstrumentsMessage(t *testing.T) {
	adapter := NewOKXAdapter("SWAP", "https://www.okx.com", "wss://example")
	message := `{"arg":{"channel":"instruments","instType":"SWAP"},"data":[` +
		`{"instId":"MUU-USDT-SWAP","instType":"SWAP","instFamily":"MUU-USDT","instCategory":"3","ruleType":"rebase_contract","state":"rebase","baseCcy":"","quoteCcy":"","settleCcy":"USDT","ctVal":"1","ctMult":"1","ctValCcy":"MUU","listTime":"1720000000000","expTime":""}` +
		`]}`
	symbols, err := adapter.ParseInstrumentsMessage([]byte(message))
	if err != nil {
		t.Fatal(err)
	}
	if len(symbols) != 1 {
		t.Fatalf("expected one symbol, got %d", len(symbols))
	}
	sym := symbols[0]
	if sym.Symbol != "MUU-USDT-SWAP" || sym.Exchange != "okx" {
		t.Fatalf("unexpected symbol identity: %+v", sym)
	}
	if sym.AssetClass != market.AssetClassEquity {
		t.Fatalf("expected equity asset class, got %q", sym.AssetClass)
	}
	if sym.LifecyclePhase != market.PhaseRebase {
		t.Fatalf("expected rebase phase, got %q", sym.LifecyclePhase)
	}
	if !strings.Contains(string(sym.Raw), "instFamily") {
		t.Fatalf("raw payload missing instFamily: %s", sym.Raw)
	}
}

func TestOKXParseInstrumentsMessageIgnoresNonInstrumentFrames(t *testing.T) {
	adapter := NewOKXAdapter("SWAP", "https://www.okx.com", "wss://example")
	for _, frame := range []string{
		`{"event":"subscribe","arg":{"channel":"instruments","instType":"SWAP"}}`,
		`{"arg":{"channel":"tickers","instType":"SWAP"},"data":[{"instId":"BTC-USDT-SWAP"}]}`,
		`pong`,
	} {
		symbols, err := adapter.ParseInstrumentsMessage([]byte(frame))
		if err != nil {
			t.Fatalf("frame %q errored: %v", frame, err)
		}
		if len(symbols) != 0 {
			t.Fatalf("frame %q should yield no symbols, got %d", frame, len(symbols))
		}
	}
}
