package instrument

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"crypto-ticket/internal/market"
)

type fakeFetcher struct {
	batches [][]market.SymbolInfo
	idx     int
}

func (f *fakeFetcher) Name() string       { return "binance" }
func (f *fakeFetcher) MarketType() string { return "um_futures" }
func (f *fakeFetcher) FetchSymbols(_ context.Context, _ *http.Client) ([]market.SymbolInfo, error) {
	if f.idx >= len(f.batches) {
		return f.batches[len(f.batches)-1], nil
	}
	b := f.batches[f.idx]
	f.idx++
	return b, nil
}

type captureSink struct {
	events []market.InstrumentChangeEvent
}

func (c *captureSink) HandleInstrumentEvent(_ context.Context, event market.InstrumentChangeEvent) error {
	c.events = append(c.events, event)
	return nil
}

type countingStore struct {
	upserts int
	symbols int
}

func (c *countingStore) UpsertSymbols(_ context.Context, symbols []market.SymbolInfo) error {
	c.upserts++
	c.symbols += len(symbols)
	return nil
}

func equitySymbol(symbol string, phase string, rule string) market.SymbolInfo {
	return market.SymbolInfo{
		Exchange:       "okx",
		SourceMarket:   "okx:SWAP",
		Symbol:         symbol,
		MarketType:     "SWAP",
		AssetClass:     market.AssetClassEquity,
		LifecyclePhase: phase,
		RuleType:       rule,
	}
}

func newTestMonitor(sink EventSink, store SymbolStore) *Monitor {
	return New(nopSource{}, sink, store, Config{})
}

type fakeAliasLinker struct {
	calls       int
	successor   string
	predecessor string
}

func (f *fakeAliasLinker) Link(_ string, _ string, successor string, predecessor string, _ int64) {
	f.calls++
	f.successor = successor
	f.predecessor = predecessor
}

func equityFamilySymbol(symbol string, family string) market.SymbolInfo {
	s := equitySymbol(symbol, market.PhaseContinuous, market.RuleNormal)
	s.IsActive = true
	raw, _ := json.Marshal(map[string]any{"instId": symbol, "instFamily": family})
	s.Raw = raw
	return s
}

type nopSource struct{}

func (nopSource) Name() string                                      { return "okx" }
func (nopSource) MarketType() string                                { return "SWAP" }
func (nopSource) WSURL() string                                     { return "wss://example" }
func (nopSource) BuildInstrumentsSubscribePayload() ([]byte, error) { return []byte("{}"), nil }
func (nopSource) ParseInstrumentsMessage([]byte) ([]market.SymbolInfo, error) {
	return nil, nil
}

func TestMonitorFirstBatchIsBaseline(t *testing.T) {
	sink := &captureSink{}
	store := &countingStore{}
	monitor := newTestMonitor(sink, store)

	monitor.processBatch(context.Background(), []market.SymbolInfo{
		equitySymbol("MUU-USDT-SWAP", market.PhaseContinuous, market.RuleNormal),
		equitySymbol("KORU-USDT-SWAP", market.PhaseContinuous, market.RuleNormal),
	})

	if len(sink.events) != 0 {
		t.Fatalf("baseline batch should emit no events, got %d", len(sink.events))
	}
	if store.symbols != 2 {
		t.Fatalf("expected 2 symbols upserted, got %d", store.symbols)
	}
}

func TestMonitorEmitsRebaseOnTransition(t *testing.T) {
	sink := &captureSink{}
	monitor := newTestMonitor(sink, nil)
	ctx := context.Background()

	monitor.processBatch(ctx, []market.SymbolInfo{
		equitySymbol("MUU-USDT-SWAP", market.PhaseContinuous, market.RuleNormal),
	})
	monitor.processBatch(ctx, []market.SymbolInfo{
		equitySymbol("MUU-USDT-SWAP", market.PhaseRebase, market.RuleNormal),
	})

	if len(sink.events) != 1 {
		t.Fatalf("expected one rebase event, got %d", len(sink.events))
	}
	event := sink.events[0]
	if event.EventType != market.InstrumentEventRebase || event.Symbol != "MUU-USDT-SWAP" {
		t.Fatalf("unexpected event: %+v", event)
	}
	if event.PreviousHash == "" || event.CurrentHash == "" || event.PreviousHash == event.CurrentHash {
		t.Fatalf("expected distinct non-empty fingerprints: %+v", event)
	}
}

func TestMonitorIgnoresNonCorporateAndCryptoChurn(t *testing.T) {
	sink := &captureSink{}
	monitor := newTestMonitor(sink, nil)
	ctx := context.Background()

	// Crypto going through a rebase-like phase must never escalate.
	crypto := func(phase string) market.SymbolInfo {
		return market.SymbolInfo{
			Exchange:       "okx",
			Symbol:         "BTC-USDT-SWAP",
			MarketType:     "SWAP",
			AssetClass:     market.AssetClassCrypto,
			LifecyclePhase: phase,
		}
	}
	monitor.processBatch(ctx, []market.SymbolInfo{crypto(market.PhaseContinuous)})
	monitor.processBatch(ctx, []market.SymbolInfo{crypto(market.PhaseRebase)})

	if len(sink.events) != 0 {
		t.Fatalf("crypto churn should emit no events, got %d", len(sink.events))
	}
}

func TestMonitorDetectsRenameByFamily(t *testing.T) {
	sink := &captureSink{}
	alias := &fakeAliasLinker{}
	monitor := newTestMonitor(sink, nil)
	monitor.SetAliasLinker(alias)
	ctx := context.Background()

	// Baseline: family maps to its first live instId, no rename yet.
	monitor.processBatch(ctx, []market.SymbolInfo{equityFamilySymbol("OLDX-USDT-SWAP", "SHARED-USDT")})
	if alias.calls != 0 {
		t.Fatalf("baseline should not record a rename, calls=%d", alias.calls)
	}

	// The family's live instId changes -> rename.
	monitor.processBatch(ctx, []market.SymbolInfo{equityFamilySymbol("NEWX-USDT-SWAP", "SHARED-USDT")})
	if alias.calls != 1 || alias.successor != "NEWX-USDT-SWAP" || alias.predecessor != "OLDX-USDT-SWAP" {
		t.Fatalf("unexpected alias link: calls=%d %s<-%s", alias.calls, alias.successor, alias.predecessor)
	}
	found := false
	for _, e := range sink.events {
		if e.EventType == market.InstrumentEventRenamed && e.Symbol == "NEWX-USDT-SWAP" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected a rename event on the sink")
	}
}

func TestPollingMonitorDetectsBinanceSplitHalt(t *testing.T) {
	sink := &captureSink{}
	eq := func(status string, phase string) market.SymbolInfo {
		return market.SymbolInfo{
			Exchange: "binance", SourceMarket: "binance:um_futures", Symbol: "WENUSDT",
			MarketType: "um_futures", AssetClass: market.AssetClassEquity,
			LifecyclePhase: phase, Status: status, IsActive: status == "TRADING",
		}
	}
	fetcher := &fakeFetcher{batches: [][]market.SymbolInfo{
		{eq("TRADING", market.PhaseContinuous)}, // baseline
		{eq("SETTLING", market.PhaseSuspend)},   // split halt
	}}
	monitor := NewPolling(fetcher, sink, nil, Config{})
	ctx := context.Background()

	monitor.pollOnce(ctx) // baseline: no events
	if len(sink.events) != 0 {
		t.Fatalf("baseline poll should emit nothing, got %d", len(sink.events))
	}
	monitor.pollOnce(ctx) // halt: corporate-action candidate
	found := false
	for _, e := range sink.events {
		if e.EventType == market.InstrumentEventSuspended && e.Symbol == "WENUSDT" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected an equity_suspended event, got %+v", sink.events)
	}
}

func TestPollingMonitorRecoversInitialBinanceHalt(t *testing.T) {
	sink := &captureSink{}
	fetcher := &fakeFetcher{batches: [][]market.SymbolInfo{{{
		Exchange: "binance", SourceMarket: "binance:um_futures", Symbol: "KORUUSDT",
		MarketType: "um_futures", AssetClass: market.AssetClassEquity,
		LifecyclePhase: market.PhaseHalt, Status: "TRADING_HALT",
	}}}}
	monitor := NewPolling(fetcher, sink, nil, Config{EmitInitialCorporateState: true})
	monitor.pollOnce(context.Background())
	if len(sink.events) != 1 || sink.events[0].EventType != market.InstrumentEventSuspended {
		t.Fatalf("initial halt should recover a candidate, got %+v", sink.events)
	}
}

func TestMonitorNoEventWhenUnchanged(t *testing.T) {
	sink := &captureSink{}
	monitor := newTestMonitor(sink, nil)
	ctx := context.Background()

	batch := []market.SymbolInfo{equitySymbol("MUU-USDT-SWAP", market.PhaseContinuous, market.RuleNormal)}
	monitor.processBatch(ctx, batch)
	monitor.processBatch(ctx, batch)

	if len(sink.events) != 0 {
		t.Fatalf("repeated identical batch should emit no events, got %d", len(sink.events))
	}
}
