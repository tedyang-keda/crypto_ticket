package guardian

import (
	"context"
	"net/http"
	"testing"

	"crypto-ticket/internal/exchange"
	"crypto-ticket/internal/market"
	"crypto-ticket/internal/storage"
)

func TestHandleFinalBarDetectsWatermarkGapAndRepairs(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryHistoricalStore()
	base := int64(1_710_000_000_000)
	upsertActiveSymbol(t, store)
	if err := store.UpsertBars(ctx, []market.Bar{testBar(base, 100, 105, 99, 101, 1)}); err != nil {
		t.Fatalf("upsert baseline: %v", err)
	}

	fetcher := &fakeFetcher{bars: []market.Bar{
		testBar(base+60_000, 101, 106, 100, 102, 2),
		testBar(base+120_000, 102, 107, 101, 103, 3),
	}}
	repairer := &storeRepairer{store: store}
	g := New(store, repairer, []Fetcher{fetcher}, Config{Enabled: true, AuditWindow: 10 * 60_000_000_000})

	current := testBar(base+180_000, 103, 108, 102, 104, 4)
	if err := g.handleFinalBar(ctx, current); err != nil {
		t.Fatalf("handle final: %v", err)
	}
	if len(repairer.bars) != 2 {
		t.Fatalf("expected two repaired bars, got %+v", repairer.bars)
	}
	if repairer.bars[0].StartMS != base+60_000 || repairer.bars[1].StartMS != base+120_000 {
		t.Fatalf("unexpected repair range: %+v", repairer.bars)
	}
	state, err := store.LoadKlineGuardianState(ctx, "binance", "BTCUSDT", "1m")
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if state == nil ||
		state.Status != "gap_repaired" ||
		state.LastFinalStartMS != current.StartMS ||
		state.LastGapStartMS != base+60_000 ||
		state.LastGapEndMS != base+120_000 ||
		state.LastCheckedStartMS != base+60_000 ||
		state.LastCheckedEndMS != base+120_000 {
		t.Fatalf("unexpected state: %+v", state)
	}
}

func TestRepairRangeRepairsMissingAndMismatchedBars(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryHistoricalStore()
	base := int64(1_710_000_000_000)
	upsertActiveSymbol(t, store)
	if err := store.UpsertBars(ctx, []market.Bar{testBar(base, 100, 105, 99, 101, 1)}); err != nil {
		t.Fatalf("upsert local: %v", err)
	}

	fetcher := &fakeFetcher{bars: []market.Bar{
		testBar(base, 100, 110, 99, 104, 5),
		testBar(base+60_000, 104, 111, 103, 108, 6),
	}}
	repairer := &storeRepairer{store: store}
	g := New(store, repairer, []Fetcher{fetcher}, Config{Enabled: true})

	result, err := g.repairRangeWithFetcher(ctx, fetcher, "binance", "BTCUSDT", base, base+60_000, "window_audit")
	if err != nil {
		t.Fatalf("repair range: %v", err)
	}
	if result.Checked != 2 || result.Repaired != 2 {
		t.Fatalf("unexpected repair result: %+v", result)
	}
	bars, err := store.BarsInRange(ctx, "binance", "BTCUSDT", "1m", base, base+60_000)
	if err != nil {
		t.Fatalf("bars in range: %v", err)
	}
	if len(bars) != 2 || bars[0].ClosePrice != 104 || bars[1].ClosePrice != 108 {
		t.Fatalf("expected repaired store bars, got %+v", bars)
	}
	state, err := store.LoadKlineGuardianState(ctx, "binance", "BTCUSDT", "1m")
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if state == nil || state.Status != "repaired" || state.LastCheckedStartMS != base || state.LastCheckedEndMS != base+60_000 {
		t.Fatalf("unexpected state: %+v", state)
	}
}

type fakeFetcher struct {
	bars []market.Bar
}

func (f *fakeFetcher) Name() string {
	return "binance"
}

func (f *fakeFetcher) MarketType() string {
	return "um_futures"
}

func (f *fakeFetcher) FetchKlines(_ context.Context, _ *http.Client, request exchange.KlineRequest) ([]market.Bar, error) {
	var out []market.Bar
	for _, bar := range f.bars {
		if bar.Symbol != request.Symbol || bar.Timeframe != request.Timeframe {
			continue
		}
		if request.StartMS > 0 && bar.StartMS < request.StartMS {
			continue
		}
		if request.EndMS > 0 && bar.StartMS > request.EndMS {
			continue
		}
		out = append(out, bar)
	}
	return out, nil
}

type storeRepairer struct {
	store *storage.MemoryHistoricalStore
	bars  []market.Bar
}

func (r *storeRepairer) RepairFinalBars(ctx context.Context, bars []market.Bar) error {
	r.bars = append(r.bars, bars...)
	return r.store.UpsertBars(ctx, bars)
}

func upsertActiveSymbol(t *testing.T, store *storage.MemoryHistoricalStore) {
	t.Helper()
	if err := store.UpsertSymbols(context.Background(), []market.SymbolInfo{{
		Exchange: "binance", Symbol: "BTCUSDT", MarketType: "um_futures", Status: "TRADING", IsActive: true,
	}}); err != nil {
		t.Fatalf("upsert symbol: %v", err)
	}
}

func testBar(startMS int64, open float64, high float64, low float64, closePrice float64, volume float64) market.Bar {
	return market.DecorateBar(market.Bar{
		Exchange: "binance", Symbol: "BTCUSDT", MarginType: "umargin", Timeframe: "1m",
		StartMS: startMS, EndMS: startMS + 59_999,
		OpenPrice: open, HighPrice: high, LowPrice: low, ClosePrice: closePrice,
		Volume: volume, QuoteVolume: volume * closePrice, TradeCount: int64(volume),
		LastTickMS: startMS + 59_999, IsFinal: true, Source: "rest", Reason: "test",
	})
}
