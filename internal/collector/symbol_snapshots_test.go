package collector

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"crypto-ticket/internal/market"
	"crypto-ticket/internal/storage"
)

type snapshotTestFetcher struct {
	items []market.SymbolInfo
	err   error
}

func (f *snapshotTestFetcher) Name() string       { return "binance" }
func (f *snapshotTestFetcher) MarketType() string { return "coin_futures" }
func (f *snapshotTestFetcher) FetchSymbols(context.Context, *http.Client) ([]market.SymbolInfo, error) {
	return f.items, f.err
}

func TestSymbolSnapshotWorkerCoversDisabledMarketSeedsGhostsAndPreservesFailure(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryHistoricalStore()
	if err := store.UpsertSymbols(ctx, []market.SymbolInfo{{Exchange: "binance", Symbol: "GHOSTUSD_PERP", MarketType: "coin_futures", IsActive: true, Status: "TRADING", FirstSeenAtMS: 123}}); err != nil {
		t.Fatal(err)
	}
	fetcher := &snapshotTestFetcher{items: []market.SymbolInfo{{Exchange: "binance", Symbol: "BTCUSD_PERP", MarketType: "coin_futures", Status: "TRADING", IsActive: true, OfficialActive: true, ListedAtMS: 50}}}
	source := SymbolSnapshotSource{Fetcher: fetcher, SubscriptionEnabled: false}
	worker := NewSymbolSnapshotWorker([]SymbolSnapshotSource{source}, store, time.Minute)
	if err := worker.RefreshOnce(ctx, source); err != nil {
		t.Fatal(err)
	}
	items, _ := store.ListSymbolSnapshots(ctx, "binance")
	if len(items) != 2 {
		t.Fatalf("missing registry ghost: %+v", items)
	}
	for _, item := range items {
		if item.Subscribed {
			t.Fatal("disabled collection market must never be selected")
		}
		if item.Symbol == "BTCUSD_PERP" && (!item.OfficialActive || item.Presence != "present" || item.SubscriptionReason != "market_disabled") {
			t.Fatalf("bad official-live disabled symbol: %+v", item)
		}
		if item.Symbol == "GHOSTUSD_PERP" && (item.Delisted || item.Presence != "absent" || item.ListedAtMS != 0 || item.LastPresentAtMS != 0) {
			t.Fatalf("ghost invented official lifecycle: %+v", item)
		}
	}
	before, _ := store.ListSymbolSnapshotSyncs(ctx)
	fetcher.err = errors.New("upstream timeout")
	if err := worker.RefreshOnce(ctx, source); err == nil {
		t.Fatal("expected failed refresh")
	}
	after, _ := store.ListSymbolSnapshots(ctx, "binance")
	state, _ := store.ListSymbolSnapshotSyncs(ctx)
	if len(after) != len(items) || after[0].CapturedAtMS != items[0].CapturedAtMS || state[0].Status != "unavailable" || state[0].LastSuccessMS != before[0].LastSuccessMS {
		t.Fatalf("failure erased last successful inventory: %+v %+v", after, state)
	}
	fetcher.err = nil
	fetcher.items = nil
	if err := worker.RefreshOnce(ctx, source); err == nil {
		t.Fatal("empty response must not replace inventory")
	}
	after, _ = store.ListSymbolSnapshots(ctx, "binance")
	if len(after) != 2 || after[0].CapturedAtMS != items[0].CapturedAtMS {
		t.Fatal("empty inventory overwrote snapshot")
	}
}
