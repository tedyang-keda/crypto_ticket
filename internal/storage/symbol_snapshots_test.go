package storage

import (
	"context"
	"testing"

	"crypto-ticket/internal/market"
)

func TestMemorySymbolSnapshotsAtomicValidationAndFreshness(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryHistoricalStore()
	batch := market.SymbolSnapshotBatch{Exchange: "okx", MarketType: "SWAP", CapturedAtMS: 100, Symbols: []market.SymbolSnapshot{{Exchange: "okx", Symbol: "BTC-USDT-SWAP", MarketType: "SWAP", CapturedAtMS: 100, Presence: "present", OfficialRaw: []byte(`{"state":"live"}`)}}}
	if err := store.SaveSymbolSnapshot(ctx, batch); err != nil {
		t.Fatal(err)
	}
	batch.Symbols[0].OfficialRaw[0] = 'X'
	items, _ := store.ListSymbolSnapshots(ctx, "okx")
	if string(items[0].OfficialRaw) != `{"state":"live"}` {
		t.Fatal("mutable caller raw data leaked into persisted snapshot")
	}
	items[0].OfficialRaw[0] = 'X'
	items, _ = store.ListSymbolSnapshots(ctx, "okx")
	if string(items[0].OfficialRaw) != `{"state":"live"}` {
		t.Fatal("read caller mutated stored snapshot")
	}
	bad := market.SymbolSnapshotBatch{Exchange: "okx", MarketType: "SWAP", CapturedAtMS: 200, Symbols: []market.SymbolSnapshot{{Exchange: "okx", Symbol: "ETH-USDT-SWAP", MarketType: "SWAP", CapturedAtMS: 200}, {Exchange: "binance", Symbol: "BTCUSDT", MarketType: "SWAP", CapturedAtMS: 200}}}
	if err := store.SaveSymbolSnapshot(ctx, bad); err == nil {
		t.Fatal("cross-scope batch accepted")
	}
	items, _ = store.ListSymbolSnapshots(ctx, "")
	if len(items) != 1 || items[0].Symbol != "BTC-USDT-SWAP" {
		t.Fatalf("failed batch partially published: %+v", items)
	}
	if err := store.RecordSymbolSnapshotFailure(ctx, market.SymbolSnapshotSync{Exchange: "okx", MarketType: "SWAP", LastAttemptMS: 200, LastError: "timeout"}); err != nil {
		t.Fatal(err)
	}
	state, _ := store.ListSymbolSnapshotSyncs(ctx)
	if state[0].LastSuccessMS != 100 || state[0].Status != "unavailable" || state[0].SymbolCount != 1 {
		t.Fatalf("failure reset successful watermark: %+v", state)
	}
	if err := store.SaveSymbolSnapshot(ctx, batch); err == nil {
		t.Fatal("stale batch overwrote newer scope attempt")
	}
}
