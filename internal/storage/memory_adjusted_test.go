package storage

import (
	"context"
	"testing"

	"crypto-ticket/internal/market"
)

func TestMemoryRecentBarsMergesMaterializedAndDynamicAdjustedRows(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryHistoricalStore()
	raw := []market.Bar{
		testStorageBar(0, 100), testStorageBar(900_000, 200), testStorageBar(1_800_000, 20),
	}
	if err := store.UpsertBars(ctx, raw); err != nil {
		t.Fatal(err)
	}
	factors := []market.AdjustmentFactor{
		{Provider: "test", ProviderVersion: "v1", Exchange: "okx", SourceMarket: "okx:SWAP", Symbol: "TEST-USDT-SWAP", AdjMode: market.PriceModeBackwardAdjusted, EffectiveFromMS: 0, EffectiveToMS: 1_799_999, PriceMultiplier: 0.1, VolumeMultiplier: 10},
		{Provider: "test", ProviderVersion: "v1", Exchange: "okx", SourceMarket: "okx:SWAP", Symbol: "TEST-USDT-SWAP", AdjMode: market.PriceModeBackwardAdjusted, EffectiveFromMS: 1_800_000, PriceMultiplier: 1, VolumeMultiplier: 1},
	}
	if err := store.ReplaceAdjustmentFactors(ctx, "okx", "okx:SWAP", "TEST-USDT-SWAP", market.PriceModeBackwardAdjusted, factors); err != nil {
		t.Fatal(err)
	}
	materialized := testStorageBar(900_000, 15)
	materialized.PriceMode = market.PriceModeBackwardAdjusted
	materialized.AdjustmentStatus = market.AdjustmentStatusAdjusted
	materialized.AdjustmentProvider = "test_materialized"
	if err := store.UpsertAdjustedBars(ctx, []market.Bar{materialized}); err != nil {
		t.Fatal(err)
	}
	bars, err := store.RecentBars(ctx, market.KlineQuery{
		Exchange: "okx", SourceMarket: "okx:SWAP", Symbol: "TEST-USDT-SWAP", Timeframe: "15m",
		Limit: 3, PriceMode: market.PriceModeBackwardAdjusted,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(bars) != 3 || bars[0].ClosePrice != 10 || bars[1].ClosePrice != 15 || bars[2].ClosePrice != 20 {
		t.Fatalf("unexpected merged rows: %+v", bars)
	}
	if bars[1].AdjustmentProvider != "test_materialized" {
		t.Fatalf("materialized boundary row was not preferred: %+v", bars[1])
	}
}

func TestMemoryRecentBarsMarksCoveredNoActionAsNotRequired(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryHistoricalStore()
	raw := testStorageBar(900_000, 20)
	if err := store.UpsertBars(ctx, []market.Bar{raw}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertCorporateActionEvent(ctx, market.CorporateActionEvent{
		ActionID: "okx|zhipu|coverage", Exchange: "okx", SourceMarket: "okx:SWAP", Symbol: "TEST-USDT-SWAP",
		EventType: market.CorporateActionEventHistoricalCoverage, State: market.CorporateActionStateNotRequired,
		FirstSeenMS: 0, LastEventMS: 2_000_000,
	}); err != nil {
		t.Fatal(err)
	}
	bars, err := store.RecentBars(ctx, market.KlineQuery{
		Exchange: "okx", SourceMarket: "okx:SWAP", Symbol: "TEST-USDT-SWAP", Timeframe: "15m",
		Limit: 1, PriceMode: market.PriceModeBackwardAdjusted,
	})
	if err != nil || len(bars) != 1 {
		t.Fatalf("unexpected query result bars=%+v err=%v", bars, err)
	}
	if bars[0].AdjustmentStatus != market.AdjustmentStatusNotRequired || bars[0].ClosePrice != 20 {
		t.Fatalf("expected raw price with not_required status: %+v", bars[0])
	}
}

func TestMemoryReplaceBarsInRangeDeletesRowsMissingFromOfficialSet(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryHistoricalStore()
	original := []market.Bar{testStorageBar(0, 10), testStorageBar(900_000, 20), testStorageBar(1_800_000, 30)}
	if err := store.UpsertBars(ctx, original); err != nil {
		t.Fatal(err)
	}
	replacement := []market.Bar{testStorageBar(0, 11), testStorageBar(1_800_000, 31)}
	if err := store.ReplaceBarsInRange(ctx, "okx", "TEST-USDT-SWAP", "15m", 0, 1_800_000, replacement); err != nil {
		t.Fatal(err)
	}
	bars, err := store.BarsInRange(ctx, "okx", "TEST-USDT-SWAP", "15m", 0, 1_800_000)
	if err != nil {
		t.Fatal(err)
	}
	if len(bars) != 2 || bars[0].ClosePrice != 11 || bars[1].ClosePrice != 31 {
		t.Fatalf("official set did not replace local range: %+v", bars)
	}
}

func testStorageBar(startMS int64, price float64) market.Bar {
	return market.Bar{
		Exchange: "okx", SourceMarket: "okx:SWAP", Symbol: "TEST-USDT-SWAP", Timeframe: "15m",
		StartMS: startMS, EndMS: startMS + 899_999, OpenPrice: price, HighPrice: price,
		LowPrice: price, ClosePrice: price, Volume: 1, IsFinal: true,
	}
}
