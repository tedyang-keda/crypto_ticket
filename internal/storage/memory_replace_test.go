package storage

import (
	"context"
	"testing"

	"crypto-ticket/internal/market"
)

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
