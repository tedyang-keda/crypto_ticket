package storage

import (
	"context"
	"testing"

	"crypto-ticket/internal/market"
)

func TestMinuteVerificationMarkerRejectsChangedValues(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryHistoricalStore()
	bar := market.Bar{Exchange: "binance", Symbol: "BTCUSDT", Timeframe: "1m", StartMS: 60_000,
		EndMS: 119_999, OpenPrice: 100, HighPrice: 105, LowPrice: 99, ClosePrice: 101,
		Volume: 1, QuoteVolume: 100, IsFinal: true}
	if err := store.UpsertBars(ctx, []market.Bar{bar}); err != nil {
		t.Fatal(err)
	}
	changed := bar
	changed.ClosePrice = 104
	if err := store.MarkMinutesVerified(ctx, []market.Bar{changed}, 1000); err == nil {
		t.Fatal("marker accepted an unrelated minute value")
	}
	rows, err := store.LoadMinuteVerifications(ctx, bar.Exchange, bar.Symbol, bar.StartMS, bar.StartMS)
	if err != nil || len(rows) != 1 || rows[0].VerifiedAtMS != 0 {
		t.Fatalf("failed marker CAS locked wrong values: %+v %v", rows, err)
	}
	if err := store.MarkMinutesVerified(ctx, []market.Bar{bar}, 2000); err != nil {
		t.Fatal(err)
	}
	accepted, err := store.UpsertMinuteRevisions(ctx, []market.Bar{changed}, 1500)
	if err != nil || len(accepted) != 0 {
		t.Fatalf("old REST generation bypassed marker: %+v %v", accepted, err)
	}
}
