package monitoring

import (
	"context"
	"testing"
	"time"

	"crypto-ticket/internal/aggregator"
	"crypto-ticket/internal/market"
	"crypto-ticket/internal/storage"
	"crypto-ticket/internal/timeframe"
)

func TestIntegrityAuditOnlyReportsMissingWhenSourceComplete(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryHistoricalStore()
	now := time.Date(2026, 7, 28, 1, 2, 0, 0, time.UTC)
	symbol := market.SymbolInfo{Exchange: "okx", Symbol: "BTC-USDT-SWAP", MarketType: "SWAP", IsActive: true}
	if err := store.UpsertSymbols(ctx, []market.SymbolInfo{symbol}); err != nil {
		t.Fatal(err)
	}
	// At 01:02 with a two-minute audit grace period, 00:45 is the latest
	// fully closed 15m bucket selected by the auditor.
	bucketStart := timeframe.FloorStartMS(now.Add(-17*time.Minute).UnixMilli(), "15m")
	var source []market.Bar
	for i := 0; i < 15; i++ {
		start := bucketStart + int64(i)*timeframe.MinuteMS
		source = append(source, market.Bar{
			Exchange: "okx", Symbol: symbol.Symbol, Timeframe: "1m", StartMS: start, EndMS: start + timeframe.MinuteMS - 1,
			OpenPrice: 100, HighPrice: 101, LowPrice: 99, ClosePrice: 100, Volume: 1, QuoteVolume: 100, TradeCount: 1, IsFinal: true,
		})
	}
	if err := store.UpsertBars(ctx, source); err != nil {
		t.Fatal(err)
	}
	registry := newRegistry(func() time.Time { return now })
	auditor := NewIntegrityAuditor(store, registry, IntegrityConfig{Enabled: true, Exchanges: []string{"okx"}, Timeframes: []string{"15m"}, SymbolsPerRun: 1, Buckets: 1})
	auditor.now = func() time.Time { return now }
	summary, err := auditor.AuditOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Missing != 1 || summary.Mismatch != 0 {
		t.Fatalf("expected one missing target, got %+v", summary)
	}
	if len(summary.Issues) != 1 {
		t.Fatalf("expected one issue detail, got %+v", summary.Issues)
	}
	issue := summary.Issues[0]
	if issue.Exchange != symbol.Exchange || issue.Symbol != symbol.Symbol || issue.Timeframe != "15m" || issue.StartMS != bucketStart || issue.Type != "missing" {
		t.Fatalf("unexpected issue detail: %+v", issue)
	}

	expected := aggregator.RollupBars("15m", source, true, "test", now.UnixMilli())
	if err := store.UpsertBars(ctx, []market.Bar{*expected}); err != nil {
		t.Fatal(err)
	}
	summary, err = auditor.AuditOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Missing != 0 || summary.Mismatch != 0 || summary.InvalidOHLC != 0 || len(summary.Issues) != 0 {
		t.Fatalf("expected clean audit, got %+v", summary)
	}
	if values, ok := summary.ByTimeframe["15m"]; !ok || values != [3]int{} {
		t.Fatalf("expected clean timeframe counters to be retained, got %+v", summary.ByTimeframe)
	}

	if err := store.UpsertBars(ctx, source[:14]); err != nil {
		t.Fatal(err)
	}
}

func TestCompleteSourceHandlesUTCWeekBoundary(t *testing.T) {
	start := time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC).UnixMilli()
	var bars []market.Bar
	for i := 0; i < 7; i++ {
		dayStart := start + int64(i)*timeframe.DayMS
		bars = append(bars, market.Bar{StartMS: dayStart, EndMS: dayStart + timeframe.DayMS - 1, Timeframe: "1D", IsFinal: true})
	}
	if !completeSource(bars, "1D", start, timeframe.EndMS(start, "1W")) {
		t.Fatal("expected complete weekly source")
	}
}
