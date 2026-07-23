package adjustment

import (
	"context"
	"math"
	"net/http"
	"testing"

	"crypto-ticket/internal/exchange"
	"crypto-ticket/internal/market"
	"crypto-ticket/internal/storage"
)

type historicalKlineStub struct {
	bars []market.Bar
}

func (s historicalKlineStub) FetchKlines(_ context.Context, _ *http.Client, _ exchange.KlineRequest) ([]market.Bar, error) {
	return s.bars, nil
}

func historicalBar(startMS int64, open float64, closePrice float64, volume float64) market.Bar {
	return market.Bar{
		Exchange: "binance", SourceMarket: "binance:um_futures", Symbol: "KORUUSDT", Timeframe: "1m",
		StartMS: startMS, EndMS: startMS + 59_999, OpenPrice: open, HighPrice: math.Max(open, closePrice),
		LowPrice: math.Min(open, closePrice), ClosePrice: closePrice, Volume: volume, IsFinal: true,
	}
}

func TestHistoricalBackfillerWritesAuthoritativeFactor(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryHistoricalStore()
	bars := []market.Bar{
		historicalBar(100_000, 480, 481.11, 4),
		historicalBar(160_000, 22.68, 23, 8),
		historicalBar(220_000, 23, 23.2, 9),
	}
	backfiller := NewHistoricalBackfiller(store, historicalKlineStub{bars: bars}, nil, HistoricalBackfillConfig{})
	result, err := backfiller.Backfill(ctx, HistoricalAction{
		Exchange: "binance", SourceMarket: "binance:um_futures", Symbol: "KORUUSDT", Ratio: 20,
		WindowStartMS: 1, WindowEndMS: 300_000, PublishedMS: 50_000,
		AnnouncementCode: "koru-adjust", Title: "KORUUSDT adjustment", Raw: []byte(`{"code":"koru-adjust"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Boundary.BoundaryMS != 160_000 || len(result.Segments) != 2 {
		t.Fatalf("unexpected result: %+v", result)
	}
	pre := result.Segments[0]
	if pre.Provider != BinanceProviderName || math.Abs(pre.PriceMultiplier-0.05) > 1e-9 || pre.VolumeMultiplier != 20 {
		t.Fatalf("official ratio was not persisted: %+v", pre)
	}
	factors, err := store.ListAdjustmentFactors(ctx, "binance", "binance:um_futures", "KORUUSDT", market.PriceModeBackwardAdjusted)
	if err != nil || len(factors) != 2 {
		t.Fatalf("stored factors err=%v factors=%+v", err, factors)
	}

	second, err := backfiller.Backfill(ctx, result.Action)
	if err != nil || !second.AlreadyExists {
		t.Fatalf("backfill must be idempotent: result=%+v err=%v", second, err)
	}
}

func TestHistoricalBackfillerDryRunDoesNotWrite(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryHistoricalStore()
	backfiller := NewHistoricalBackfiller(store, historicalKlineStub{bars: []market.Bar{
		historicalBar(100_000, 400, 400, 1), historicalBar(160_000, 20, 20, 1),
	}}, nil, HistoricalBackfillConfig{DryRun: true})
	result, err := backfiller.Backfill(ctx, HistoricalAction{
		Exchange: "binance", SourceMarket: "binance:um_futures", Symbol: "KORUUSDT", Ratio: 20,
		WindowStartMS: 1, WindowEndMS: 200_000, AnnouncementCode: "dry-run",
	})
	if err != nil || len(result.Segments) != 2 {
		t.Fatalf("dry-run should calculate result: %+v err=%v", result, err)
	}
	factors, err := store.ListAdjustmentFactors(ctx, "binance", "binance:um_futures", "KORUUSDT", market.PriceModeBackwardAdjusted)
	if err != nil || len(factors) != 0 {
		t.Fatalf("dry-run wrote factors: err=%v factors=%+v", err, factors)
	}
}

func TestLocateHistoricalBoundaryRejectsUnrelatedGap(t *testing.T) {
	bars := []market.Bar{
		historicalBar(100_000, 100, 100, 1),
		historicalBar(160_000, 50, 50, 1),
	}
	if got, ok := LocateHistoricalBoundary(bars, 20, 0.25); ok {
		t.Fatalf("2:1 observed gap must not match official 20:1 ratio: %+v", got)
	}
}
