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

func TestHistoricalBackfillerSupportsOKXOfficialAction(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryHistoricalStore()
	bars := []market.Bar{
		historicalBar(100_000, 100, 100, 1),
		historicalBar(160_000, 10, 10.2, 10),
	}
	backfiller := NewHistoricalBackfiller(store, historicalKlineStub{bars: bars}, nil, HistoricalBackfillConfig{})
	result, err := backfiller.Backfill(ctx, HistoricalAction{
		Exchange: "okx", SourceMarket: "okx:SWAP", Symbol: "OPENAI-USDT-SWAP", Ratio: 10,
		WindowStartMS: 1, WindowEndMS: 300_000, AnnouncementCode: "openai-rebase",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Segments) != 2 || result.Segments[0].Provider != OKXProviderName || result.Segments[0].EventType != HistoricalEventOKXRebase {
		t.Fatalf("unexpected OKX factors: %+v", result.Segments)
	}
}

func TestRebuildBoundaryBarsRollsUpAdjustedOneMinute(t *testing.T) {
	const minute = int64(60_000)
	boundary := int64(10 * minute)
	raw := make([]market.Bar, 0, 15)
	for i := 0; i < 15; i++ {
		price := 100 + float64(i)/10
		if int64(i)*minute >= boundary {
			price = 10 + float64(i-10)/10
		}
		raw = append(raw, market.Bar{
			Exchange: "okx", SourceMarket: "okx:SWAP", Symbol: "OPENAI-USDT-SWAP", Timeframe: "1m",
			StartMS: int64(i) * minute, EndMS: int64(i+1)*minute - 1,
			OpenPrice: price, HighPrice: price, LowPrice: price, ClosePrice: price,
			Volume: 1, IsFinal: true,
		})
	}
	segments := CumulativeBackwardSegments(market.AdjustmentFactor{
		Provider: OKXProviderName, ProviderVersion: ProviderVersion, Exchange: "okx", SourceMarket: "okx:SWAP",
		Symbol: "OPENAI-USDT-SWAP", EventType: HistoricalEventOKXRebase,
	}, []LedgerEvent{{EffectiveMS: boundary, PriceMultiplier: 0.1, VolumeMultiplier: 10, EventType: HistoricalEventOKXRebase}})
	rawBars, adjustedBars := rebuildBoundaryBars(HistoricalAction{
		Exchange: "okx", SourceMarket: "okx:SWAP", Symbol: "OPENAI-USDT-SWAP",
	}, boundary, raw, segments)
	raw15 := findTestBar(rawBars, "15m", 0)
	adjusted15 := findTestBar(adjustedBars, "15m", 0)
	if raw15 == nil || adjusted15 == nil {
		t.Fatalf("missing boundary rollups raw=%v adjusted=%v", raw15, adjusted15)
	}
	if raw15.HighPrice < 100 || adjusted15.HighPrice > 11 {
		t.Fatalf("boundary scale was not repaired raw=%+v adjusted=%+v", *raw15, *adjusted15)
	}
	if adjusted15.RawHighPrice != raw15.HighPrice || adjusted15.AdjustmentStatus != market.AdjustmentStatusAdjusted {
		t.Fatalf("materialized raw evidence missing: %+v", *adjusted15)
	}
}

func findTestBar(bars []market.Bar, tf string, startMS int64) *market.Bar {
	for i := range bars {
		if bars[i].Timeframe == tf && bars[i].StartMS == startMS {
			return &bars[i]
		}
	}
	return nil
}
