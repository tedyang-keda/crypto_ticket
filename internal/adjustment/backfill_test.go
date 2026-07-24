package adjustment

import (
	"context"
	"math"
	"net/http"
	"testing"

	"crypto-ticket/internal/exchange"
	"crypto-ticket/internal/market"
	"crypto-ticket/internal/storage"
	"crypto-ticket/internal/timeframe"
)

type historicalKlineStub struct {
	boundaryBars   []market.Bar
	emptyTimeframe string
	requests       []exchange.KlineRequest
}

func (s *historicalKlineStub) FetchKlines(_ context.Context, _ *http.Client, request exchange.KlineRequest) ([]market.Bar, error) {
	s.requests = append(s.requests, request)
	if request.StartMS == 1 {
		return append([]market.Bar(nil), s.boundaryBars...), nil
	}
	if request.Timeframe == s.emptyTimeframe {
		return nil, nil
	}
	return []market.Bar{{
		Exchange: "binance", SourceMarket: "binance:um_futures", Symbol: request.Symbol,
		Timeframe: request.Timeframe, StartMS: request.StartMS,
		EndMS: timeframe.EndMS(request.StartMS, request.Timeframe), OpenPrice: 20, HighPrice: 21,
		LowPrice: 19, ClosePrice: 20, Volume: 1, IsFinal: true,
	}}, nil
}

func historicalBar(startMS int64, open float64, closePrice float64, volume float64) market.Bar {
	return market.Bar{
		Exchange: "binance", SourceMarket: "binance:um_futures", Symbol: "KORUUSDT", Timeframe: "1m",
		StartMS: startMS, EndMS: startMS + 59_999, OpenPrice: open, HighPrice: math.Max(open, closePrice),
		LowPrice: math.Min(open, closePrice), ClosePrice: closePrice, Volume: volume, IsFinal: true,
	}
}

func TestHistoricalBackfillerReplacesOfficialBarsForEveryTimeframe(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryHistoricalStore()
	source := &historicalKlineStub{boundaryBars: []market.Bar{
		historicalBar(100_000, 480, 481.11, 4),
		historicalBar(160_000, 22.68, 23, 8),
	}}
	backfiller := NewHistoricalBackfiller(store, source, nil, HistoricalBackfillConfig{})
	result, err := backfiller.Backfill(ctx, HistoricalAction{
		Exchange: "binance", SourceMarket: "binance:um_futures", Symbol: "KORUUSDT", Ratio: 20,
		WindowStartMS: 1, WindowEndMS: 300_000, PublishedMS: 50_000,
		AnnouncementCode: "koru-adjust", Title: "KORUUSDT adjustment",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Boundary.BoundaryMS != 160_000 || result.RawBarsReplaced != 11 {
		t.Fatalf("unexpected repair result: %+v", result)
	}
	if len(source.requests) != 12 {
		t.Fatalf("expected boundary request plus eleven official timeframes, got %d", len(source.requests))
	}
	for _, tf := range []string{"1m", "5m", "15m", "30m", "1H", "2H", "4H", "6H", "12H", "1D", "1W"} {
		bars, err := store.BarsInRange(ctx, "binance", "KORUUSDT", tf, math.MinInt64, math.MaxInt64)
		if err != nil || len(bars) != 1 {
			t.Fatalf("official %s bars were not replaced: bars=%+v err=%v", tf, bars, err)
		}
	}
}

func TestOfficialRepairTimeframesByExchange(t *testing.T) {
	tests := []struct {
		exchange string
		want     []string
	}{
		{
			exchange: "binance",
			want:     []string{"1m", "5m", "15m", "30m", "1H", "2H", "4H", "6H", "12H", "1D", "1W"},
		},
		{
			exchange: "okx",
			want:     []string{"1m", "5m", "15m", "30m", "1H", "2H", "4H", "6H", "12H", "1D", "2D", "1W"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.exchange, func(t *testing.T) {
			got := officialRepairTimeframes(tc.exchange)
			if len(got) != len(tc.want) {
				t.Fatalf("unexpected frames: got=%v want=%v", got, tc.want)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Fatalf("unexpected frames: got=%v want=%v", got, tc.want)
				}
			}
		})
	}
}

func TestHistoricalBackfillerDryRunDoesNotWrite(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryHistoricalStore()
	source := &historicalKlineStub{boundaryBars: []market.Bar{
		historicalBar(100_000, 400, 400, 1), historicalBar(160_000, 20, 20, 1),
	}}
	backfiller := NewHistoricalBackfiller(store, source, nil, HistoricalBackfillConfig{DryRun: true})
	result, err := backfiller.Backfill(ctx, HistoricalAction{
		Exchange: "binance", SourceMarket: "binance:um_futures", Symbol: "KORUUSDT", Ratio: 20,
		WindowStartMS: 1, WindowEndMS: 200_000, AnnouncementCode: "dry-run",
	})
	if err != nil || result.RawBarsReplaced != 0 {
		t.Fatalf("unexpected dry-run result: %+v err=%v", result, err)
	}
	bars, err := store.BarsInRange(ctx, "binance", "KORUUSDT", "1m", 0, math.MaxInt64)
	if err != nil || len(bars) != 0 {
		t.Fatalf("dry-run wrote bars: bars=%+v err=%v", bars, err)
	}
}

func TestHistoricalBackfillerDoesNotDeleteLocalBarsWhenOfficialWindowIsEmpty(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryHistoricalStore()
	existing := market.Bar{
		Exchange: "binance", Symbol: "KORUUSDT", Timeframe: "5m", StartMS: 0,
		EndMS: timeframe.EndMS(0, "5m"), OpenPrice: 99, HighPrice: 99,
		LowPrice: 99, ClosePrice: 99, Volume: 1, IsFinal: true,
	}
	if err := store.UpsertBars(ctx, []market.Bar{existing}); err != nil {
		t.Fatal(err)
	}
	source := &historicalKlineStub{
		emptyTimeframe: "5m",
		boundaryBars: []market.Bar{
			historicalBar(100_000, 400, 400, 1), historicalBar(160_000, 20, 20, 1),
		},
	}
	backfiller := NewHistoricalBackfiller(store, source, nil, HistoricalBackfillConfig{})
	_, err := backfiller.Backfill(ctx, HistoricalAction{
		Exchange: "binance", SourceMarket: "binance:um_futures", Symbol: "KORUUSDT", Ratio: 20,
		WindowStartMS: 1, WindowEndMS: 200_000, AnnouncementCode: "empty-window",
	})
	if err == nil {
		t.Fatal("expected empty official timeframe to fail the repair")
	}
	bars, readErr := store.BarsInRange(ctx, "binance", "KORUUSDT", "5m", math.MinInt64, math.MaxInt64)
	if readErr != nil || len(bars) != 1 || bars[0].OpenPrice != existing.OpenPrice {
		t.Fatalf("existing bars changed after rejected repair: bars=%+v err=%v", bars, readErr)
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

func TestOfficialRepairRangeIncludesAdjacentBuckets(t *testing.T) {
	boundary := int64(1782802800000)
	dayStart := timeframe.FloorStartMS(boundary, "1D")
	dayEnd := timeframe.EndMS(dayStart, "1D")
	start4H, end4H := officialRepairRange(boundary, "4H", dayStart, dayEnd)
	if start4H != int64(1782763200000) || end4H != dayEnd {
		t.Fatalf("unexpected 4H repair range [%d,%d]", start4H, end4H)
	}
	start1D, end1D := officialRepairRange(boundary, "1D", dayStart, dayEnd)
	if start1D != dayStart-2*24*60*60*1000 || end1D != dayEnd+2*24*60*60*1000 {
		t.Fatalf("unexpected 1D repair range [%d,%d]", start1D, end1D)
	}
}
