package guardian

import (
	"context"
	"errors"
	"math"
	"net/http"
	"testing"
	"time"

	"crypto-ticket/internal/exchange"
	"crypto-ticket/internal/market"
	"crypto-ticket/internal/storage"
	"crypto-ticket/internal/timeframe"
)

type dailyCoverageFetcher struct {
	daily, hourly []market.Bar
	hourErr       error
	requests      []exchange.KlineRequest
}

func (f *dailyCoverageFetcher) Name() string       { return "binance" }
func (f *dailyCoverageFetcher) MarketType() string { return "um_futures" }
func (f *dailyCoverageFetcher) FetchKlines(_ context.Context, _ *http.Client, request exchange.KlineRequest) ([]market.Bar, error) {
	f.requests = append(f.requests, request)
	if request.Timeframe == "1H" {
		return f.hourly, f.hourErr
	}
	return f.daily, nil
}

func zeroActivityHours(day int64) []market.Bar {
	bars := make([]market.Bar, 24)
	for i := range bars {
		start := day + int64(i)*timeframe.DurationMS("1H")
		bars[i] = dailyTestBar(start, 100, 100, 100, 100)
		bars[i].Timeframe = "1H"
		bars[i].EndMS = timeframe.EndMS(start, "1H")
		bars[i].LastTickMS = bars[i].EndMS
		bars[i].Volume, bars[i].QuoteVolume, bars[i].ContractVolume, bars[i].TradeCount = 0, 0, 0, 0
	}
	return bars
}

func TestDailyRepairConfirmsOnlyCompleteZeroActivityDay(t *testing.T) {
	ctx := context.Background()
	day := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC).UnixMilli() // Sunday is not itself evidence.
	store := storage.NewMemoryHistoricalStore()
	fetcher := &dailyCoverageFetcher{hourly: zeroActivityHours(day)}
	repairer := &dailyRollupRepairSpy{storeRepairer: &storeRepairer{store: store}}
	g := New(store, repairer, []Fetcher{fetcher}, Config{Enabled: true})
	g.now = func() time.Time { return time.UnixMilli(day).Add(48 * time.Hour) }
	request := timeframeRepairRequest{Exchange: "binance", Symbol: "BTCUSDT", Timeframe: "1D", StartMS: day, EndMS: day, ExpectData: true, ExpectedStartMS: day, ForceReplay: true}
	result, err := g.repairTimeframeRange(ctx, fetcher, request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Checked != 0 || result.Repaired != 0 || len(result.Events) != 1 || result.Events[0].EventType != "confirmed_no_trades" {
		t.Fatalf("expected explicit exclusion without manufactured candle: %+v", result)
	}
	if len(repairer.bars) != 0 || repairer.calls != 0 {
		t.Fatal("no-trade evidence must neither manufacture daily bars nor rebuild without daily sources")
	}
	if len(fetcher.requests) != 2 || fetcher.requests[1].StartMS != day || fetcher.requests[1].EndMS != day+23*timeframe.DurationMS("1H") || fetcher.requests[1].Limit != 24 {
		t.Fatalf("unexpected selective hourly query: %+v", fetcher.requests)
	}
	state, _ := store.LoadKlineGuardianState(ctx, "binance", "BTCUSDT", "1D")
	if state == nil || state.Status != "confirmed_no_trades" {
		t.Fatalf("missing explicit persisted evidence status: %+v", state)
	}
}

func TestDailyNoTradeEvidenceRejectsUncertainOrContradictoryData(t *testing.T) {
	day := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	cases := []struct {
		name   string
		mutate func(*dailyCoverageFetcher) []market.Bar
	}{
		{"empty", func(f *dailyCoverageFetcher) []market.Bar { f.hourly = nil; return nil }},
		{"missing_hour", func(f *dailyCoverageFetcher) []market.Bar { f.hourly = f.hourly[:23]; return nil }},
		{"duplicate_hour", func(f *dailyCoverageFetcher) []market.Bar { f.hourly[23] = f.hourly[22]; return nil }},
		{"wrong_symbol", func(f *dailyCoverageFetcher) []market.Bar { f.hourly[0].Symbol = "OTHER"; return nil }},
		{"wrong_exchange", func(f *dailyCoverageFetcher) []market.Bar { f.hourly[0].Exchange = "okx"; return nil }},
		{"wrong_timeframe", func(f *dailyCoverageFetcher) []market.Bar { f.hourly[0].Timeframe = "1m"; return nil }},
		{"non_final", func(f *dailyCoverageFetcher) []market.Bar { f.hourly[0].IsFinal = false; return nil }},
		{"wrong_end", func(f *dailyCoverageFetcher) []market.Bar { f.hourly[0].EndMS--; return nil }},
		{"invalid_price", func(f *dailyCoverageFetcher) []market.Bar { f.hourly[0].LowPrice = 101; return nil }},
		{"negative_volume", func(f *dailyCoverageFetcher) []market.Bar { f.hourly[0].Volume = -1; return nil }},
		{"nan_volume", func(f *dailyCoverageFetcher) []market.Bar { f.hourly[0].Volume = math.NaN(); return nil }},
		{"inf_volume", func(f *dailyCoverageFetcher) []market.Bar { f.hourly[0].QuoteVolume = math.Inf(1); return nil }},
		{"positive_volume", func(f *dailyCoverageFetcher) []market.Bar { f.hourly[0].Volume = 1; return nil }},
		{"positive_quote", func(f *dailyCoverageFetcher) []market.Bar { f.hourly[0].QuoteVolume = 1; return nil }},
		{"positive_contract", func(f *dailyCoverageFetcher) []market.Bar { f.hourly[0].ContractVolume = 1; return nil }},
		{"positive_trades", func(f *dailyCoverageFetcher) []market.Bar { f.hourly[0].TradeCount = 1; return nil }},
		{"negative_trades", func(f *dailyCoverageFetcher) []market.Bar { f.hourly[0].TradeCount = -1; return nil }},
		{"local_daily_activity", func(_ *dailyCoverageFetcher) []market.Bar { return []market.Bar{dailyTestBar(day, 100, 101, 99, 100)} }},
		{"local_hourly_activity", func(_ *dailyCoverageFetcher) []market.Bar {
			bar := zeroActivityHours(day)[0]
			bar.ContractVolume = 1
			return []market.Bar{bar}
		}},
		{"invalid_daily_response", func(f *dailyCoverageFetcher) []market.Bar {
			bar := dailyTestBar(day, 100, 101, 99, 100)
			bar.IsFinal = false
			f.daily = []market.Bar{bar}
			return nil
		}},
		{"rest_error", func(f *dailyCoverageFetcher) []market.Bar { f.hourErr = errors.New("official timeout"); return nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := storage.NewMemoryHistoricalStore()
			fetcher := &dailyCoverageFetcher{hourly: zeroActivityHours(day)}
			if err := store.UpsertBars(ctx, tc.mutate(fetcher)); err != nil {
				t.Fatal(err)
			}
			repairer := &storeRepairer{store: store}
			g := New(store, repairer, []Fetcher{fetcher}, Config{Enabled: true})
			g.now = func() time.Time { return time.UnixMilli(day).Add(48 * time.Hour) }
			result, err := g.repairTimeframeRange(ctx, fetcher, timeframeRepairRequest{Exchange: "binance", Symbol: "BTCUSDT", Timeframe: "1D", StartMS: day, EndMS: day, ExpectData: true, ExpectedStartMS: day})
			if err == nil || len(repairer.bars) != 0 {
				t.Fatalf("uncertain evidence must remain pending without writes: result=%+v err=%v", result, err)
			}
			for _, event := range result.Events {
				if event.EventType == "confirmed_no_trades" {
					t.Fatalf("false no-trade evidence: %+v", result)
				}
			}
			state, _ := store.LoadKlineGuardianState(ctx, "binance", "BTCUSDT", "1D")
			if state == nil || !isRetryableDailyStatus(state.Status) {
				t.Fatalf("uncertain evidence lost retry state: %+v", state)
			}
		})
	}
}

func TestDailyRepairKeepsOfficialZeroDailyAndRepairsMixedRange(t *testing.T) {
	ctx := context.Background()
	day := timeframe.FloorStartMS(1_710_000_000_000, "1D")
	store := storage.NewMemoryHistoricalStore()
	zeroDaily := dailyTestBar(day, 100, 100, 100, 100)
	zeroDaily.Volume, zeroDaily.QuoteVolume, zeroDaily.TradeCount = 0, 0, 0
	gapDay := timeframe.NextStartMS(day, "1D")
	fetcher := &dailyCoverageFetcher{daily: []market.Bar{zeroDaily}, hourly: zeroActivityHours(gapDay)}
	repairer := &dailyRollupRepairSpy{storeRepairer: &storeRepairer{store: store}}
	g := New(store, repairer, []Fetcher{fetcher}, Config{Enabled: true})
	result, err := g.repairTimeframeRange(ctx, fetcher, timeframeRepairRequest{Exchange: "binance", Symbol: "BTCUSDT", Timeframe: "1D", StartMS: day, EndMS: gapDay, ExpectData: true, ExpectedStartMS: day})
	if err != nil || result.Checked != 1 || result.Repaired != 1 || len(result.Events) != 2 || repairer.calls != 1 || len(repairer.bars) != 1 {
		t.Fatalf("mixed valid daily and confirmed date failed: result=%+v err=%v repairer=%+v", result, err, repairer)
	}
	if repairer.bars[0].StartMS != day || repairer.bars[0].Volume != 0 {
		t.Fatal("official zero-volume daily candle must be retained")
	}
	result, err = g.repairTimeframeRange(ctx, fetcher, timeframeRepairRequest{Exchange: "binance", Symbol: "BTCUSDT", Timeframe: "1D", StartMS: day, EndMS: gapDay, ExpectData: true, ExpectedStartMS: day})
	if err != nil || result.Repaired != 0 || len(repairer.bars) != 1 {
		t.Fatalf("mixed daily repair not idempotent: result=%+v err=%v", result, err)
	}
}

func TestDailyNoTradeEvidenceBoundsFallbackRequests(t *testing.T) {
	ctx := context.Background()
	day := timeframe.FloorStartMS(1_710_000_000_000, "1D")
	store := storage.NewMemoryHistoricalStore()
	fetcher := &dailyCoverageFetcher{}
	g := New(store, &storeRepairer{store: store}, []Fetcher{fetcher}, Config{Enabled: true})
	_, err := g.repairTimeframeRange(ctx, fetcher, timeframeRepairRequest{Exchange: "binance", Symbol: "BTCUSDT", Timeframe: "1D", StartMS: day, EndMS: day + 30*timeframe.DurationMS("1D"), ExpectData: true})
	if err == nil || len(fetcher.requests) != 1+maxNoTradeDayChecksPerRange {
		t.Fatalf("unbounded fallback or false healthy result: requests=%d err=%v", len(fetcher.requests), err)
	}
}
