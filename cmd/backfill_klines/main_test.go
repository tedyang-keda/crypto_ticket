package main

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"crypto-ticket/internal/config"
	"crypto-ticket/internal/exchange"
	"crypto-ticket/internal/market"
	"crypto-ticket/internal/timeframe"
)

type fallbackFetcher struct {
	bars  []market.Bar
	calls []exchange.KlineRequest
}

func (f *fallbackFetcher) Name() string {
	return "test"
}

func (f *fallbackFetcher) FetchKlines(_ context.Context, _ *http.Client, request exchange.KlineRequest) ([]market.Bar, error) {
	f.calls = append(f.calls, request)
	if request.Timeframe == "5D" || request.Timeframe == "3M" {
		return nil, fmt.Errorf("%w: test timeframe", exchange.ErrUnsupportedKlineInterval)
	}
	return f.bars, nil
}

func TestFetchBackfillBarsRollsUpUnsupportedTarget(t *testing.T) {
	targetStart := timeframe.FloorStartMS(time.Now().UTC().AddDate(0, -2, 0).UnixMilli(), "5D")
	source := make([]market.Bar, 0, 5)
	for i := 0; i < 5; i++ {
		startMS := targetStart + int64(i)*timeframe.DayMS
		source = append(source, market.Bar{
			Exchange: "okx", Symbol: "TEST-USDT-SWAP", Timeframe: "1D",
			StartMS: startMS, EndMS: timeframe.EndMS(startMS, "1D"),
			OpenPrice: 10 + float64(i), HighPrice: 12 + float64(i), LowPrice: 9 + float64(i), ClosePrice: 11 + float64(i),
			Volume: 1, QuoteVolume: 10, ContractVolume: 2, TradeCount: 3, IsFinal: true,
		})
	}
	fetcher := &fallbackFetcher{bars: source}

	bars, strategy, err := fetchBackfillBars(context.Background(), http.DefaultClient, fetcher, exchange.KlineRequest{
		Symbol:     "TEST-USDT-SWAP",
		Timeframe:  "5D",
		StartMS:    targetStart,
		EndMS:      timeframe.EndMS(targetStart, "5D"),
		Adjustment: exchange.KlineAdjustmentAuto,
	})
	if err != nil {
		t.Fatal(err)
	}
	if strategy != "rollup:1D" || len(bars) != 1 {
		t.Fatalf("unexpected fallback strategy=%s bars=%+v", strategy, bars)
	}
	bar := bars[0]
	if bar.OpenPrice != 10 || bar.HighPrice != 16 || bar.LowPrice != 9 || bar.ClosePrice != 15 {
		t.Fatalf("unexpected OHLC: %+v", bar)
	}
	if bar.Volume != 5 || bar.QuoteVolume != 50 || bar.ContractVolume != 10 || bar.TradeCount != 15 {
		t.Fatalf("unexpected totals: %+v", bar)
	}
	if len(fetcher.calls) != 2 || fetcher.calls[1].Timeframe != "1D" || fetcher.calls[1].Adjustment != exchange.KlineAdjustmentAuto {
		t.Fatalf("unexpected requests: %+v", fetcher.calls)
	}
}

func TestSourceLimitForTargetIncludesCompleteBuckets(t *testing.T) {
	if got := sourceLimitForTarget("2W", "1D", 3); got < 42 {
		t.Fatalf("expected at least three two-week buckets, got source limit %d", got)
	}
	if got := sourceLimitForTarget("3M", "1D", 1); got < 180 {
		t.Fatalf("expected enough daily bars for a three-month bucket, got %d", got)
	}
}

func TestFetchBackfillBarsUsesExplicitWindowForLargeRollup(t *testing.T) {
	fetcher := &fallbackFetcher{}
	_, _, err := fetchBackfillBars(context.Background(), http.DefaultClient, fetcher, exchange.KlineRequest{
		Symbol: "TEST", Timeframe: "3M", Limit: 300,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(fetcher.calls) != 2 {
		t.Fatalf("expected target and source requests, got %+v", fetcher.calls)
	}
	source := fetcher.calls[1]
	if source.Timeframe != "1D" || source.StartMS <= 0 || source.Limit != 0 {
		t.Fatalf("expected paginated daily source window, got %+v", source)
	}
}

func TestNormalizeTimeframeCSVKeepsMinuteAndMonth(t *testing.T) {
	frames := normalizeTimeframeCSV("1m,5m,1M,1m,3M")
	want := []string{"1m", "5m", "1M", "3M"}
	if len(frames) != len(want) {
		t.Fatalf("unexpected frames: %v", frames)
	}
	for i := range want {
		if frames[i] != want[i] {
			t.Fatalf("unexpected frames: %v", frames)
		}
	}
}

func TestMakeExchangeRuntimesFiltersMarketType(t *testing.T) {
	configs := []config.ExchangeConfig{
		{Name: "binance", MarketType: "um_futures", Enabled: true},
		{Name: "binance", MarketType: "coin_futures", Enabled: true},
		{Name: "okx", MarketType: "SWAP", Enabled: true},
	}
	runtimes := makeExchangeRuntimes(configs, []string{"binance"}, map[string]bool{"um_futures": true})
	if len(runtimes) != 1 || runtimes[0].config.MarketType != "um_futures" {
		t.Fatalf("unexpected runtimes: %+v", runtimes)
	}
}

func TestForwardAdjustOneMinuteBarsHandlesStockSplit(t *testing.T) {
	startMS := timeframe.FloorStartMS(time.Now().UTC().AddDate(0, -1, 0).UnixMilli(), "1D")
	raw := []market.Bar{
		{Exchange: "binance", Symbol: "KORUUSDT", Timeframe: "1m", StartMS: startMS, EndMS: startMS + timeframe.MinuteMS - 1, OpenPrice: 500, HighPrice: 510, LowPrice: 480, ClosePrice: 481, Volume: 10, QuoteVolume: 5_000, IsFinal: true},
		{Exchange: "binance", Symbol: "KORUUSDT", Timeframe: "1m", StartMS: startMS + timeframe.MinuteMS, EndMS: startMS + 2*timeframe.MinuteMS - 1, OpenPrice: 22.68, HighPrice: 23, LowPrice: 22, ClosePrice: 22.5, Volume: 300, QuoteVolume: 6_750, IsFinal: true},
	}

	adjusted, actions := forwardAdjustOneMinuteBars(raw)
	if len(actions) != 1 {
		t.Fatalf("expected one corporate action, got %+v", actions)
	}
	if actions[0].EffectiveMS != raw[1].StartMS || actions[0].AppliedMultiplier != 0.05 {
		t.Fatalf("unexpected action: %+v", actions[0])
	}
	if adjusted[0].OpenPrice != 25 || adjusted[0].HighPrice != 25.5 || adjusted[0].LowPrice != 24 || adjusted[0].ClosePrice != 24.05 {
		t.Fatalf("unexpected adjusted OHLC: %+v", adjusted[0])
	}
	if adjusted[0].Volume != 200 || adjusted[0].QuoteVolume != 5_000 {
		t.Fatalf("unexpected adjusted volume: %+v", adjusted[0])
	}
}

func TestForwardAdjustOneMinuteBarsHandlesReverseSplit(t *testing.T) {
	startMS := timeframe.FloorStartMS(time.Now().UTC().AddDate(0, -1, 0).UnixMilli(), "1D")
	raw := []market.Bar{
		{Exchange: "binance", Symbol: "TESTUSDT", Timeframe: "1m", StartMS: startMS, EndMS: startMS + timeframe.MinuteMS - 1, OpenPrice: 10, HighPrice: 11, LowPrice: 9, ClosePrice: 10, Volume: 100, IsFinal: true},
		{Exchange: "binance", Symbol: "TESTUSDT", Timeframe: "1m", StartMS: startMS + timeframe.MinuteMS, EndMS: startMS + 2*timeframe.MinuteMS - 1, OpenPrice: 49, HighPrice: 51, LowPrice: 48, ClosePrice: 50, Volume: 20, IsFinal: true},
	}

	adjusted, actions := forwardAdjustOneMinuteBars(raw)
	if len(actions) != 1 || actions[0].AppliedMultiplier != 5 {
		t.Fatalf("unexpected reverse split: %+v", actions)
	}
	if adjusted[0].OpenPrice != 50 || adjusted[0].Volume != 20 {
		t.Fatalf("unexpected reverse-adjusted bar: %+v", adjusted[0])
	}
}

func TestForwardAdjustOneMinuteBarsIgnoresOrdinaryMove(t *testing.T) {
	startMS := timeframe.FloorStartMS(time.Now().UTC().AddDate(0, -1, 0).UnixMilli(), "1D")
	raw := []market.Bar{
		{Exchange: "binance", Symbol: "TESTUSDT", Timeframe: "1m", StartMS: startMS, EndMS: startMS + timeframe.MinuteMS - 1, OpenPrice: 100, HighPrice: 101, LowPrice: 99, ClosePrice: 100, Volume: 1, IsFinal: true},
		{Exchange: "binance", Symbol: "TESTUSDT", Timeframe: "1m", StartMS: startMS + timeframe.MinuteMS, EndMS: startMS + 2*timeframe.MinuteMS - 1, OpenPrice: 70, HighPrice: 72, LowPrice: 69, ClosePrice: 71, Volume: 1, IsFinal: true},
	}

	adjusted, actions := forwardAdjustOneMinuteBars(raw)
	if len(actions) != 0 || adjusted[0].OpenPrice != raw[0].OpenPrice {
		t.Fatalf("ordinary move was treated as a corporate action: actions=%+v bars=%+v", actions, adjusted)
	}
}

func TestForwardAdjustedOneMinuteRollupRemovesMixedScale(t *testing.T) {
	startMS := timeframe.FloorStartMS(time.Now().UTC().AddDate(0, -1, 0).UnixMilli(), "1D")
	raw := []market.Bar{
		{Exchange: "binance", Symbol: "CRWDUSDT", Timeframe: "1m", StartMS: startMS, EndMS: startMS + timeframe.MinuteMS - 1, OpenPrice: 772, HighPrice: 774, LowPrice: 760, ClosePrice: 773, Volume: 10, IsFinal: true},
		{Exchange: "binance", Symbol: "CRWDUSDT", Timeframe: "1m", StartMS: startMS + timeframe.MinuteMS, EndMS: startMS + 2*timeframe.MinuteMS - 1, OpenPrice: 193, HighPrice: 205, LowPrice: 185, ClosePrice: 196, Volume: 40, IsFinal: true},
	}
	adjusted, _ := forwardAdjustOneMinuteBars(raw)
	bars := deriveBackfillBars(exchange.KlineRequest{Symbol: "CRWDUSDT", Timeframe: "1D"}, adjusted)
	if len(bars) != 1 {
		t.Fatalf("expected one daily bar, got %+v", bars)
	}
	if bars[0].OpenPrice != 193 || bars[0].HighPrice != 205 || bars[0].LowPrice != 185 || bars[0].ClosePrice != 196 {
		t.Fatalf("mixed-scale daily bar was not repaired: %+v", bars[0])
	}
}

func TestRetainedStartMSClampsFiniteRetentionWindows(t *testing.T) {
	now := time.Date(2026, 7, 26, 10, 0, 0, 0, time.UTC)
	requested := time.Date(2025, 11, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	tests := []struct {
		tf       string
		keepDays int
	}{
		{tf: "1m", keepDays: 7},
		{tf: "5m", keepDays: 30},
		{tf: "12H", keepDays: 180},
	}
	for _, test := range tests {
		want := now.AddDate(0, 0, -test.keepDays).UnixMilli()
		if got := retainedStartMS(test.tf, requested, now); got != want {
			t.Fatalf("timeframe=%s expected cutoff %d, got %d", test.tf, want, got)
		}
	}
	if got := retainedStartMS("1D", requested, now); got != requested {
		t.Fatalf("daily count retention should preserve a newer explicit start, got %d", got)
	}
}

func TestRetainedStartMSKeepsNewerExplicitStart(t *testing.T) {
	now := time.Date(2026, 7, 26, 10, 0, 0, 0, time.UTC)
	requested := now.AddDate(0, 0, -2).UnixMilli()
	if got := retainedStartMS("1m", requested, now); got != requested {
		t.Fatalf("expected explicit newer start %d, got %d", requested, got)
	}
}

func TestRetainedRequestCapsHighTimeframesAt300Bars(t *testing.T) {
	now := time.Date(2026, 7, 26, 10, 0, 0, 0, time.UTC)
	start, limit := retainedRequest("1W", now.AddDate(-10, 0, 0).UnixMilli(), 0, 1000, now)
	if start != 0 || limit != 300 {
		t.Fatalf("expected latest 300 weekly bars, got start=%d limit=%d", start, limit)
	}
	start, limit = retainedRequest("1W", now.AddDate(0, 0, -7).UnixMilli(), 0, 100, now)
	if start == 0 || limit != 100 {
		t.Fatalf("expected recent explicit weekly request to remain bounded, got start=%d limit=%d", start, limit)
	}
}

func TestValidateContinuousKlinesDetectsForwardAdjustment(t *testing.T) {
	raw := []market.Bar{
		{StartMS: 1, EndMS: 1, OpenPrice: 100, ClosePrice: 100},
		{StartMS: 2, EndMS: 2, OpenPrice: 100, ClosePrice: 20},
		{StartMS: 3, EndMS: 3, OpenPrice: 20, ClosePrice: 20},
	}
	adjusted := []market.Bar{
		{StartMS: 1, EndMS: 1, OpenPrice: 20, ClosePrice: 20},
		{StartMS: 2, EndMS: 2, OpenPrice: 20, ClosePrice: 20},
		{StartMS: 3, EndMS: 3, OpenPrice: 20, ClosePrice: 20},
	}
	actions := []corporateAction{{EffectiveMS: 2, ObservedRatio: 0.2, AppliedMultiplier: 0.2}}
	if !validateContinuousKlines(raw, adjusted, actions) {
		t.Fatal("expected continuous data to pass forward-adjustment validation")
	}
	if validateContinuousKlines(raw, raw, actions) {
		t.Fatal("raw data must fail forward-adjustment validation")
	}
}

func TestCandidateCorporateActionBucketsFindsIntraBucketSplit(t *testing.T) {
	raw := []market.Bar{
		{StartMS: 1, HighPrice: 110, LowPrice: 90, ClosePrice: 100},
		{StartMS: 2, HighPrice: 105, LowPrice: 4, ClosePrice: 5},
		{StartMS: 3, HighPrice: 6, LowPrice: 4, ClosePrice: 5},
	}
	continuous := []market.Bar{
		{StartMS: 1, HighPrice: 5.5, LowPrice: 4.5, ClosePrice: 5},
		{StartMS: 2, HighPrice: 5.25, LowPrice: 4, ClosePrice: 5},
		{StartMS: 3, HighPrice: 6, LowPrice: 4, ClosePrice: 5},
	}
	candidates := candidateCorporateActionBuckets(raw, continuous)
	if len(candidates) != 1 || candidates[0].StartMS != 2 {
		t.Fatalf("expected split bucket at 2, got %+v", candidates)
	}
}
