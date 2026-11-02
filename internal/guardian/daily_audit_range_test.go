package guardian

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"crypto-ticket/internal/exchange"
	"crypto-ticket/internal/market"
	"crypto-ticket/internal/storage"
	"crypto-ticket/internal/timeframe"
)

type dailyAuditRangeFetcher struct {
	daily       map[int64]market.Bar
	hourlyError map[int64]error
	requests    []exchange.KlineRequest
}

func (f *dailyAuditRangeFetcher) Name() string       { return "binance" }
func (f *dailyAuditRangeFetcher) MarketType() string { return "um_futures" }
func (f *dailyAuditRangeFetcher) FetchKlines(_ context.Context, _ *http.Client, request exchange.KlineRequest) ([]market.Bar, error) {
	f.requests = append(f.requests, request)
	if request.Timeframe == "1H" {
		if err := f.hourlyError[request.StartMS]; err != nil {
			return nil, err
		}
		return zeroActivityHours(request.StartMS), nil
	}
	var out []market.Bar
	for start := request.StartMS; start <= request.EndMS; start = timeframe.NextStartMS(start, "1D") {
		if bar, exists := f.daily[start]; exists {
			out = append(out, bar)
		}
	}
	return out, nil
}

type dailyAuditRangeFixture struct {
	g       *Guardian
	store   *storage.MemoryHistoricalStore
	fetcher *dailyAuditRangeFetcher
	target  auditTarget
	r       dailyAuditRange
}

func newDailyAuditRangeFixture(t *testing.T, days int) dailyAuditRangeFixture {
	t.Helper()
	ctx := context.Background()
	day := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	r := dailyAuditRange{StartMS: day, EndMS: day + int64(days-1)*timeframe.DurationMS("1D")}
	store := storage.NewMemoryHistoricalStore()
	if err := store.UpsertKlineGuardianState(ctx, market.KlineGuardianState{
		Exchange: "binance", Symbol: "BTCUSDT", Timeframe: "1D", Status: "repair_error",
		LastCheckedStartMS: r.StartMS, LastCheckedEndMS: r.EndMS,
	}); err != nil {
		t.Fatal(err)
	}
	fetcher := &dailyAuditRangeFetcher{daily: make(map[int64]market.Bar), hourlyError: make(map[int64]error)}
	g := New(store, &storeRepairer{store: store}, []Fetcher{fetcher}, Config{Enabled: true})
	g.now = func() time.Time { return time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC) }
	return dailyAuditRangeFixture{
		g: g, store: store, fetcher: fetcher, r: r,
		target: auditTarget{Exchange: "binance", Symbol: "BTCUSDT", MarketType: "um_futures", Fetcher: fetcher, ListedAtMS: day},
	}
}

func TestDailyAuditRangeCompletesZeroActivityDaysBeyondFirstSeven(t *testing.T) {
	ctx := context.Background()
	f := newDailyAuditRangeFixture(t, 10)
	if err := f.g.repairDailyAuditRange(ctx, f.target, f.r, true); err != nil {
		t.Fatalf("all ten confirmed no-trade days must complete: %v", err)
	}
	var daily []exchange.KlineRequest
	hourly := 0
	for _, request := range f.fetcher.requests {
		switch request.Timeframe {
		case "1D":
			daily = append(daily, request)
			if request.Limit > maxNoTradeDayChecksPerRange {
				t.Fatalf("daily chunk exceeded fallback capacity: %+v", request)
			}
		case "1H":
			hourly++
		}
	}
	dayMS := timeframe.DurationMS("1D")
	if len(daily) != 2 || daily[0].StartMS != f.r.StartMS || daily[0].EndMS != f.r.StartMS+6*dayMS || daily[1].StartMS != f.r.StartMS+7*dayMS || daily[1].EndMS != f.r.EndMS || hourly != 10 {
		t.Fatalf("expected ordered seven/three-day chunks and ten selective hourly checks: daily=%+v hourly=%d", daily, hourly)
	}
	state, err := f.store.LoadKlineGuardianState(ctx, "binance", "BTCUSDT", "1D")
	if err != nil || state == nil || isRetryableDailyStatus(state.Status) || state.LastCheckedStartMS != f.r.StartMS || state.LastCheckedEndMS != f.r.EndMS {
		t.Fatalf("all chunks succeeded but original wide retry remains unresolved: state=%+v err=%v", state, err)
	}
	bars, err := f.store.BarsInRange(ctx, "binance", "BTCUSDT", "1D", f.r.StartMS, f.r.EndMS)
	if err != nil || len(bars) != 0 {
		t.Fatalf("no-trade evidence must not synthesize daily candles: bars=%+v err=%v", bars, err)
	}
}

func TestDailyAuditRangeEighthDayFailurePreservesOriginalWideRetry(t *testing.T) {
	ctx := context.Background()
	f := newDailyAuditRangeFixture(t, 10)
	f.fetcher.hourlyError[f.r.StartMS+7*timeframe.DurationMS("1D")] = errors.New("day eight official REST unavailable")
	if err := f.g.repairDailyAuditRange(ctx, f.target, f.r, true); err == nil {
		t.Fatal("failed eighth date must not be declared healthy after first seven dates succeed")
	}
	state, err := f.store.LoadKlineGuardianState(ctx, "binance", "BTCUSDT", "1D")
	if err != nil || state == nil || !isRetryableDailyStatus(state.Status) || state.LastCheckedStartMS != f.r.StartMS || state.LastCheckedEndMS != f.r.EndMS {
		t.Fatalf("failed later chunk lost original wide retry: state=%+v err=%v", state, err)
	}
	seenEighth := false
	for _, request := range f.fetcher.requests {
		if request.Timeframe == "1H" && request.StartMS == f.r.StartMS+7*timeframe.DurationMS("1D") {
			seenEighth = true
		}
	}
	if !seenEighth {
		t.Fatal("the eighth date was never checked")
	}
}

func TestDailyAuditRangeFirstChunkFailureStillProcessesSecondChunk(t *testing.T) {
	ctx := context.Background()
	f := newDailyAuditRangeFixture(t, 10)
	f.fetcher.hourlyError[f.r.StartMS] = errors.New("first chunk official REST unavailable")
	secondStart := f.r.StartMS + 7*timeframe.DurationMS("1D")
	f.fetcher.daily[secondStart] = dailyTestBar(secondStart, 100, 101, 99, 100)
	if err := f.g.repairDailyAuditRange(ctx, f.target, f.r, true); err == nil {
		t.Fatal("unknown first chunk must preserve the original failure")
	}
	bars, err := f.store.BarsInRange(ctx, "binance", "BTCUSDT", "1D", secondStart, secondStart)
	if err != nil || len(bars) != 1 || bars[0].StartMS != secondStart {
		t.Fatalf("first chunk failure starved a later repairable chunk: bars=%+v err=%v", bars, err)
	}
	state, err := f.store.LoadKlineGuardianState(ctx, "binance", "BTCUSDT", "1D")
	if err != nil || state == nil || !isRetryableDailyStatus(state.Status) || state.LastCheckedStartMS != f.r.StartMS || state.LastCheckedEndMS != f.r.EndMS {
		t.Fatalf("later successful chunk erased earlier unknown date: state=%+v err=%v", state, err)
	}
}

func TestDailyAuditRangePrelistingWideRetryNeedsNoREST(t *testing.T) {
	ctx := context.Background()
	f := newDailyAuditRangeFixture(t, 20)
	f.target.ListedAtMS = f.r.EndMS + 2*timeframe.DurationMS("1D") + 5*timeframe.DurationMS("1H")
	if err := f.g.repairDailyAuditRange(ctx, f.target, f.r, true); err != nil {
		t.Fatalf("prelisting wide interval must be explicitly excluded: %v", err)
	}
	if len(f.fetcher.requests) != 0 {
		t.Fatalf("prelisting chunking issued unnecessary official requests: %+v", f.fetcher.requests)
	}
	state, err := f.store.LoadKlineGuardianState(ctx, "binance", "BTCUSDT", "1D")
	if err != nil || state == nil || isRetryableDailyStatus(state.Status) || state.LastCheckedStartMS != f.r.StartMS || state.LastCheckedEndMS != f.r.EndMS {
		t.Fatalf("prelisting chunks did not clear original wide retry: state=%+v err=%v", state, err)
	}
}

func TestDailyAuditRangeForwardsForceReplayToEveryChunk(t *testing.T) {
	ctx := context.Background()
	f := newDailyAuditRangeFixture(t, 10)
	// A healthy initial state prevents repairTimeframeRange from independently
	// enabling replay, so every direct write below depends on the helper flag.
	if err := f.store.UpsertKlineGuardianState(ctx, market.KlineGuardianState{
		Exchange: "binance", Symbol: "BTCUSDT", Timeframe: "1D", Status: "ok",
		LastCheckedStartMS: f.r.StartMS, LastCheckedEndMS: f.r.EndMS,
	}); err != nil {
		t.Fatal(err)
	}
	var matching []market.Bar
	for day := f.r.StartMS; day <= f.r.EndMS; day = timeframe.NextStartMS(day, "1D") {
		bar := dailyTestBar(day, 100, 101, 99, 100)
		f.fetcher.daily[day] = bar
		matching = append(matching, bar)
	}
	if err := f.store.UpsertBars(ctx, matching); err != nil {
		t.Fatal(err)
	}
	if err := f.g.repairDailyAuditRange(ctx, f.target, f.r, true); err != nil {
		t.Fatal(err)
	}
	spy := f.g.repairer.(*storeRepairer)
	if len(spy.bars) != len(matching) {
		t.Fatalf("forced replay did not reach every matching daily candle across chunks: writes=%d expected=%d", len(spy.bars), len(matching))
	}
}
