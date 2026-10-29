package guardian

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crypto-ticket/internal/exchange"
	"crypto-ticket/internal/market"
	"crypto-ticket/internal/storage"
	"crypto-ticket/internal/timeframe"
)

type minuteFetcherFunc func(context.Context, exchange.KlineRequest) ([]market.Bar, error)

func (f minuteFetcherFunc) Name() string       { return "binance" }
func (f minuteFetcherFunc) MarketType() string { return "um_futures" }
func (f minuteFetcherFunc) FetchKlines(ctx context.Context, _ *http.Client, request exchange.KlineRequest) ([]market.Bar, error) {
	return f(ctx, request)
}

func completeMinuteRange(request exchange.KlineRequest) []market.Bar {
	var bars []market.Bar
	for start := request.StartMS; start <= request.EndMS; start += timeframe.MinuteMS {
		bars = append(bars, testBar(start, 100, 101, 99, 100, 1))
	}
	return bars
}

func setMinuteTestSymbol(t *testing.T, store *storage.MemoryHistoricalStore, now time.Time, listedAtMS int64) auditTarget {
	t.Helper()
	if err := store.UpsertSymbols(context.Background(), []market.SymbolInfo{{
		Exchange: "binance", Symbol: "BTCUSDT", MarketType: "um_futures", IsActive: true,
		Status: "TRADING", LastSeenAtMS: now.UnixMilli(), ListedAtMS: listedAtMS,
	}}); err != nil {
		t.Fatal(err)
	}
	return auditTarget{Exchange: "binance", Symbol: "BTCUSDT", MarketType: "um_futures", ListedAtMS: listedAtMS}
}

func TestMinuteFailuresPersistAcrossUnrelatedSuccessAndRestart(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryHistoricalStore()
	now := time.Date(2026, 10, 29, 8, 0, 0, 0, time.UTC)
	base := now.Add(-10 * time.Minute).UnixMilli()
	setMinuteTestSymbol(t, store, now, base-timeframe.DurationMS("1D"))
	fetcher := minuteFetcherFunc(func(context.Context, exchange.KlineRequest) ([]market.Bar, error) {
		summary, err := store.MinuteAuditSummary(ctx, now.UnixMilli())
		if err != nil || summary.Running != 1 {
			t.Fatalf("REST started before a durable lease existed: summary=%+v err=%v", summary, err)
		}
		return nil, errors.New("temporary official response failure")
	})
	g := New(store, &storeRepairer{store: store}, []Fetcher{fetcher}, Config{Enabled: true, RequireMinuteTaskStore: true})
	g.now = func() time.Time { return now }
	for _, start := range []int64{base, base + 3*timeframe.MinuteMS} {
		if _, err := g.repairRangeWithFetcher(ctx, fetcher, "binance", "BTCUSDT", start, start, "window_audit"); err == nil {
			t.Fatal("official failure was lost")
		}
	}
	good := minuteFetcherFunc(func(_ context.Context, request exchange.KlineRequest) ([]market.Bar, error) {
		return completeMinuteRange(request), nil
	})
	if _, err := g.repairRangeWithFetcher(ctx, good, "binance", "BTCUSDT", base+5*timeframe.MinuteMS, base+5*timeframe.MinuteMS, "window_audit"); err != nil {
		t.Fatal(err)
	}
	summary, _ := store.MinuteAuditSummary(ctx, now.UnixMilli())
	if summary.Pending != 2 {
		t.Fatalf("an unrelated successful range erased failures: %+v", summary)
	}
	// Restart after both ranges have left the ordinary 30-minute window.
	now = now.Add(45 * time.Minute)
	setMinuteTestSymbol(t, store, now, base-timeframe.DurationMS("1D"))
	restarted := New(store, &storeRepairer{store: store}, []Fetcher{good}, Config{Enabled: true, RequireMinuteTaskStore: true})
	restarted.now = func() time.Time { return now }
	if processed, err := restarted.minuteRetryPass(ctx, "binance", false); err != nil || processed != 2 {
		t.Fatalf("persisted minute retries did not resume: processed=%d err=%v", processed, err)
	}
	summary, _ = store.MinuteAuditSummary(ctx, now.UnixMilli())
	if summary.Pending != 0 || summary.Running != 0 || summary.Blocked != 0 {
		t.Fatalf("successful ranges were not acknowledged: %+v", summary)
	}
	for _, start := range []int64{base, base + 3*timeframe.MinuteMS} {
		bars, err := store.BarsInRange(ctx, "binance", "BTCUSDT", "1m", start, start)
		if err != nil || len(bars) != 1 {
			t.Fatalf("old failed minute was not repaired: start=%d bars=%v err=%v", start, bars, err)
		}
	}
}

func TestMinuteUncheckedPartialAndEmptyResponsesStayUnfinished(t *testing.T) {
	ctx := context.Background()
	for _, empty := range []bool{false, true} {
		t.Run(fmt.Sprintf("empty=%t", empty), func(t *testing.T) {
			store := storage.NewMemoryHistoricalStore()
			now := time.Date(2026, 10, 29, 8, 0, 0, 0, time.UTC)
			base := now.Add(-10 * time.Minute).UnixMilli()
			fetcher := minuteFetcherFunc(func(_ context.Context, request exchange.KlineRequest) ([]market.Bar, error) {
				if empty {
					return nil, nil
				}
				return completeMinuteRange(exchange.KlineRequest{StartMS: request.StartMS, EndMS: request.StartMS}), nil
			})
			g := New(store, &storeRepairer{store: store}, []Fetcher{fetcher}, Config{Enabled: true})
			g.now = func() time.Time { return now }
			if _, err := g.repairRangeWithFetcher(ctx, fetcher, "binance", "BTCUSDT", base, base+timeframe.MinuteMS, "window_audit"); err == nil || !strings.Contains(err.Error(), "unchecked") {
				t.Fatalf("unverified response was treated as success: %v", err)
			}
			summary, _ := store.MinuteAuditSummary(ctx, now.UnixMilli())
			if summary.Blocked != 1 {
				t.Fatalf("unverified range was lost: %+v", summary)
			}
			missing, _ := store.BarsInRange(ctx, "binance", "BTCUSDT", "1m", base+timeframe.MinuteMS, base+timeframe.MinuteMS)
			if len(missing) != 0 {
				t.Fatalf("an absent official minute was fabricated: %+v", missing)
			}
		})
	}
}

func TestMinuteWindowWatermarkRegistersDowntimeBeforeAdvancing(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryHistoricalStore()
	now := time.Date(2026, 10, 29, 8, 0, 0, 0, time.UTC)
	fetcher := minuteFetcherFunc(func(_ context.Context, request exchange.KlineRequest) ([]market.Bar, error) {
		return completeMinuteRange(request), nil
	})
	g := New(store, &storeRepairer{store: store}, []Fetcher{fetcher}, Config{Enabled: true})
	g.now = func() time.Time { return now }
	end := g.auditEndStart()
	start := end - 29*timeframe.MinuteMS
	target := setMinuteTestSymbol(t, store, now, start-timeframe.DurationMS("1D"))
	target.Fetcher = fetcher
	previous := start - 61*timeframe.MinuteMS
	if err := g.saveMinuteCursor(ctx, target, "window", previous); err != nil {
		t.Fatal(err)
	}
	if _, err := g.auditMinuteWindow(ctx, target, start, end); err != nil {
		t.Fatal(err)
	}
	due, err := store.ListDueMinuteAuditTasksByKind(ctx, "binance", "non_daily", now.UnixMilli(), 10)
	if err != nil || len(due) != 1 || due[0].Kind != "catchup" || due[0].StartMS != previous+timeframe.MinuteMS || due[0].EndMS != start-timeframe.MinuteMS {
		t.Fatalf("downtime interval was skipped: tasks=%+v err=%v", due, err)
	}
	cursor, _ := store.LoadMinuteAuditCursor(ctx, "binance", "BTCUSDT", "window")
	if cursor == nil || cursor.ThroughMS != end {
		t.Fatalf("unexpected registration watermark: %+v", cursor)
	}
}

func TestMinuteDayAuditRechecksOfficialRevisionAndResumesRegistration(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryHistoricalStore()
	now := time.Date(2026, 10, 29, 8, 0, 0, 0, time.UTC)
	day := timeframe.FloorStartMS(now.UnixMilli(), "1D") - timeframe.DurationMS("1D")
	initial := completeMinuteRange(exchange.KlineRequest{StartMS: day, EndMS: day + timeframe.DurationMS("1D") - timeframe.MinuteMS})
	if err := store.UpsertBars(ctx, initial); err != nil {
		t.Fatal(err)
	}
	setMinuteTestSymbol(t, store, now, day-100*timeframe.DurationMS("1D"))
	var requests atomic.Int32
	fetcher := minuteFetcherFunc(func(ctx context.Context, request exchange.KlineRequest) ([]market.Bar, error) {
		requests.Add(1)
		if enabled, _ := ctx.Value(minuteDayAuditContextKey{}).(bool); !enabled {
			t.Error("daily minute fetch did not carry the background HTTP budget")
		}
		bars := completeMinuteRange(request)
		bars[37] = testBar(day+37*timeframe.MinuteMS, 100, 103, 99, 102, 1)
		return bars, nil
	})
	config := Config{Enabled: true, MinuteDayAuditEnabled: true, RequireMinuteTaskStore: true}
	g := New(store, &storeRepairer{store: store}, []Fetcher{fetcher}, config)
	g.now = func() time.Time { return now }
	if err := g.MinuteDayAuditOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 0 {
		t.Fatal("scheduling performed an inline day fetch")
	}
	restarted := New(store, &storeRepairer{store: store}, []Fetcher{fetcher}, config)
	restarted.now = func() time.Time { return now }
	if err := restarted.MinuteDayAuditOnce(ctx); err != nil {
		t.Fatal(err)
	}
	summary, _ := store.MinuteAuditSummary(ctx, now.UnixMilli())
	if summary.DailyTotal != 1 || summary.Pending != 1 {
		t.Fatalf("restart duplicated or skipped yesterday: %+v", summary)
	}
	if processed, err := restarted.minuteRetryPass(ctx, "binance", true); err != nil || processed != 1 {
		t.Fatalf("daily minute audit failed: processed=%d err=%v", processed, err)
	}
	bars, _ := store.BarsInRange(ctx, "binance", "BTCUSDT", "1m", day+37*timeframe.MinuteMS, day+37*timeframe.MinuteMS)
	if len(bars) != 1 || bars[0].ClosePrice != 102 {
		t.Fatalf("late official revision was not repaired: %+v", bars)
	}
	summary, _ = store.MinuteAuditSummary(ctx, now.UnixMilli())
	if summary.DailyCompleted != 1 || summary.DailyTotal != 1 || summary.Pending != 0 {
		t.Fatalf("daily completion was not retained: %+v", summary)
	}
}

type enqueueSignalStore struct {
	*storage.MemoryHistoricalStore
	enqueued chan int64
}

func (s *enqueueSignalStore) EnqueueMinuteAuditTask(ctx context.Context, task market.MinuteAuditTask) (int64, error) {
	id, err := s.MemoryHistoricalStore.EnqueueMinuteAuditTask(ctx, task)
	if err == nil {
		s.enqueued <- id
	}
	return id, err
}

func TestMinuteLeaseIsAcquiredAfterSymbolLockAndCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := &enqueueSignalStore{MemoryHistoricalStore: storage.NewMemoryHistoricalStore(), enqueued: make(chan int64, 1)}
	now := time.Date(2026, 10, 29, 8, 0, 0, 0, time.UTC)
	var requests atomic.Int32
	fetcher := minuteFetcherFunc(func(context.Context, exchange.KlineRequest) ([]market.Bar, error) {
		requests.Add(1)
		return nil, nil
	})
	g := New(store, &storeRepairer{store: store.MemoryHistoricalStore}, []Fetcher{fetcher}, Config{Enabled: true})
	g.now = func() time.Time { return now }
	lock := g.minuteLock("binance", "BTCUSDT")
	lock.Lock()
	done := make(chan error, 1)
	go func() {
		_, err := g.repairRangeWithFetcher(ctx, fetcher, "binance", "BTCUSDT", now.Add(-time.Minute).UnixMilli(), now.Add(-time.Minute).UnixMilli(), "test")
		done <- err
	}()
	id := <-store.enqueued
	task, err := store.LoadMinuteAuditTask(context.Background(), id)
	if err != nil || task == nil || task.Status != "pending" || task.Attempts != 0 {
		lock.Unlock()
		t.Fatalf("a symbol-lock waiter consumed its lease: task=%+v err=%v", task, err)
	}
	cancel()
	lock.Unlock()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled waiter proceeded: %v", err)
	}
	if requests.Load() != 0 {
		t.Fatal("cancelled waiter called REST")
	}
}

type finishFailureStore struct {
	*storage.MemoryHistoricalStore
	fail bool
}

func (s *finishFailureStore) FinishMinuteAuditTask(ctx context.Context, id int64, token, outcome string, nextMS int64, force bool, lastError string, nowMS int64) error {
	if s.fail {
		return errors.New("ack database unavailable")
	}
	return s.MemoryHistoricalStore.FinishMinuteAuditTask(ctx, id, token, outcome, nextMS, force, lastError, nowMS)
}

type verifiedMinuteSpy struct {
	store        *storage.MemoryHistoricalStore
	verified     []market.Bar
	verifiedAtMS int64
	directWrites int
	replays      int
}

func (s *verifiedMinuteSpy) RepairFinalBars(ctx context.Context, bars []market.Bar) error {
	s.directWrites++
	return s.store.UpsertBars(ctx, bars)
}
func (s *verifiedMinuteSpy) VerifyFinalBars(ctx context.Context, bars []market.Bar, verifiedAtMS int64) error {
	s.verified = append(s.verified, bars...)
	s.verifiedAtMS = verifiedAtMS
	return s.store.UpsertBars(ctx, bars)
}
func (s *verifiedMinuteSpy) RepairRollups(context.Context, []market.Bar) error {
	s.replays++
	return nil
}

func TestMinuteAckFailureReplaysCascadeAfterLeaseRecovery(t *testing.T) {
	ctx := context.Background()
	store := &finishFailureStore{MemoryHistoricalStore: storage.NewMemoryHistoricalStore(), fail: true}
	now := time.Date(2026, 10, 29, 8, 0, 0, 0, time.UTC)
	base := now.Add(-time.Minute).UnixMilli()
	setMinuteTestSymbol(t, store.MemoryHistoricalStore, now, base-timeframe.DurationMS("1D"))
	fetcher := minuteFetcherFunc(func(_ context.Context, request exchange.KlineRequest) ([]market.Bar, error) {
		return completeMinuteRange(request), nil
	})
	spy := &verifiedMinuteSpy{store: store.MemoryHistoricalStore}
	g := New(store, spy, []Fetcher{fetcher}, Config{Enabled: true})
	g.now = func() time.Time { return now }
	if _, err := g.repairRangeWithFetcher(ctx, fetcher, "binance", "BTCUSDT", base, base, "test"); err == nil {
		t.Fatal("failed acknowledgment was hidden")
	}
	summary, _ := store.MinuteAuditSummary(ctx, now.UnixMilli())
	if summary.Running != 1 {
		t.Fatalf("unacknowledged repair disappeared: %+v", summary)
	}
	store.fail = false
	now = now.Add(minuteTaskLease + time.Minute)
	setMinuteTestSymbol(t, store.MemoryHistoricalStore, now, base-timeframe.DurationMS("1D"))
	restarted := New(store, spy, []Fetcher{fetcher}, Config{Enabled: true})
	restarted.now = func() time.Time { return now }
	if processed, err := restarted.minuteRetryPass(ctx, "binance", false); err != nil || processed != 1 {
		t.Fatalf("expired lease was not recovered: processed=%d err=%v", processed, err)
	}
	if spy.replays != 1 {
		t.Fatalf("recovered task did not replay its possibly unfinished cascade: replays=%d", spy.replays)
	}
	summary, _ = store.MinuteAuditSummary(ctx, now.UnixMilli())
	if summary.Pending+summary.Running+summary.Blocked != 0 {
		t.Fatalf("recovered task remained pending: %+v", summary)
	}
}

func TestMinuteVerificationMarksUnchangedOfficialBars(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryHistoricalStore()
	now := time.Date(2026, 10, 29, 8, 0, 0, 0, time.UTC)
	base := now.Add(-3 * time.Minute).UnixMilli()
	request := exchange.KlineRequest{StartMS: base, EndMS: base + timeframe.MinuteMS}
	if err := store.UpsertBars(ctx, completeMinuteRange(request)); err != nil {
		t.Fatal(err)
	}
	fetcher := minuteFetcherFunc(func(_ context.Context, request exchange.KlineRequest) ([]market.Bar, error) {
		return completeMinuteRange(request), nil
	})
	spy := &verifiedMinuteSpy{store: store}
	g := New(store, spy, []Fetcher{fetcher}, Config{Enabled: true})
	g.now = func() time.Time { return now }
	result, err := g.repairTimeframeRange(ctx, fetcher, timeframeRepairRequest{Exchange: "binance", Symbol: "BTCUSDT", Timeframe: "1m", StartMS: request.StartMS, EndMS: request.EndMS, ExpectData: true})
	if err != nil || result.Repaired != 0 || len(spy.verified) != 2 || spy.directWrites != 0 || spy.replays != 0 || spy.verifiedAtMS != now.UnixMilli() {
		t.Fatalf("unchanged official bars were not verified correctly: result=%+v spy=%+v err=%v", result, spy, err)
	}
}

func TestMinuteTaskDoesNotExcludeWhenSnapshotRefreshIsUnavailable(t *testing.T) {
	ctx := context.Background()
	store := &eligibilityStore{MemoryHistoricalStore: storage.NewMemoryHistoricalStore()}
	now := time.Date(2026, 10, 29, 8, 0, 0, 0, time.UTC)
	target := setMinuteTestSymbol(t, store.MemoryHistoricalStore, now, now.Add(-24*time.Hour).UnixMilli())
	store.snapshots = []market.SymbolSnapshot{{Exchange: "binance", Symbol: "BTCUSDT", MarketType: "um_futures", Presence: "present", Subscribed: false, SubscriptionReason: "contract_filtered", CapturedAtMS: now.UnixMilli()}}
	store.syncs = []market.SymbolSnapshotSync{{Exchange: "binance", MarketType: "um_futures", Status: "unavailable", LastSuccessMS: now.UnixMilli()}}
	g := New(store, &storeRepairer{store: store.MemoryHistoricalStore}, []Fetcher{&fakeFetcher{}}, Config{Enabled: true, RequireSymbolSnapshots: true})
	g.now = func() time.Time { return now }
	task := g.newMinuteTask(target, now.Add(-time.Minute).UnixMilli(), now.Add(-time.Minute).UnixMilli(), "catchup")
	id, err := store.EnqueueMinuteAuditTask(ctx, task)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.minuteRetryPass(ctx, "binance", false); err != nil {
		t.Fatal(err)
	}
	stored, _ := store.LoadMinuteAuditTask(ctx, id)
	if stored == nil || stored.Status != "blocked" {
		t.Fatalf("unavailable inventory discarded work: %+v", stored)
	}
}

func TestDailyLeadingPartialWithOfficialBoundaryRemainsValid(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryHistoricalStore()
	day := timeframe.FloorStartMS(1_710_000_000_000, "1D")
	listed := day + 2*timeframe.DurationMS("1D")
	fetcher := &timeframeFetcher{name: "binance", marketType: "um_futures", bars: []market.Bar{dailyTestBar(listed, 100, 101, 99, 100)}}
	g := New(store, &storeRepairer{store: store}, []Fetcher{fetcher}, Config{Enabled: true})
	if _, err := g.repairTimeframeRange(ctx, fetcher, timeframeRepairRequest{Exchange: "binance", Symbol: "BTCUSDT", Timeframe: "1D", StartMS: day, EndMS: listed, ExpectedStartMS: listed, ExpectData: true}); err != nil {
		t.Fatalf("confirmed pre-listing interval was treated as a gap: %v", err)
	}
}

func TestMinuteDayHTTPBudgetComposesWithGlobalBudget(t *testing.T) {
	global := &requestPacer{interval: 12 * time.Millisecond}
	background := &requestPacer{interval: 30 * time.Millisecond}
	type started struct {
		at    time.Time
		daily bool
	}
	starts := make(chan started, 8)
	var wg sync.WaitGroup
	for i := range 8 {
		daily := i%2 == 0
		wg.Add(1)
		go func() {
			defer wg.Done()
			var slow *requestPacer
			if daily {
				slow = background
			}
			if err := waitRequestPacers(context.Background(), global, slow); err != nil {
				t.Error(err)
				return
			}
			starts <- started{at: time.Now(), daily: daily}
		}()
	}
	wg.Wait()
	close(starts)
	var all, days []time.Time
	for start := range starts {
		all = append(all, start.at)
		if start.daily {
			days = append(days, start.at)
		}
	}
	for _, group := range []struct {
		starts []time.Time
		min    time.Duration
	}{{all, 9 * time.Millisecond}, {days, 25 * time.Millisecond}} {
		sort.Slice(group.starts, func(i, j int) bool { return group.starts[i].Before(group.starts[j]) })
		for i := 1; i < len(group.starts); i++ {
			if gap := group.starts[i].Sub(group.starts[i-1]); gap < group.min {
				t.Fatalf("composed request budget allowed a burst: gap=%s required=%s", gap, group.min)
			}
		}
	}
}

func TestMinuteComparisonTreatsBinanceZeroTradeCountAsOfficialValue(t *testing.T) {
	official := testBar(1_710_000_000_000, 100, 101, 99, 100, 1)
	local := official
	official.TradeCount, local.TradeCount = 0, 3
	if !barsDiffer(local, official, defaultFloatTolerance) {
		t.Fatal("Binance official zero trades failed to correct a local nonzero count")
	}
	local.Exchange, official.Exchange = "okx", "okx"
	if barsDiffer(local, official, defaultFloatTolerance) {
		t.Fatal("OKX omitted trade count was treated as an authoritative zero")
	}
}
