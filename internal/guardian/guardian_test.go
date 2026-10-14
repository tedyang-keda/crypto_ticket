package guardian

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crypto-ticket/internal/exchange"
	"crypto-ticket/internal/market"
	"crypto-ticket/internal/storage"
	"crypto-ticket/internal/timeframe"
)

func TestHandleFinalBarDetectsWatermarkGapAndRepairs(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryHistoricalStore()
	base := int64(1_710_000_000_000)
	upsertActiveSymbol(t, store)
	if err := store.UpsertBars(ctx, []market.Bar{testBar(base, 100, 105, 99, 101, 1)}); err != nil {
		t.Fatalf("upsert baseline: %v", err)
	}

	fetcher := &fakeFetcher{bars: []market.Bar{
		testBar(base+60_000, 101, 106, 100, 102, 2),
		testBar(base+120_000, 102, 107, 101, 103, 3),
	}}
	repairer := &storeRepairer{store: store}
	g := New(store, repairer, []Fetcher{fetcher}, Config{Enabled: true, AuditWindow: 10 * 60_000_000_000})

	current := testBar(base+180_000, 103, 108, 102, 104, 4)
	if err := g.handleFinalBar(ctx, current); err != nil {
		t.Fatalf("handle final: %v", err)
	}
	if len(repairer.bars) != 2 {
		t.Fatalf("expected two repaired bars, got %+v", repairer.bars)
	}
	if repairer.bars[0].StartMS != base+60_000 || repairer.bars[1].StartMS != base+120_000 {
		t.Fatalf("unexpected repair range: %+v", repairer.bars)
	}
	state, err := store.LoadKlineGuardianState(ctx, "binance", "BTCUSDT", "1m")
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if state == nil ||
		state.Status != "gap_repaired" ||
		state.LastFinalStartMS != current.StartMS ||
		state.LastGapStartMS != base+60_000 ||
		state.LastGapEndMS != base+120_000 ||
		state.LastCheckedStartMS != base+60_000 ||
		state.LastCheckedEndMS != base+120_000 {
		t.Fatalf("unexpected state: %+v", state)
	}
}

func TestRepairRangeRepairsMissingAndMismatchedBars(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryHistoricalStore()
	base := int64(1_710_000_000_000)
	upsertActiveSymbol(t, store)
	if err := store.UpsertBars(ctx, []market.Bar{testBar(base, 100, 105, 99, 101, 1)}); err != nil {
		t.Fatalf("upsert local: %v", err)
	}

	fetcher := &fakeFetcher{bars: []market.Bar{
		testBar(base, 100, 110, 99, 104, 5),
		testBar(base+60_000, 104, 111, 103, 108, 6),
	}}
	repairer := &storeRepairer{store: store}
	g := New(store, repairer, []Fetcher{fetcher}, Config{Enabled: true})

	result, err := g.repairRangeWithFetcher(ctx, fetcher, "binance", "BTCUSDT", base, base+60_000, "window_audit")
	if err != nil {
		t.Fatalf("repair range: %v", err)
	}
	if result.Checked != 2 || result.Repaired != 2 {
		t.Fatalf("unexpected repair result: %+v", result)
	}
	bars, err := store.BarsInRange(ctx, "binance", "BTCUSDT", "1m", base, base+60_000)
	if err != nil {
		t.Fatalf("bars in range: %v", err)
	}
	if len(bars) != 2 || bars[0].ClosePrice != 104 || bars[1].ClosePrice != 108 {
		t.Fatalf("expected repaired store bars, got %+v", bars)
	}
	state, err := store.LoadKlineGuardianState(ctx, "binance", "BTCUSDT", "1m")
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if state == nil || state.Status != "repaired" || state.LastCheckedStartMS != base || state.LastCheckedEndMS != base+60_000 {
		t.Fatalf("unexpected state: %+v", state)
	}
}

func TestAuditTargetsSkipStaleSymbols(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryHistoricalStore()
	now := market.NowMS()
	if err := store.UpsertSymbols(ctx, []market.SymbolInfo{
		{Exchange: "binance", Symbol: "BTCUSDT", MarketType: "um_futures", Status: "TRADING", IsActive: true, LastSeenAtMS: now},
		{Exchange: "binance", Symbol: "OLDUSDT", MarketType: "um_futures", Status: "TRADING", IsActive: true, LastSeenAtMS: now - int64(time.Hour/time.Millisecond)},
		{Exchange: "binance", Symbol: "UNKNOWNUSDT", MarketType: "um_futures", Status: "TRADING", IsActive: true},
	}); err != nil {
		t.Fatalf("upsert symbols: %v", err)
	}
	g := New(store, &storeRepairer{store: store}, []Fetcher{&fakeFetcher{}}, Config{Enabled: true, SymbolMaxAge: 10 * time.Minute})

	targets, err := g.auditTargets(ctx)
	if err != nil {
		t.Fatalf("audit targets: %v", err)
	}
	if len(targets) != 1 || targets[0].Symbol != "BTCUSDT" {
		t.Fatalf("expected only fresh symbol, got %+v", targets)
	}
}

func TestAuditOnceChecksAllTargetsWhenSymbolsPerRunIsZero(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryHistoricalStore()
	now := market.NowMS()
	var symbols []market.SymbolInfo
	for i := 0; i < 75; i++ {
		symbols = append(symbols, market.SymbolInfo{
			Exchange: "binance", Symbol: fmt.Sprintf("TEST%03dUSDT", i), MarketType: "um_futures",
			Status: "TRADING", IsActive: true, LastSeenAtMS: now,
		})
	}
	if err := store.UpsertSymbols(ctx, symbols); err != nil {
		t.Fatal(err)
	}
	fetcher := &countingFetcher{name: "binance", marketType: "um_futures"}
	g := New(store, &storeRepairer{store: store}, []Fetcher{fetcher}, Config{Enabled: true, SymbolsPerRun: 0})
	result, err := g.auditOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.CheckedSymbols != len(symbols) || fetcher.count.Load() != int64(len(symbols)) {
		t.Fatalf("expected full audit of %d symbols, result=%+v requests=%d", len(symbols), result, fetcher.count.Load())
	}
}

func TestGuardianUsesForwardAdjustmentForOKX(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryHistoricalStore()
	base := int64(1_710_000_000_000)
	fetcher := &fakeFetcher{name: "okx", marketType: "SWAP", bars: []market.Bar{
		testBarFor("okx", "KORU-USDT-SWAP", base, 5),
	}}
	g := New(store, &storeRepairer{store: store}, []Fetcher{fetcher}, Config{Enabled: true})
	if _, err := g.repairRangeWithFetcher(ctx, fetcher, "okx", "KORU-USDT-SWAP", base, base, "test"); err != nil {
		t.Fatal(err)
	}
	if fetcher.lastRequest.Adjustment != exchange.KlineAdjustmentAuto {
		t.Fatalf("expected OKX auto/forward adjustment, got %q", fetcher.lastRequest.Adjustment)
	}
}

func TestDailyRepairMissingMismatchAndIdempotent(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryHistoricalStore()
	base := timeframe.FloorStartMS(1_710_000_000_000, "1D")
	official := dailyTestBar(base, 100, 110, 99, 105)
	fetcher := &timeframeFetcher{name: "binance", marketType: "um_futures", bars: []market.Bar{official}}
	repairer := &storeRepairer{store: store}
	g := New(store, repairer, []Fetcher{fetcher}, Config{Enabled: true})

	result, err := g.repairTimeframeRange(ctx, fetcher, timeframeRepairRequest{
		Exchange: "binance", Symbol: "BTCUSDT", Timeframe: "1D", StartMS: base, EndMS: base, Reason: "daily_test", ExpectData: true,
	})
	if err != nil || result.Repaired != 1 || len(repairer.bars) != 1 {
		t.Fatalf("expected missing daily repair, result=%+v err=%v bars=%d", result, err, len(repairer.bars))
	}
	result, err = g.repairTimeframeRange(ctx, fetcher, timeframeRepairRequest{
		Exchange: "binance", Symbol: "BTCUSDT", Timeframe: "1D", StartMS: base, EndMS: base, Reason: "daily_test", ExpectData: true,
	})
	if err != nil || result.Repaired != 0 || len(repairer.bars) != 1 {
		t.Fatalf("expected idempotent daily repair, result=%+v err=%v bars=%d", result, err, len(repairer.bars))
	}
	bad := official
	bad.ClosePrice = 106
	fetcher.bars = []market.Bar{bad}
	result, err = g.repairTimeframeRange(ctx, fetcher, timeframeRepairRequest{
		Exchange: "binance", Symbol: "BTCUSDT", Timeframe: "1D", StartMS: base, EndMS: base, Reason: "daily_test", ExpectData: true,
	})
	if err != nil || result.Repaired != 1 {
		t.Fatalf("expected daily mismatch repair, result=%+v err=%v", result, err)
	}
}

func TestDailyRepairRejectsNonFinalAndEmptyExisting(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryHistoricalStore()
	base := timeframe.FloorStartMS(1_710_000_000_000, "1D")
	official := dailyTestBar(base, 100, 110, 99, 105)
	official.IsFinal = false
	fetcher := &timeframeFetcher{name: "binance", marketType: "um_futures", bars: []market.Bar{official}}
	g := New(store, &storeRepairer{store: store}, []Fetcher{fetcher}, Config{Enabled: true})
	if _, err := g.repairTimeframeRange(ctx, fetcher, timeframeRepairRequest{
		Exchange: "binance", Symbol: "BTCUSDT", Timeframe: "1D", StartMS: base, EndMS: base, Reason: "daily_test", ExpectData: true,
	}); err == nil {
		t.Fatal("expected non-final official response to fail")
	}
	if err := store.UpsertBars(ctx, []market.Bar{dailyTestBar(base, 100, 110, 99, 105)}); err != nil {
		t.Fatal(err)
	}
	fetcher.bars = nil
	if _, err := g.repairTimeframeRange(ctx, fetcher, timeframeRepairRequest{
		Exchange: "binance", Symbol: "BTCUSDT", Timeframe: "1D", StartMS: base, EndMS: base, Reason: "daily_test",
	}); err == nil {
		t.Fatal("expected empty response with local data to fail")
	}
}

func TestDailyRepairRecordsRESTErrorAndRetries(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryHistoricalStore()
	base := timeframe.FloorStartMS(1_710_000_000_000, "1D")
	fetcher := &timeframeFetcher{name: "binance", marketType: "um_futures", err: fmt.Errorf("temporary HTTP timeout")}
	g := New(store, &storeRepairer{store: store}, []Fetcher{fetcher}, Config{Enabled: true})
	request := timeframeRepairRequest{Exchange: "binance", Symbol: "BTCUSDT", Timeframe: "1D", StartMS: base, EndMS: base, Reason: "daily_test", ExpectData: true}
	if _, err := g.repairTimeframeRange(ctx, fetcher, request); err == nil {
		t.Fatal("expected REST error")
	}
	state, err := store.LoadKlineGuardianState(ctx, "binance", "BTCUSDT", "1D")
	if err != nil || state == nil || state.Status != "rest_error" {
		t.Fatalf("expected persisted REST error state, state=%+v err=%v", state, err)
	}
	fetcher.err = nil
	fetcher.bars = []market.Bar{dailyTestBar(base, 100, 110, 99, 105)}
	if _, err := g.repairTimeframeRange(ctx, fetcher, request); err != nil {
		t.Fatalf("expected retry to repair daily bar: %v", err)
	}
}

func TestDailyAuditRunsWithObserverDisabled(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryHistoricalStore()
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	base := timeframe.FloorStartMS(now.Add(-24*time.Hour).UnixMilli(), "1D")
	if err := store.UpsertSymbols(ctx, []market.SymbolInfo{{Exchange: "binance", Symbol: "BTCUSDT", MarketType: "um_futures", IsActive: true, LastSeenAtMS: now.UnixMilli(), FirstSeenAtMS: base}}); err != nil {
		t.Fatal(err)
	}
	fetcher := &timeframeFetcher{name: "binance", marketType: "um_futures", bars: []market.Bar{dailyTestBar(base, 100, 110, 99, 105)}}
	repairer := &storeRepairer{store: store}
	g := New(store, repairer, []Fetcher{fetcher}, Config{Enabled: true, DailyAuditEnabled: true, ObserveFinalBars: false, DailyAuditDays: 2})
	g.now = func() time.Time { return now }
	if err := g.DailyAuditOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if len(repairer.bars) == 0 || g.cfg.ObserveFinalBars {
		t.Fatalf("expected daily repair with observer disabled, bars=%d cfg=%+v", len(repairer.bars), g.cfg)
	}
	if err := g.ObserveFinalBar(ctx, testBar(base, 100, 101, 99, 100, 1)); err != nil {
		t.Fatal(err)
	}
	if got := len(g.finalBars); got != 0 {
		t.Fatalf("observer disabled should not enqueue final bar, got %d", got)
	}
}

func TestDailyAuditWorkerRunsImmediatelyOnStartup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := storage.NewMemoryHistoricalStore()
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	base := timeframe.FloorStartMS(now.Add(-24*time.Hour).UnixMilli(), "1D")
	if err := store.UpsertSymbols(ctx, []market.SymbolInfo{{Exchange: "binance", Symbol: "BTCUSDT", MarketType: "um_futures", IsActive: true, LastSeenAtMS: now.UnixMilli(), FirstSeenAtMS: base}}); err != nil {
		t.Fatal(err)
	}
	fetcher := &timeframeFetcher{name: "binance", marketType: "um_futures", bars: []market.Bar{dailyTestBar(base, 100, 110, 99, 105)}, called: make(chan struct{}, 1)}
	g := New(store, &storeRepairer{store: store}, []Fetcher{fetcher}, Config{
		Enabled: true, DailyAuditEnabled: true, DailyAuditInterval: time.Hour,
		DailyAuditDays: 1, DailyAuditLookbackDays: 1, ObserveFinalBars: false,
	})
	g.now = func() time.Time { return now }
	runDone := make(chan error, 1)
	go func() { runDone <- g.Run(ctx) }()
	select {
	case <-fetcher.called:
	case <-time.After(time.Second):
		t.Fatal("daily audit did not fetch bars during startup")
	}
	cancel()
	select {
	case <-runDone:
	case <-time.After(time.Second):
		t.Fatal("guardian workers did not stop after cancellation")
	}
	if len(g.finalBars) != 0 {
		t.Fatal("final-bar worker must stay disabled when ObserveFinalBars is false")
	}
}

func TestGuardianBoundsConcurrentAuditsPerExchange(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryHistoricalStore()
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	symbols := make([]market.SymbolInfo, 10)
	for i := range symbols {
		symbols[i] = market.SymbolInfo{
			Exchange: "binance", Symbol: fmt.Sprintf("S%02d", i), MarketType: "um_futures",
			Status: "TRADING", IsActive: true, LastSeenAtMS: now.UnixMilli(),
		}
	}
	if err := store.UpsertSymbols(ctx, symbols); err != nil {
		t.Fatal(err)
	}
	fetcher := &inflightTrackingFetcher{delay: 20 * time.Millisecond}
	g := New(store, &storeRepairer{store: store}, []Fetcher{fetcher}, Config{
		Enabled: true, DailyAuditEnabled: true, DailyAuditDays: 1,
		DailyAuditLookbackDays: 1, MaxInflightPerExchange: 3,
	})
	g.now = func() time.Time { return now }

	errCh := make(chan error, 2)
	go func() { errCh <- g.AuditOnce(ctx) }()
	go func() { errCh <- g.DailyAuditOnce(ctx) }()
	for range 2 {
		if err := <-errCh; err != nil {
			t.Fatalf("parallel guardian audit failed: %v", err)
		}
	}
	if got := fetcher.max.Load(); got > 3 {
		t.Fatalf("exchange REST concurrency exceeded configured limit: got=%d limit=3", got)
	}
	if got := fetcher.max.Load(); got < 2 {
		t.Fatalf("expected concurrent REST requests, observed max=%d", got)
	}
}

func TestRequestPacerKeepsConcurrentRequestsSpaced(t *testing.T) {
	const requestCount = 5
	pacer := &requestPacer{interval: 20 * time.Millisecond}
	starts := make(chan time.Time, requestCount)
	var wg sync.WaitGroup
	for range requestCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := pacer.Wait(context.Background()); err != nil {
				t.Errorf("wait for request slot: %v", err)
				return
			}
			starts <- time.Now()
		}()
	}
	wg.Wait()
	close(starts)
	var observed []time.Time
	for started := range starts {
		observed = append(observed, started)
	}
	sort.Slice(observed, func(i, j int) bool { return observed[i].Before(observed[j]) })
	for i := 1; i < len(observed); i++ {
		if gap := observed[i].Sub(observed[i-1]); gap < 15*time.Millisecond {
			t.Fatalf("concurrent request starts were too close: delta=%s starts=%v", gap, observed)
		}
	}
}

func TestRequestPacingTransportLimitsEachHTTPAttempt(t *testing.T) {
	const requestCount = 3
	base := &recordingRoundTripper{starts: make(chan time.Time, requestCount)}
	client := &http.Client{Transport: requestPacingTransport{
		base: base,
		pacers: map[string]*requestPacer{
			"binance": {interval: 15 * time.Millisecond},
		},
	}}
	ctx := context.WithValue(context.Background(), guardianExchangeContextKey{}, "binance")
	for range requestCount {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://example.test", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
	}
	close(base.starts)
	var starts []time.Time
	for started := range base.starts {
		starts = append(starts, started)
	}
	for i := 1; i < len(starts); i++ {
		if gap := starts[i].Sub(starts[i-1]); gap < 12*time.Millisecond {
			t.Fatalf("physical HTTP attempts were not paced: delta=%s starts=%v", gap, starts)
		}
	}
}

type fakeFetcher struct {
	name        string
	marketType  string
	bars        []market.Bar
	lastRequest exchange.KlineRequest
}

type countingFetcher struct {
	name       string
	marketType string
	count      atomic.Int64
}

type timeframeFetcher struct {
	name       string
	marketType string
	bars       []market.Bar
	err        error
	called     chan struct{}
}

type inflightTrackingFetcher struct {
	active atomic.Int32
	max    atomic.Int32
	delay  time.Duration
}

type recordingRoundTripper struct {
	starts chan time.Time
}

func (r *recordingRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	r.starts <- time.Now()
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: http.NoBody, Request: request}, nil
}

func (f *inflightTrackingFetcher) Name() string       { return "binance" }
func (f *inflightTrackingFetcher) MarketType() string { return "um_futures" }
func (f *inflightTrackingFetcher) FetchKlines(_ context.Context, _ *http.Client, _ exchange.KlineRequest) ([]market.Bar, error) {
	active := f.active.Add(1)
	for current := f.max.Load(); active > current && !f.max.CompareAndSwap(current, active); current = f.max.Load() {
	}
	defer f.active.Add(-1)
	time.Sleep(f.delay)
	return nil, nil
}

func (f *timeframeFetcher) Name() string       { return f.name }
func (f *timeframeFetcher) MarketType() string { return f.marketType }
func (f *timeframeFetcher) FetchKlines(_ context.Context, _ *http.Client, request exchange.KlineRequest) ([]market.Bar, error) {
	if f.called != nil {
		select {
		case f.called <- struct{}{}:
		default:
		}
	}
	if f.err != nil {
		return nil, f.err
	}
	var out []market.Bar
	for _, bar := range f.bars {
		if bar.Symbol != request.Symbol || bar.Timeframe != request.Timeframe || bar.StartMS < request.StartMS || bar.StartMS > request.EndMS {
			continue
		}
		out = append(out, bar)
	}
	return out, nil
}

func (f *countingFetcher) Name() string       { return f.name }
func (f *countingFetcher) MarketType() string { return f.marketType }
func (f *countingFetcher) FetchKlines(_ context.Context, _ *http.Client, _ exchange.KlineRequest) ([]market.Bar, error) {
	f.count.Add(1)
	return nil, nil
}

func (f *fakeFetcher) Name() string {
	if f.name != "" {
		return f.name
	}
	return "binance"
}

func (f *fakeFetcher) MarketType() string {
	if f.marketType != "" {
		return f.marketType
	}
	return "um_futures"
}

func (f *fakeFetcher) FetchKlines(_ context.Context, _ *http.Client, request exchange.KlineRequest) ([]market.Bar, error) {
	f.lastRequest = request
	var out []market.Bar
	for _, bar := range f.bars {
		if bar.Symbol != request.Symbol || bar.Timeframe != request.Timeframe {
			continue
		}
		if request.StartMS > 0 && bar.StartMS < request.StartMS {
			continue
		}
		if request.EndMS > 0 && bar.StartMS > request.EndMS {
			continue
		}
		out = append(out, bar)
	}
	return out, nil
}

func testBarFor(exchangeName string, symbol string, startMS int64, closePrice float64) market.Bar {
	bar := testBar(startMS, closePrice, closePrice, closePrice, closePrice, 1)
	bar.Exchange = exchangeName
	bar.Symbol = symbol
	return market.DecorateBar(bar)
}

type storeRepairer struct {
	store *storage.MemoryHistoricalStore
	bars  []market.Bar
}

func (r *storeRepairer) RepairFinalBars(ctx context.Context, bars []market.Bar) error {
	r.bars = append(r.bars, bars...)
	return r.store.UpsertBars(ctx, bars)
}

func upsertActiveSymbol(t *testing.T, store *storage.MemoryHistoricalStore) {
	t.Helper()
	if err := store.UpsertSymbols(context.Background(), []market.SymbolInfo{{
		Exchange: "binance", Symbol: "BTCUSDT", MarketType: "um_futures", Status: "TRADING", IsActive: true,
	}}); err != nil {
		t.Fatalf("upsert symbol: %v", err)
	}
}

func testBar(startMS int64, open float64, high float64, low float64, closePrice float64, volume float64) market.Bar {
	return market.DecorateBar(market.Bar{
		Exchange: "binance", Symbol: "BTCUSDT", MarginType: "umargin", Timeframe: "1m",
		StartMS: startMS, EndMS: startMS + 59_999,
		OpenPrice: open, HighPrice: high, LowPrice: low, ClosePrice: closePrice,
		Volume: volume, QuoteVolume: volume * closePrice, TradeCount: int64(volume),
		LastTickMS: startMS + 59_999, IsFinal: true, Source: "rest", Reason: "test",
	})
}

func dailyTestBar(startMS int64, open float64, high float64, low float64, closePrice float64) market.Bar {
	return market.DecorateBar(market.Bar{
		Exchange: "binance", Symbol: "BTCUSDT", MarginType: "umargin", Timeframe: "1D",
		StartMS: startMS, EndMS: timeframe.EndMS(startMS, "1D"),
		OpenPrice: open, HighPrice: high, LowPrice: low, ClosePrice: closePrice,
		Volume: 1, QuoteVolume: closePrice, TradeCount: 1,
		LastTickMS: timeframe.EndMS(startMS, "1D"), IsFinal: true, Source: "rest", Reason: "test",
	})
}
