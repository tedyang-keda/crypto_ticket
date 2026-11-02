package guardian

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"crypto-ticket/internal/exchange"
	"crypto-ticket/internal/market"
	"crypto-ticket/internal/storage"
	"crypto-ticket/internal/timeframe"
)

// dailyListingFetcher records request boundaries without changing the existing
// shared fetcher fixtures used by the other Guardian tests.
type dailyListingFetcher struct {
	*timeframeFetcher
	mu       sync.Mutex
	requests []exchange.KlineRequest
}

func (f *dailyListingFetcher) FetchKlines(ctx context.Context, client *http.Client, request exchange.KlineRequest) ([]market.Bar, error) {
	f.mu.Lock()
	f.requests = append(f.requests, request)
	f.mu.Unlock()
	return f.timeframeFetcher.FetchKlines(ctx, client, request)
}

func (f *dailyListingFetcher) recordedRequests() []exchange.KlineRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]exchange.KlineRequest(nil), f.requests...)
}

func TestDailyListingTargetsNeverUseFirstSeenAsOfficialBoundary(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	listed := now.AddDate(0, -3, 0).UnixMilli()
	for _, test := range []struct {
		name      string
		firstSeen int64
		listedAt  int64
	}{
		{name: "observed_after_listing", firstSeen: now.Add(-time.Hour).UnixMilli(), listedAt: listed},
		{name: "observed_before_corrected_listing", firstSeen: now.AddDate(0, -6, 0).UnixMilli(), listedAt: listed},
		{name: "observation_without_official_listing", firstSeen: listed},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := storage.NewMemoryHistoricalStore()
			if err := store.UpsertSymbols(ctx, []market.SymbolInfo{{
				Exchange: "binance", Symbol: "BTCUSDT", MarketType: "um_futures", IsActive: true,
				FirstSeenAtMS: test.firstSeen, ListedAtMS: test.listedAt, LastSeenAtMS: now.UnixMilli(),
			}}); err != nil {
				t.Fatal(err)
			}
			g := New(store, &storeRepairer{store: store}, []Fetcher{&fakeFetcher{}}, Config{Enabled: true})
			g.now = func() time.Time { return now }
			targets, err := g.dailyAuditTargets(ctx)
			if err != nil || len(targets) != 1 {
				t.Fatalf("daily targets = %+v, err = %v", targets, err)
			}
			if targets[0].ListedAtMS != test.listedAt {
				t.Fatalf("expected official listing %d, got %d (first_seen=%d)", test.listedAt, targets[0].ListedAtMS, test.firstSeen)
			}
		})
	}
}

func TestDailyListingTargetsPreferFreshOfficialSnapshot(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	registryListing := now.AddDate(0, -6, 0).UnixMilli()
	snapshotListing := now.AddDate(0, -2, 0).Add(7 * time.Hour).UnixMilli()
	store := &eligibilityStore{MemoryHistoricalStore: storage.NewMemoryHistoricalStore()}
	if err := store.UpsertSymbols(ctx, []market.SymbolInfo{{
		Exchange: "binance", Symbol: "BTCUSDT", MarketType: "um_futures", IsActive: true,
		FirstSeenAtMS: now.Add(-time.Hour).UnixMilli(), ListedAtMS: registryListing, LastSeenAtMS: now.UnixMilli(),
	}}); err != nil {
		t.Fatal(err)
	}
	store.snapshots = []market.SymbolSnapshot{{
		Exchange: "binance", Symbol: "BTCUSDT", MarketType: "um_futures", Presence: "present",
		Subscribed: true, OfficialActive: true, ListedAtMS: snapshotListing, CapturedAtMS: now.UnixMilli(),
	}}
	store.syncs = []market.SymbolSnapshotSync{{Exchange: "binance", MarketType: "um_futures", Status: "ok", LastSuccessMS: now.UnixMilli()}}
	g := New(store, &storeRepairer{store: store.MemoryHistoricalStore}, []Fetcher{&fakeFetcher{}}, Config{Enabled: true, RequireSymbolSnapshots: true})
	g.now = func() time.Time { return now }
	targets, err := g.dailyAuditTargets(ctx)
	if err != nil || len(targets) != 1 || targets[0].ListedAtMS != snapshotListing {
		t.Fatalf("fresh snapshot listing must win over registry/first_seen: targets=%+v err=%v", targets, err)
	}
}

func TestDailyListingPrelistingRangeSkipsRESTAndClearsOldRetry(t *testing.T) {
	ctx := context.Background()
	day := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC).UnixMilli()
	dayMS := timeframe.DurationMS("1D")
	for _, forceReplay := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal", true: "forced_retry"}[forceReplay], func(t *testing.T) {
			store := storage.NewMemoryHistoricalStore()
			if err := store.UpsertKlineGuardianState(ctx, market.KlineGuardianState{
				Exchange: "binance", Symbol: "BTCUSDT", Timeframe: "1D", Status: "repair_error",
				LastCheckedStartMS: day, LastCheckedEndMS: day + dayMS,
			}); err != nil {
				t.Fatal(err)
			}
			fetcher := &dailyListingFetcher{timeframeFetcher: &timeframeFetcher{name: "binance", marketType: "um_futures"}}
			repairer := &storeRepairer{store: store}
			g := New(store, repairer, []Fetcher{fetcher}, Config{Enabled: true})
			_, err := g.repairTimeframeRange(ctx, fetcher, timeframeRepairRequest{
				Exchange: "binance", Symbol: "BTCUSDT", Timeframe: "1D", Reason: "daily_test",
				StartMS: day, EndMS: day + dayMS, ExpectedStartMS: day + 2*dayMS + 12*timeframe.DurationMS("1H"),
				ForceReplay: forceReplay,
			})
			if err != nil {
				t.Fatalf("confirmed prelisting interval should be excluded: %v", err)
			}
			if requests := fetcher.recordedRequests(); len(requests) != 0 || len(repairer.bars) != 0 {
				t.Fatalf("prelisting exclusion must not request or write bars: requests=%+v writes=%d", requests, len(repairer.bars))
			}
			state, err := store.LoadKlineGuardianState(ctx, "binance", "BTCUSDT", "1D")
			if err != nil || state == nil || state.Status != "not_listed" || state.LastCheckedStartMS != day || state.LastCheckedEndMS != day+dayMS {
				t.Fatalf("prelisting exclusion must replace its stale retry over the original interval: state=%+v err=%v", state, err)
			}
		})
	}
}

func TestDailyListingCrossingRangeClipsRESTButRetainsOriginalAuditRange(t *testing.T) {
	ctx := context.Background()
	day := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC).UnixMilli()
	dayMS := timeframe.DurationMS("1D")
	listedDay := day + 2*dayMS
	end := listedDay + dayMS
	store := storage.NewMemoryHistoricalStore()
	if err := store.UpsertKlineGuardianState(ctx, market.KlineGuardianState{
		Exchange: "binance", Symbol: "BTCUSDT", Timeframe: "1D", Status: "repair_error",
		LastCheckedStartMS: day, LastCheckedEndMS: end,
	}); err != nil {
		t.Fatal(err)
	}
	// An old erroneous prelisting record must not force an official candle to
	// exist before the exchange's authoritative listing boundary.
	if err := store.UpsertBars(ctx, []market.Bar{dailyTestBar(day, 100, 101, 99, 100)}); err != nil {
		t.Fatal(err)
	}
	fetcher := &dailyListingFetcher{timeframeFetcher: &timeframeFetcher{name: "binance", marketType: "um_futures", bars: []market.Bar{
		dailyTestBar(listedDay, 100, 101, 99, 100), dailyTestBar(end, 100, 102, 99, 101),
	}}}
	g := New(store, &storeRepairer{store: store}, []Fetcher{fetcher}, Config{Enabled: true})
	result, err := g.repairTimeframeRange(ctx, fetcher, timeframeRepairRequest{
		Exchange: "binance", Symbol: "BTCUSDT", Timeframe: "1D", Reason: "daily_test", ExpectData: true,
		StartMS: day, EndMS: end, ExpectedStartMS: listedDay + 13*timeframe.DurationMS("1H"), ForceReplay: true,
	})
	if err != nil || result.Repaired != 2 {
		t.Fatalf("crossing listing boundary should verify only the listed UTC days: result=%+v err=%v", result, err)
	}
	requests := fetcher.recordedRequests()
	if len(requests) != 1 || requests[0].Timeframe != "1D" || requests[0].StartMS != listedDay || requests[0].EndMS != end || requests[0].Limit != 2 {
		t.Fatalf("official daily request was not clipped to listing UTC day: %+v", requests)
	}
	state, err := store.LoadKlineGuardianState(ctx, "binance", "BTCUSDT", "1D")
	if err != nil || state == nil || isRetryableDailyStatus(state.Status) || state.LastCheckedStartMS != day || state.LastCheckedEndMS != end {
		t.Fatalf("successful clipped request must clear original crossing retry: state=%+v err=%v", state, err)
	}
	oldBars, err := store.BarsInRange(ctx, "binance", "BTCUSDT", "1D", day, day)
	if err != nil || len(oldBars) != 1 {
		t.Fatalf("boundary exclusion must not delete historical records: bars=%+v err=%v", oldBars, err)
	}
}

func TestDailyListingPartialListingDayStillRequiresVerification(t *testing.T) {
	ctx := context.Background()
	day := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC).UnixMilli()
	for _, available := range []bool{false, true} {
		t.Run(map[bool]string{false: "absent", true: "present"}[available], func(t *testing.T) {
			store := storage.NewMemoryHistoricalStore()
			fetcher := &dailyListingFetcher{timeframeFetcher: &timeframeFetcher{name: "binance", marketType: "um_futures"}}
			if available {
				fetcher.bars = []market.Bar{dailyTestBar(day, 100, 101, 99, 100)}
			}
			g := New(store, &storeRepairer{store: store}, []Fetcher{fetcher}, Config{Enabled: true})
			_, err := g.repairTimeframeRange(ctx, fetcher, timeframeRepairRequest{
				Exchange: "binance", Symbol: "BTCUSDT", Timeframe: "1D", Reason: "daily_test", ExpectData: true,
				StartMS: day, EndMS: day, ExpectedStartMS: day + 12*timeframe.DurationMS("1H"),
			})
			if available && err != nil || !available && err == nil {
				t.Fatalf("partial listing day must be checked, available=%v err=%v", available, err)
			}
			requests := fetcher.recordedRequests()
			if len(requests) == 0 || requests[0].Timeframe != "1D" || requests[0].StartMS != day || requests[0].EndMS != day {
				t.Fatalf("partial listing day was skipped or shifted: %+v", requests)
			}
		})
	}
}

func TestDailyListingMissingMetadataCannotMakeEmptyHistoryHealthy(t *testing.T) {
	ctx := context.Background()
	day := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC).UnixMilli()
	store := storage.NewMemoryHistoricalStore()
	fetcher := &dailyListingFetcher{timeframeFetcher: &timeframeFetcher{name: "binance", marketType: "um_futures"}}
	g := New(store, &storeRepairer{store: store}, []Fetcher{fetcher}, Config{Enabled: true})
	_, err := g.repairTimeframeRange(ctx, fetcher, timeframeRepairRequest{
		Exchange: "binance", Symbol: "BTCUSDT", Timeframe: "1D", StartMS: day, EndMS: day, Reason: "daily_test",
	})
	if err == nil {
		t.Fatal("empty official history without listing/no-trade evidence must remain retryable")
	}
	state, stateErr := store.LoadKlineGuardianState(ctx, "binance", "BTCUSDT", "1D")
	if stateErr != nil || state == nil || !isRetryableDailyStatus(state.Status) {
		t.Fatalf("unknown listing metadata must not establish healthy/no-data status: state=%+v err=%v", state, stateErr)
	}
}

func TestDailyListingAuditClearsPrelistingRetryQueueWithoutREST(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	today := timeframe.FloorStartMS(now.UnixMilli(), "1D")
	dayMS := timeframe.DurationMS("1D")
	old := dailyAuditRange{StartMS: today - 12*dayMS, EndMS: today - 10*dayMS}
	store := storage.NewMemoryHistoricalStore()
	if err := store.UpsertSymbols(ctx, []market.SymbolInfo{{
		Exchange: "binance", Symbol: "BTCUSDT", MarketType: "um_futures", IsActive: true,
		ListedAtMS: today + timeframe.DurationMS("1H"), LastSeenAtMS: now.UnixMilli(),
	}}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertKlineGuardianState(ctx, market.KlineGuardianState{
		Exchange: "binance", Symbol: "BTCUSDT", Timeframe: "1D", Status: "repair_error",
		LastCheckedStartMS: old.StartMS, LastCheckedEndMS: old.EndMS,
	}); err != nil {
		t.Fatal(err)
	}
	fetcher := &dailyListingFetcher{timeframeFetcher: &timeframeFetcher{name: "binance", marketType: "um_futures"}}
	g := New(store, &storeRepairer{store: store}, []Fetcher{fetcher}, Config{Enabled: true, DailyAuditDays: 2, DailyAuditLookbackDays: 4})
	g.now = func() time.Time { return now }
	g.dailyRetries["binance:BTCUSDT"] = map[dailyAuditRange]bool{old: true}
	if err := g.DailyAuditOnce(ctx); err != nil {
		t.Fatalf("prelisting daily audit should resolve every queued interval: %v", err)
	}
	if requests := fetcher.recordedRequests(); len(requests) != 0 {
		t.Fatalf("all audited dates precede official listing and need no REST: %+v", requests)
	}
	if pending := g.dailyRetries["binance:BTCUSDT"]; len(pending) != 0 {
		t.Fatalf("confirmed prelisting retry remains queued: %+v", pending)
	}
	state, err := store.LoadKlineGuardianState(ctx, "binance", "BTCUSDT", "1D")
	if err != nil || state == nil || state.Status != "not_listed" || state.LastCheckedStartMS != old.StartMS || state.LastCheckedEndMS != old.EndMS {
		t.Fatalf("persisted retry was not resolved over its original range: state=%+v err=%v", state, err)
	}
}
