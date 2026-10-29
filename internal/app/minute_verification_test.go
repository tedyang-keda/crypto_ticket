package app

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"crypto-ticket/internal/market"
	"crypto-ticket/internal/realtime"
	"crypto-ticket/internal/storage"
	"crypto-ticket/internal/timeframe"
)

func verificationTestMinute(startMS int64, closePrice float64) market.Bar {
	return market.Bar{Exchange: "binance", Symbol: "BTCUSDT", Timeframe: "1m", StartMS: startMS,
		EndMS: startMS + 59_999, OpenPrice: 100, HighPrice: closePrice, LowPrice: 99, ClosePrice: closePrice,
		Volume: 1, QuoteVolume: 100, TradeCount: 3, IsFinal: true, LastTickMS: startMS + 59_999}
}

type countedMinuteStore struct {
	*storage.MemoryHistoricalStore
	revisions int
	marks     int
}

func (s *countedMinuteStore) UpsertMinuteRevisions(ctx context.Context, bars []market.Bar, generationMS int64) ([]market.Bar, error) {
	s.revisions += len(bars)
	return s.MemoryHistoricalStore.UpsertMinuteRevisions(ctx, bars, generationMS)
}

func (s *countedMinuteStore) MarkMinutesVerified(ctx context.Context, bars []market.Bar, generationMS int64) error {
	s.marks += len(bars)
	return s.MemoryHistoricalStore.MarkMinutesVerified(ctx, bars, generationMS)
}

func TestVerifiedMinuteRejectsLateFinalAndDurablyQueuesConflict(t *testing.T) {
	ctx := context.Background()
	store := &countedMinuteStore{MemoryHistoricalStore: storage.NewMemoryHistoricalStore()}
	service := NewMarketService(store, realtime.NewHub(), []string{"1m"}, 300)
	base := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC).UnixMilli()
	bar := verificationTestMinute(base, 101)
	if err := service.IngestKline(ctx, bar); err != nil {
		t.Fatal(err)
	}
	if err := service.VerifyFinalBars(ctx, []market.Bar{bar}, 1000); err != nil {
		t.Fatal(err)
	}
	if store.revisions != 0 || store.marks != 1 {
		t.Fatalf("unchanged minute must only receive its first marker: revisions=%d marks=%d", store.revisions, store.marks)
	}
	if err := service.VerifyFinalBars(ctx, []market.Bar{bar}, 1100); err != nil {
		t.Fatal(err)
	}
	if err := service.IngestKline(ctx, bar); err != nil {
		t.Fatal(err)
	}
	if store.revisions != 0 || store.marks != 1 {
		t.Fatal("repeated unchanged verification rewrote the minute or marker")
	}
	late := verificationTestMinute(base, 103)
	late.LastTickMS = bar.LastTickMS + 1_000_000 // WS event time is not a REST revision version.
	for i := 0; i < 2; i++ {
		if err := service.IngestKline(ctx, late); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := store.LoadMinuteVerifications(ctx, "binance", "BTCUSDT", base, base)
	if err != nil || len(rows) != 1 || rows[0].Bar.ClosePrice != 101 || rows[0].VerifiedAtMS != 1000 {
		t.Fatalf("late final replaced calibrated minute: %+v %v", rows, err)
	}
	tasks, err := store.ListDueMinuteAuditTasks(ctx, "binance", market.NowMS()+1000, 10)
	if err != nil || len(tasks) != 1 || tasks[0].Kind != "conflict" || tasks[0].StartMS != base || !tasks[0].ForceReplay {
		t.Fatalf("expected one deduplicated durable conflict: %+v %v", tasks, err)
	}
	if err := service.VerifyFinalBars(ctx, []market.Bar{late}, 1200); err != nil {
		t.Fatal(err)
	}
	// Even an older request whose response arrives after an unchanged newer
	// request cannot roll the symbol back during this process lifetime.
	if err := service.VerifyFinalBars(ctx, []market.Bar{late}, 1400); err != nil {
		t.Fatal(err)
	}
	if err := service.VerifyFinalBars(ctx, []market.Bar{bar}, 1300); err == nil {
		t.Fatal("stale official request reported success")
	}
	rows, err = store.LoadMinuteVerifications(ctx, "binance", "BTCUSDT", base, base)
	if err != nil || rows[0].Bar.ClosePrice != 103 || rows[0].VerifiedAtMS != 1200 {
		t.Fatalf("official revision or old-request gate failed: %+v %v", rows, err)
	}
}

func TestStaleVerificationOfDifferentRangeRemainsRetryable(t *testing.T) {
	ctx := context.Background()
	store := &countedMinuteStore{MemoryHistoricalStore: storage.NewMemoryHistoricalStore()}
	service := NewMarketService(store, realtime.NewHub(), []string{"1m", "15m"}, 300)
	base := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC).UnixMilli()
	newerRange := verificationTestMinute(base, 101)
	if err := service.VerifyFinalBars(ctx, []market.Bar{newerRange}, 2000); err != nil {
		t.Fatal(err)
	}
	olderRange := verificationTestMinute(base-60_000, 102)
	if err := service.VerifyFinalBars(ctx, []market.Bar{olderRange}, 1000); err == nil {
		t.Fatal("different-range stale request must fail so its durable task can retry")
	}
	if store.revisions != 1 || store.marks != 1 {
		t.Fatalf("stale different-range request wrote or marked data: revisions=%d marks=%d", store.revisions, store.marks)
	}
	rows, err := store.LoadMinuteVerifications(ctx, "binance", "BTCUSDT", olderRange.StartMS, olderRange.StartMS)
	if err != nil || len(rows) != 0 {
		t.Fatalf("stale request unexpectedly persisted source or cascaded: %+v %v", rows, err)
	}
	if err := service.VerifyFinalBars(ctx, []market.Bar{olderRange}, 3000); err != nil {
		t.Fatalf("fresh retry of the earlier range failed: %v", err)
	}
}

func TestRESTSourceAloneDoesNotCalibrateMinute(t *testing.T) {
	ctx := context.Background()
	service := newTestMarketService()
	base := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC).UnixMilli()
	bar := verificationTestMinute(base, 101)
	bar.Source = "rest"
	if err := service.RepairFinalBars(ctx, []market.Bar{bar}); err != nil {
		t.Fatal(err)
	}
	updated := verificationTestMinute(base, 102)
	if err := service.IngestKline(ctx, updated); err != nil {
		t.Fatal(err)
	}
	store := service.store.(storage.MinuteVerificationStore)
	rows, err := store.LoadMinuteVerifications(ctx, "binance", "BTCUSDT", base, base)
	if err != nil || len(rows) != 1 || rows[0].VerifiedAtMS != 0 || rows[0].Bar.ClosePrice != 102 {
		t.Fatalf("Source=rest incorrectly granted verified status: %+v %v", rows, err)
	}
}

func TestVerifyFinalBarsRepairsBinanceOfficialZeroTradeCount(t *testing.T) {
	ctx := context.Background()
	service := newTestMarketService()
	base := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC).UnixMilli()
	local := verificationTestMinute(base, 101)
	if err := service.store.UpsertBars(ctx, []market.Bar{local}); err != nil {
		t.Fatal(err)
	}
	official := local
	official.TradeCount = 0
	if err := service.VerifyFinalBars(ctx, []market.Bar{official}, 1000); err != nil {
		t.Fatal(err)
	}
	rows, err := service.store.(storage.MinuteVerificationStore).LoadMinuteVerifications(ctx, local.Exchange, local.Symbol, base, base)
	if err != nil || len(rows) != 1 || rows[0].Bar.TradeCount != 0 || rows[0].VerifiedAtMS != 1000 {
		t.Fatalf("nonzero count was calibrated against official zero: %+v %v", rows, err)
	}
}

type blockedRollupSourceStore struct {
	*storage.MemoryHistoricalStore
	block int32
	read  chan struct{}
	allow chan struct{}
}

func (s *blockedRollupSourceStore) BarsInRange(ctx context.Context, exchange, symbol, tf string, startMS, endMS int64) ([]market.Bar, error) {
	rows, err := s.MemoryHistoricalStore.BarsInRange(ctx, exchange, symbol, tf, startMS, endMS)
	if tf == "1m" && endMS-startMS == 15*60_000-1 && atomic.CompareAndSwapInt32(&s.block, 1, 0) {
		close(s.read)
		select {
		case <-s.allow:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return rows, err
}

func TestSymbolLockPreventsOlderRollupOverwritingVerifiedRepair(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := &blockedRollupSourceStore{MemoryHistoricalStore: storage.NewMemoryHistoricalStore(), read: make(chan struct{}), allow: make(chan struct{})}
	service := NewMarketService(store, realtime.NewHub(), []string{"1m", "15m"}, 300)
	base := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC).UnixMilli()
	var minutes []market.Bar
	for i := 0; i < 15; i++ {
		minutes = append(minutes, verificationTestMinute(base+int64(i)*60_000, 101))
	}
	if err := store.UpsertBars(ctx, minutes); err != nil {
		t.Fatal(err)
	}
	atomic.StoreInt32(&store.block, 1)
	wsDone := make(chan error, 1)
	go func() { wsDone <- service.IngestKline(ctx, minutes[14]) }()
	select {
	case <-store.read:
	case <-ctx.Done():
		t.Fatal("WS rollup did not reach blocked source read")
	}
	repaired := verificationTestMinute(base+2*60_000, 110)
	verifyDone := make(chan error, 1)
	go func() { verifyDone <- service.VerifyFinalBars(ctx, []market.Bar{repaired}, 1000) }()
	select {
	case err := <-verifyDone:
		t.Fatalf("same-symbol repair bypassed active source-read/write lock: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(store.allow)
	for _, done := range []chan error{wsDone, verifyDone} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("symbol write chain deadlocked")
		}
	}
	rows, err := store.BarsInRange(ctx, "binance", "BTCUSDT", "15m", base, base)
	if err != nil || len(rows) != 1 || rows[0].HighPrice != 110 {
		t.Fatalf("old rollup overwrote corrected source snapshot: %+v %v", rows, err)
	}
}

func TestRepairRollupsTraversesCorrectChildrenToRepairStaleGrandchild(t *testing.T) {
	ctx := context.Background()
	service := newTestMarketServiceWithFrames([]string{"1m", "15m", "30m", "1H"})
	base := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC).UnixMilli()
	var minutes []market.Bar
	for i := 0; i < 60; i++ {
		minutes = append(minutes, verificationTestMinute(base+int64(i)*60_000, 101))
	}
	if err := service.store.UpsertBars(ctx, minutes); err != nil {
		t.Fatal(err)
	}
	if err := service.RepairRollups(ctx, minutes); err != nil {
		t.Fatal(err)
	}
	rows, err := service.store.BarsInRange(ctx, "binance", "BTCUSDT", "1H", base, base)
	if err != nil || len(rows) != 1 {
		t.Fatalf("initial cascade missing: %+v %v", rows, err)
	}
	stale := rows[0]
	stale.ClosePrice, stale.HighPrice = 130, 130
	if err := service.store.UpsertBars(ctx, []market.Bar{stale}); err != nil {
		t.Fatal(err)
	}
	if err := service.RepairRollups(ctx, minutes[:1]); err != nil {
		t.Fatal(err)
	}
	rows, err = service.store.BarsInRange(ctx, "binance", "BTCUSDT", "1H", base, timeframe.EndMS(base, "1H"))
	if err != nil || len(rows) != 1 || rows[0].ClosePrice != 101 {
		t.Fatalf("correct direct child prevented grandchild replay: %+v %v", rows, err)
	}
}
