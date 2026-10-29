package storage

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crypto-ticket/internal/market"
)

func minuteTaskForTest(start int64, kind string) market.MinuteAuditTask {
	return market.MinuteAuditTask{
		Exchange: "BINANCE", Symbol: "btcusdt", MarketType: "um_futures", Kind: kind,
		StartMS: start, EndMS: start + 29*60_000, ExpectData: true,
		CreatedAtMS: start + 31*60_000,
	}
}

func TestMinuteAuditIndependentRangesAndRetryBackoff(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryHistoricalStore()
	start := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC).UnixMilli()
	now := start + 31*60_000
	first := minuteTaskForTest(start, "window")
	id1, err := store.EnqueueMinuteAuditTask(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	id2, err := store.EnqueueMinuteAuditTask(ctx, minuteTaskForTest(start+60_000, "window"))
	if err != nil || id2 == id1 {
		t.Fatalf("second failed range was lost: id=%d err=%v", id2, err)
	}
	leased, err := store.LeaseMinuteAuditTask(ctx, id1, "attempt-one", now, now+60_000)
	if err != nil || !leased {
		t.Fatalf("lease: %t %v", leased, err)
	}
	if err := store.FinishMinuteAuditTask(ctx, id1, "attempt-one", "retry", now+10*60_000, true, "official response incomplete", now+1); err != nil {
		t.Fatal(err)
	}
	duplicate, err := store.EnqueueMinuteAuditTask(ctx, first)
	if err != nil || duplicate != id1 {
		t.Fatalf("scheduler did not deduplicate range: id=%d err=%v", duplicate, err)
	}
	if ok, err := store.LeaseMinuteAuditTask(ctx, id1, "too-early", now+2, now+60_000); err != nil || ok {
		t.Fatalf("duplicate enqueue reset retry backoff: %t %v", ok, err)
	}
	persisted, err := store.LoadMinuteAuditTask(ctx, id1)
	if err != nil || persisted == nil || persisted.Attempts != 1 || !persisted.ForceReplay || persisted.Status != "retry" {
		t.Fatalf("direct runner cannot recover persisted retry metadata: %+v %v", persisted, err)
	}
	due, err := store.ListDueMinuteAuditTasks(ctx, "binance", now+10*60_000, 10)
	if err != nil || len(due) != 2 {
		t.Fatalf("independent retry ranges missing: %+v %v", due, err)
	}
	for _, task := range due {
		if task.ID == id1 && (!task.ForceReplay || task.Attempts != 1 || task.LastError == "") {
			t.Fatalf("duplicate enqueue erased retry evidence: %+v", task)
		}
	}
}

func TestMinuteAuditWorkGroupFilteringPrecedesLimit(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryHistoricalStore()
	start := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC).UnixMilli()
	now := start + 31*60_000
	for i := 0; i < 5; i++ {
		if _, err := store.EnqueueMinuteAuditTask(ctx, minuteTaskForTest(start+int64(i)*60_000, "window")); err != nil {
			t.Fatal(err)
		}
	}
	daily := minuteTaskForTest(start, "daily")
	dailyID, err := store.EnqueueMinuteAuditTask(ctx, daily)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := store.ListDueMinuteAuditTasksByKind(ctx, "binance", "daily", now+10*60_000, 1)
	if err != nil || len(tasks) != 1 || tasks[0].ID != dailyID {
		t.Fatalf("rolling backlog hid daily work behind the limit: %+v %v", tasks, err)
	}
	tasks, err = store.ListDueMinuteAuditTasksByKind(ctx, "binance", "non_daily", now+10*60_000, 1)
	if err != nil || len(tasks) != 1 || tasks[0].Kind == "daily" {
		t.Fatalf("non-daily worker received daily task: %+v %v", tasks, err)
	}
}

func TestMinuteAuditLeaseReclaimRejectsOldAcknowledgement(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryHistoricalStore()
	start := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC).UnixMilli()
	now := start + 31*60_000
	id, err := store.EnqueueMinuteAuditTask(ctx, minuteTaskForTest(start, "window"))
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := store.LeaseMinuteAuditTask(ctx, id, "old", now, now+1000); err != nil || !ok {
		t.Fatalf("first claim: %t %v", ok, err)
	}
	if ok, err := store.LeaseMinuteAuditTask(ctx, id, "new", now+1001, now+5000); err != nil || !ok {
		t.Fatalf("crashed worker could not be recovered: %t %v", ok, err)
	}
	if err := store.FinishMinuteAuditTask(ctx, id, "old", "completed", 0, false, "", now+1002); !errors.Is(err, ErrMinuteAuditLeaseLost) {
		t.Fatalf("stale acknowledgement accepted: %v", err)
	}
	if err := store.FinishMinuteAuditTask(ctx, id, "new", "completed", 0, false, "", now+1003); err != nil {
		t.Fatal(err)
	}
	if err := store.FinishMinuteAuditTask(ctx, id, "new", "completed", 0, false, "", now+1004); err != nil {
		t.Fatalf("completion is not idempotent: %v", err)
	}
	summary, err := store.MinuteAuditSummary(ctx, now+1004)
	if err != nil || summary.Pending != 0 || summary.Running != 0 {
		t.Fatalf("completed rolling task retained as backlog: %+v %v", summary, err)
	}
}

func TestMinuteAuditOnlyOneConcurrentLease(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryHistoricalStore()
	start := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC).UnixMilli()
	now := start + 31*60_000
	id, err := store.EnqueueMinuteAuditTask(ctx, minuteTaskForTest(start, "window"))
	if err != nil {
		t.Fatal(err)
	}
	var claims atomic.Int32
	var wait sync.WaitGroup
	for i := 0; i < 32; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if ok, err := store.LeaseMinuteAuditTask(ctx, id, "worker", now, now+1000); err != nil {
				t.Error(err)
			} else if ok {
				claims.Add(1)
			}
		}()
	}
	wait.Wait()
	if claims.Load() != 1 {
		t.Fatalf("%d workers own the same range", claims.Load())
	}
}

func TestMinuteAuditCursorAndDailyCoverageAreSeparate(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryHistoricalStore()
	start := time.Date(2026, 10, 28, 0, 0, 0, 0, time.UTC).UnixMilli()
	now := start + 25*60*60_000
	task := minuteTaskForTest(start, "daily")
	task.EndMS = start + 1439*60_000
	id, err := store.EnqueueMinuteAuditTask(ctx, task)
	if err != nil {
		t.Fatal(err)
	}
	cursor := market.MinuteAuditCursor{Exchange: "BINANCE", Symbol: "btcusdt", Kind: "daily", ThroughMS: task.EndMS, UpdatedAtMS: now}
	if err := store.SaveMinuteAuditCursor(ctx, cursor); err != nil {
		t.Fatal(err)
	}
	summary, err := store.MinuteAuditSummary(ctx, now)
	if err != nil || summary.DailyCompleted != 0 || summary.DailyTotal != 1 || summary.Pending != 1 {
		t.Fatalf("registered cursor falsely counted verified: %+v %v", summary, err)
	}
	if ok, err := store.LeaseMinuteAuditTask(ctx, id, "daily", now, now+60_000); err != nil || !ok {
		t.Fatalf("claim daily: %t %v", ok, err)
	}
	if err := store.FinishMinuteAuditTask(ctx, id, "daily", "completed", 0, false, "", now+1); err != nil {
		t.Fatal(err)
	}
	summary, err = store.MinuteAuditSummary(ctx, now+2)
	if err != nil || summary.DailyCompleted != 1 || summary.Pending != 0 || summary.DailyDayStartMS != start {
		t.Fatalf("missing daily completion evidence: %+v %v", summary, err)
	}
	cursor.ThroughMS, cursor.UpdatedAtMS = start, now-100
	if err := store.SaveMinuteAuditCursor(ctx, cursor); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadMinuteAuditCursor(ctx, "binance", "BTCUSDT", "daily")
	if err != nil || loaded == nil || loaded.ThroughMS != task.EndMS || loaded.UpdatedAtMS != now {
		t.Fatalf("older scheduler regressed cursor: %+v %v", loaded, err)
	}
}

func TestMinuteAuditTerminalRetentionWithoutDailyAudits(t *testing.T) {
	for _, outcome := range []string{"excluded", "expired"} {
		t.Run(outcome, func(t *testing.T) {
			ctx := context.Background()
			store := NewMemoryHistoricalStore()
			start := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC).UnixMilli()
			oldNow := start + 31*60_000
			oldTask := minuteTaskForTest(start, "window")
			oldID, err := store.EnqueueMinuteAuditTask(ctx, oldTask)
			if err != nil {
				t.Fatal(err)
			}
			if ok, err := store.LeaseMinuteAuditTask(ctx, oldID, "old", oldNow, oldNow+60_000); err != nil || !ok {
				t.Fatalf("claim old task: %t %v", ok, err)
			}
			if err := store.FinishMinuteAuditTask(ctx, oldID, "old", outcome, 0, false, "officially inactive", oldNow+1); err != nil {
				t.Fatal(err)
			}
			pendingTask := minuteTaskForTest(start+60_000, "window")
			pendingID, err := store.EnqueueMinuteAuditTask(ctx, pendingTask)
			if err != nil {
				t.Fatal(err)
			}
			now := oldNow + int64(8*24*time.Hour/time.Millisecond)
			recentTask := minuteTaskForTest(start+int64(8*24*time.Hour/time.Millisecond), "window")
			recentID, err := store.EnqueueMinuteAuditTask(ctx, recentTask)
			if err != nil {
				t.Fatal(err)
			}
			if ok, err := store.LeaseMinuteAuditTask(ctx, recentID, "recent", now, now+60_000); err != nil || !ok {
				t.Fatalf("claim recent task: %t %v", ok, err)
			}
			if err := store.FinishMinuteAuditTask(ctx, recentID, "recent", outcome, 0, false, "officially inactive", now+1); err != nil {
				t.Fatal(err)
			}
			if old, err := store.LoadMinuteAuditTask(ctx, oldID); err != nil || old != nil {
				t.Fatalf("old terminal evidence was not pruned: %+v %v", old, err)
			}
			if pending, err := store.LoadMinuteAuditTask(ctx, pendingID); err != nil || pending == nil || pending.Status != "pending" {
				t.Fatalf("retention removed unfinished work: %+v %v", pending, err)
			}
			if recent, err := store.LoadMinuteAuditTask(ctx, recentID); err != nil || recent == nil || recent.Status != outcome {
				t.Fatalf("retention removed recent evidence: %+v %v", recent, err)
			}
		})
	}
}
