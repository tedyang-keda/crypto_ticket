package mysql

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"

	"crypto-ticket/internal/market"
	"crypto-ticket/internal/storage"
)

// This test deliberately refuses an ordinary database. Deployment validation
// supplies a dedicated short-lived database; it never truncates shared tables.
func TestMinuteAuditMySQLIntegration(t *testing.T) {
	dsn := os.Getenv("CRYPTO_TICKET_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("dedicated MySQL test DSN is not configured")
	}
	config, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		t.Fatal("invalid dedicated MySQL test DSN")
	}
	if !strings.Contains(config.DBName, "_guardian_test_") {
		t.Fatal("refusing MySQL integration outside an isolated _guardian_test_ database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := New(dsn)
	if err != nil {
		t.Fatal("open dedicated MySQL database failed")
	}
	defer store.Close()
	for _, ddl := range []string{createMinuteAuditTaskTable, createMinuteAuditCursorTable} {
		if _, err := store.db.ExecContext(ctx, ddl); err != nil {
			t.Fatal(err)
		}
	}
	start := time.Now().UTC().Truncate(24 * time.Hour).Add(-24 * time.Hour).UnixMilli()
	now := time.Now().UnixMilli()
	task := market.MinuteAuditTask{Exchange: "BINANCE", Symbol: "INTEGRATION", MarketType: "um_futures", Kind: "window", StartMS: start, EndMS: start + 29*60_000, ExpectData: true, CreatedAtMS: now}
	id1, err := store.EnqueueMinuteAuditTask(ctx, task)
	if err != nil {
		t.Fatal(err)
	}
	if duplicate, err := store.EnqueueMinuteAuditTask(ctx, task); err != nil || duplicate != id1 {
		t.Fatalf("duplicate range is not idempotent: %d %v", duplicate, err)
	}
	task.StartMS += 60_000
	task.EndMS += 60_000
	id2, err := store.EnqueueMinuteAuditTask(ctx, task)
	if err != nil || id2 == id1 {
		t.Fatalf("second range missing: %d %v", id2, err)
	}
	// A fresh repository instance simulates process restart; pending ranges live
	// in the database rather than in a Guardian-local map.
	reopened, err := New(dsn)
	if err != nil {
		t.Fatal("reopen dedicated MySQL database failed")
	}
	defer reopened.Close()
	due, err := reopened.ListDueMinuteAuditTasks(ctx, "binance", now, 10)
	if err != nil || len(due) != 2 {
		t.Fatalf("restart lost pending work: %+v %v", due, err)
	}
	if ok, err := store.LeaseMinuteAuditTask(ctx, id1, "first", now, now+1000); err != nil || !ok {
		t.Fatalf("first claim: %t %v", ok, err)
	}
	if ok, err := reopened.LeaseMinuteAuditTask(ctx, id1, "competing", now, now+1000); err != nil || ok {
		t.Fatalf("concurrent lease accepted: %t %v", ok, err)
	}
	if ok, err := reopened.LeaseMinuteAuditTask(ctx, id1, "recovered", now+1001, now+10_000); err != nil || !ok {
		t.Fatalf("expired lease not recoverable: %t %v", ok, err)
	}
	if err := store.FinishMinuteAuditTask(ctx, id1, "first", "completed", 0, false, "", now+1002); !errors.Is(err, storage.ErrMinuteAuditLeaseLost) {
		t.Fatalf("old worker acknowledged new worker's work: %v", err)
	}
	if err := reopened.FinishMinuteAuditTask(ctx, id1, "recovered", "completed", 0, false, "", now+1003); err != nil {
		t.Fatal(err)
	}
	if err := reopened.FinishMinuteAuditTask(ctx, id1, "recovered", "completed", 0, false, "", now+1004); err != nil {
		t.Fatalf("repeated acknowledgement failed: %v", err)
	}
	if ok, err := store.LeaseMinuteAuditTask(ctx, id2, "retry", now, now+10_000); err != nil || !ok {
		t.Fatalf("second range claim: %t %v", ok, err)
	}
	if err := store.FinishMinuteAuditTask(ctx, id2, "retry", "retry", now+20_000, true, "partial official response", now+1); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnqueueMinuteAuditTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	if ok, err := reopened.LeaseMinuteAuditTask(ctx, id2, "too-early", now+2, now+10_000); err != nil || ok {
		t.Fatalf("re-enqueue reset backoff: %t %v", ok, err)
	}
	persisted, err := reopened.LoadMinuteAuditTask(ctx, id2)
	if err != nil || persisted == nil || persisted.Attempts != 1 || !persisted.ForceReplay || persisted.Status != "retry" {
		t.Fatalf("retry metadata not durable: %+v %v", persisted, err)
	}
	cursor := market.MinuteAuditCursor{Exchange: "BINANCE", Symbol: "integration", Kind: "window", ThroughMS: task.EndMS, UpdatedAtMS: now}
	if err := store.SaveMinuteAuditCursor(ctx, cursor); err != nil {
		t.Fatal(err)
	}
	cursor.ThroughMS = start
	if err := store.SaveMinuteAuditCursor(ctx, cursor); err != nil {
		t.Fatal(err)
	}
	loaded, err := reopened.LoadMinuteAuditCursor(ctx, "binance", "INTEGRATION", "window")
	if err != nil || loaded == nil || loaded.ThroughMS != task.EndMS {
		t.Fatalf("durable cursor regressed: %+v %v", loaded, err)
	}
	task.Kind, task.StartMS, task.EndMS = "daily", start, start+1439*60_000
	id3, err := store.EnqueueMinuteAuditTask(ctx, task)
	if err != nil {
		t.Fatal(err)
	}
	dailyTasks, err := reopened.ListDueMinuteAuditTasksByKind(ctx, "binance", "daily", now+3, 1)
	if err != nil || len(dailyTasks) != 1 || dailyTasks[0].ID != id3 {
		t.Fatalf("daily worker cannot find its group: %+v %v", dailyTasks, err)
	}
	if ok, err := store.LeaseMinuteAuditTask(ctx, id3, "daily", now, now+10_000); err != nil || !ok {
		t.Fatalf("daily claim: %t %v", ok, err)
	}
	if err := store.FinishMinuteAuditTask(ctx, id3, "daily", "completed", 0, false, "", now+2); err != nil {
		t.Fatal(err)
	}
	summary, err := reopened.MinuteAuditSummary(ctx, now+3)
	if err != nil || summary.Pending != 1 || summary.DailyTotal != 1 || summary.DailyCompleted != 1 || summary.OldestStartMS != start+60_000 {
		t.Fatalf("incorrect durable backlog / completion summary: %+v %v", summary, err)
	}

	// Expired/excluded evidence is bounded even with no daily worker running;
	// old unfinished ranges must survive the same retention pass.
	oldNow := now - int64(8*24*time.Hour/time.Millisecond)
	oldTask := market.MinuteAuditTask{Exchange: "binance", Symbol: "RETENTION", Kind: "window", StartMS: start - int64(8*24*time.Hour/time.Millisecond), EndMS: start - int64(8*24*time.Hour/time.Millisecond), CreatedAtMS: oldNow}
	oldID, err := store.EnqueueMinuteAuditTask(ctx, oldTask)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := store.LeaseMinuteAuditTask(ctx, oldID, "old", oldNow, oldNow+10_000); err != nil || !ok {
		t.Fatalf("claim old terminal: %t %v", ok, err)
	}
	if err := store.FinishMinuteAuditTask(ctx, oldID, "old", "expired", 0, false, "retention boundary", oldNow+1); err != nil {
		t.Fatal(err)
	}
	oldTask.Symbol = "UNFINISHED"
	pendingID, err := store.EnqueueMinuteAuditTask(ctx, oldTask)
	if err != nil {
		t.Fatal(err)
	}
	oldTask.Symbol, oldTask.CreatedAtMS = "RECENT", now
	recentID, err := store.EnqueueMinuteAuditTask(ctx, oldTask)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := store.LeaseMinuteAuditTask(ctx, recentID, "recent", now, now+10_000); err != nil || !ok {
		t.Fatalf("claim recent terminal: %t %v", ok, err)
	}
	if err := store.FinishMinuteAuditTask(ctx, recentID, "recent", "excluded", 0, false, "officially inactive", now+1); err != nil {
		t.Fatal(err)
	}
	if old, err := reopened.LoadMinuteAuditTask(ctx, oldID); err != nil || old != nil {
		t.Fatalf("old terminal evidence not pruned: %+v %v", old, err)
	}
	if pending, err := reopened.LoadMinuteAuditTask(ctx, pendingID); err != nil || pending == nil || pending.Status != "pending" {
		t.Fatalf("retention removed unfinished range: %+v %v", pending, err)
	}
	if recent, err := reopened.LoadMinuteAuditTask(ctx, recentID); err != nil || recent == nil || recent.Status != "excluded" {
		t.Fatalf("retention removed recent evidence: %+v %v", recent, err)
	}
}
