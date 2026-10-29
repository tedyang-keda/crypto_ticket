package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"crypto-ticket/internal/market"
	"crypto-ticket/internal/timeframe"
)

func freshSnapshotFixture(now time.Time) repairSnapshot {
	return repairSnapshot{CapturedAtMS: now.UnixMilli(), Records: []market.SymbolSnapshot{{Exchange: "binance", Symbol: "BTCUSDT", MarketType: "um_futures", OfficialActive: true, Subscribed: true, SubscriptionReason: "enabled", Presence: "present", CapturedAtMS: now.UnixMilli(), ListedAtMS: now.AddDate(-2, 0, 0).UnixMilli()}}, Syncs: []market.SymbolSnapshotSync{{Exchange: "binance", MarketType: "um_futures", Status: "ok", LastSuccessMS: now.UnixMilli()}}}
}

func TestSnapshotPoliciesFailClosedAndKeepOfficialListing(t *testing.T) {
	now := time.Now()
	s := freshSnapshotFixture(now)
	p, err := s.policies(now, 15*time.Minute)
	if err != nil || !p["binance:BTCUSDT"].Allowed || p["binance:BTCUSDT"].ListedAtMS != s.Records[0].ListedAtMS {
		t.Fatalf("policies=%+v err=%v", p, err)
	}
	for _, mutate := range []func(*repairSnapshot){
		func(s *repairSnapshot) { s.Records[0].Subscribed = false },
		func(s *repairSnapshot) { s.Records[0].Presence = "absent" },
		func(s *repairSnapshot) { s.Records[0].OfficialActive = false },
		func(s *repairSnapshot) { s.Records[0].Delisted = true },
		func(s *repairSnapshot) { s.Syncs[0].Status = "unavailable" },
		func(s *repairSnapshot) { s.Syncs[0].LastSuccessMS++ },
		func(s *repairSnapshot) { s.Records[0].CapturedAtMS = now.Add(-time.Hour).UnixMilli() },
	} {
		s := freshSnapshotFixture(now)
		mutate(&s)
		p, err := s.policies(now, 15*time.Minute)
		if err != nil || p["binance:BTCUSDT"].Allowed || p["binance:BTCUSDT"].Reason == "" {
			t.Fatalf("unsafe policy=%+v err=%v", p, err)
		}
	}
	s = freshSnapshotFixture(now)
	s.Records = append(s.Records, s.Records[0])
	if _, err := s.policies(now, time.Hour); err == nil {
		t.Fatal("ambiguous market route accepted")
	}
}

func TestLoadSnapshotAndPathSafety(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.json")
	s := freshSnapshotFixture(time.Now())
	b, _ := json.Marshal(s)
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := loadSnapshot(options{snapshot: path})
	if err != nil || len(loaded.Records) != 1 || loaded.Records[0].Symbol != "BTCUSDT" {
		t.Fatalf("snapshot=%+v err=%v", loaded, err)
	}
	if err := validatePaths(options{report: path, snapshot: path, summary: filepath.Join(dir, "summary.json")}); err == nil {
		t.Fatal("snapshot can overwrite report")
	}
	if err := os.WriteFile(path, append(b, []byte(" {}")...), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadSnapshot(options{snapshot: path}); err == nil {
		t.Fatal("extra snapshot JSON accepted")
	}
}

type fixtureSnapshotStore struct {
	s     repairSnapshot
	reads int
}

func (f *fixtureSnapshotStore) ListSymbolSnapshots(_ context.Context, exchange string) ([]market.SymbolSnapshot, error) {
	f.reads++
	items := []market.SymbolSnapshot{}
	for _, item := range f.s.Records {
		if item.Exchange == exchange {
			items = append(items, item)
		}
	}
	return items, nil
}

func (f *fixtureSnapshotStore) ListSymbolSnapshotSyncs(context.Context) ([]market.SymbolSnapshotSync, error) {
	return f.s.Syncs, nil
}

func TestEligibilityGateRefreshesAndStopsChangedSubscriptions(t *testing.T) {
	f := &fixtureSnapshotStore{s: freshSnapshotFixture(time.Now())}
	g := &eligibilityGate{store: f, maxAge: 15 * time.Minute}
	p, err := g.policy(context.Background(), "binance", "BTCUSDT")
	if err != nil || !p.Allowed || f.reads != 2 {
		t.Fatalf("policy=%+v err=%v reads=%d", p, err, f.reads)
	}
	if _, err := g.policy(context.Background(), "binance", "BTCUSDT"); err != nil || f.reads != 2 {
		t.Fatalf("cached gate performs repeated database reads: %d %v", f.reads, err)
	}
	f.s.Records[0].Subscribed = false
	f.s.Records[0].SubscriptionReason = "contract_filtered"
	g.loadedAt = time.Now().Add(-2 * time.Minute)
	p, err = g.policy(context.Background(), "binance", "BTCUSDT")
	if err != nil || p.Allowed || !strings.Contains(p.Reason, "filtered") {
		t.Fatalf("changed subscription was not excluded: %+v %v", p, err)
	}
}

func TestPlanOnlyUsesFrozenSnapshotWithoutDatabase(t *testing.T) {
	now := time.Now().UTC()
	latest := timeframe.PreviousStartMS(timeframe.FloorStartMS(now.Add(-2*time.Minute).UnixMilli(), "15m"), "15m")
	dir := t.TempDir()
	o := options{report: filepath.Join(dir, "audit.json"), snapshot: filepath.Join(dir, "snapshot.json"), summary: filepath.Join(dir, "plan.json"), exchanges: "binance,okx", planOnly: true, binanceRPS: 1, okxRPS: 1, maxRuntime: time.Hour, maxInFlight: 8, snapshotMaxAge: 15 * time.Minute}
	report := map[string]any{"generated_at": now.Format(time.RFC3339Nano), "as_of": now.Format(time.RFC3339Nano), "complete": true, "results": []any{map[string]any{"exchange": "binance", "timeframe": "15m", "latest_completed_ms": latest, "series": []any{map[string]any{"symbol": "BTCUSDT", "canonical_bars": 1, "first_canonical_ms": latest - 2*15*timeframe.MinuteMS, "last_canonical_ms": latest - 2*15*timeframe.MinuteMS}}}}}
	b, _ := json.Marshal(report)
	if err := os.WriteFile(o.report, b, 0600); err != nil {
		t.Fatal(err)
	}
	fixture := freshSnapshotFixture(now)
	// Keep this offline fixture focused on tail repair. A historical listing
	// date would intentionally add leading tasks to the plan.
	fixture.Records[0].ListedAtMS = latest - timeframe.MinuteMS
	b, _ = json.Marshal(fixture)
	if err := os.WriteFile(o.snapshot, b, 0600); err != nil {
		t.Fatal(err)
	}
	// No DB should be opened by this plan-only/offline path.
	t.Setenv("MYSQL_DSN", "invalid-dsn-that-must-not-be-used")
	if err := run(o); err != nil {
		t.Fatal(err)
	}
	s := readSummaryForTest(t, o.summary)
	if s.Status != "planned" || s.Requested != 2 || s.ByKind["tail"] != 2 || s.PlanByExchange["binance"].Targets != 2 || len(s.PlannedTasks) != 1 {
		t.Fatalf("incorrect offline plan: %+v", s)
	}
}
