package guardian

import (
	"context"
	"errors"
	"testing"
	"time"

	"crypto-ticket/internal/market"
	"crypto-ticket/internal/storage"
)

type eligibilityStore struct {
	*storage.MemoryHistoricalStore
	snapshots []market.SymbolSnapshot
	syncs     []market.SymbolSnapshotSync
	err       error
}

func (s *eligibilityStore) ListSymbolSnapshots(context.Context, string) ([]market.SymbolSnapshot, error) {
	return s.snapshots, s.err
}
func (s *eligibilityStore) ListSymbolSnapshotSyncs(context.Context) ([]market.SymbolSnapshotSync, error) {
	return s.syncs, s.err
}

func TestGuardianLifecycleFiltersMinuteAndDailyTargets(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 16, 6, 0, 0, 0, time.UTC)
	store := &eligibilityStore{MemoryHistoricalStore: storage.NewMemoryHistoricalStore()}
	store.syncs = []market.SymbolSnapshotSync{{Exchange: "binance", MarketType: "um_futures", Status: "ok", LastSuccessMS: now.UnixMilli()}}
	for _, symbol := range []string{"LIVE", "ABSENT", "DELISTED", "STOPPED", "UNKNOWN", "STALE"} {
		if err := store.UpsertSymbols(ctx, []market.SymbolInfo{{Exchange: "binance", Symbol: symbol, MarketType: "um_futures", IsActive: true, LastSeenAtMS: now.UnixMilli()}}); err != nil {
			t.Fatal(err)
		}
		if symbol == "UNKNOWN" {
			continue
		}
		item := market.SymbolSnapshot{Exchange: "binance", Symbol: symbol, MarketType: "um_futures", Subscribed: true, Presence: "present", OfficialActive: true, CapturedAtMS: now.UnixMilli()}
		switch symbol {
		case "ABSENT":
			item.Presence = "absent"
		case "DELISTED":
			item.Delisted = true
		case "STOPPED":
			item.Subscribed, item.SubscriptionReason = false, "market_disabled"
		case "STALE":
			item.CapturedAtMS = now.Add(-time.Hour).UnixMilli()
		}
		store.snapshots = append(store.snapshots, item)
	}
	g := New(store, &storeRepairer{store: store.MemoryHistoricalStore}, []Fetcher{&fakeFetcher{}}, Config{Enabled: true, RequireSymbolSnapshots: true})
	g.now = func() time.Time { return now }
	for _, load := range []func(context.Context) ([]auditTarget, error){g.auditTargets, g.dailyAuditTargets} {
		targets, err := load(ctx)
		if err != nil || len(targets) != 1 || targets[0].Symbol != "LIVE" {
			t.Fatalf("unexpected lifecycle-filtered targets: %+v err=%v", targets, err)
		}
	}
	store.syncs[0].Status = "unavailable"
	targets, err := g.dailyAuditTargets(ctx)
	if err != nil || len(targets) != 0 {
		t.Fatalf("failed inventory refresh must not allow stale lifecycle targets: %+v %v", targets, err)
	}
	store.err = errors.New("snapshot database unavailable")
	if _, err := g.auditTargets(ctx); err == nil {
		t.Fatal("inventory read failure was hidden")
	}
}
