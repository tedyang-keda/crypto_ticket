package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"crypto-ticket/internal/config"
	"crypto-ticket/internal/gapfill"
	"crypto-ticket/internal/market"
	mysqlstore "crypto-ticket/internal/storage/mysql"

	mysqldriver "github.com/go-sql-driver/mysql"
)

// repairSnapshot is an immutable input to planning. It captures persisted official
// inventories, not an independent HTTP discovery or a mutation of subscriptions.
type repairSnapshot struct {
	CapturedAtMS int64                       `json:"captured_at_ms"`
	Records      []market.SymbolSnapshot     `json:"records"`
	Syncs        []market.SymbolSnapshotSync `json:"syncs"`
}

type snapshotReader interface {
	ListSymbolSnapshots(context.Context, string) ([]market.SymbolSnapshot, error)
	ListSymbolSnapshotSyncs(context.Context) ([]market.SymbolSnapshotSync, error)
}

func connectStore() (*mysqlstore.Store, error) {
	dsn, err := mysqldriver.ParseDSN(config.Load().MySQLDSN)
	if err != nil {
		return nil, errors.New("invalid MYSQL_DSN")
	}
	dsn.ClientFoundRows = false
	dsn.Timeout, dsn.ReadTimeout, dsn.WriteTimeout = 10*time.Second, 30*time.Second, 30*time.Second
	store, err := mysqlstore.New(dsn.FormatDSN())
	if err != nil {
		return nil, errors.New("connect mysql failed")
	}
	return store, nil
}

func captureSnapshot(ctx context.Context, store snapshotReader) (repairSnapshot, error) {
	s := repairSnapshot{CapturedAtMS: time.Now().UnixMilli(), Records: []market.SymbolSnapshot{}}
	var err error
	s.Syncs, err = store.ListSymbolSnapshotSyncs(ctx)
	if err != nil {
		return s, fmt.Errorf("read official inventory syncs: %w", err)
	}
	for _, name := range []string{"binance", "okx"} {
		items, err := store.ListSymbolSnapshots(ctx, name)
		if err != nil {
			return s, fmt.Errorf("read official inventory: %w", err)
		}
		s.Records = append(s.Records, items...)
	}
	// Read syncs again: inventory batches are atomic, but different market scopes
	// can refresh during this export. A mismatch is fail-closed in policies().
	s.Syncs, err = store.ListSymbolSnapshotSyncs(ctx)
	return s, err
}

func loadSnapshot(o options) (repairSnapshot, error) {
	if o.refreshSnapshot {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		store, err := connectStore()
		if err != nil {
			return repairSnapshot{}, err
		}
		defer store.Close()
		s, err := captureSnapshot(ctx, store)
		if err != nil {
			return s, err
		}
		b, err := json.Marshal(s)
		if err != nil {
			return s, err
		}
		if err := os.WriteFile(o.snapshot+".tmp", b, 0600); err != nil {
			return s, err
		}
		return s, os.Rename(o.snapshot+".tmp", o.snapshot)
	}
	f, err := os.Open(o.snapshot)
	if err != nil {
		return repairSnapshot{}, err
	}
	defer f.Close()
	var s repairSnapshot
	limited := &io.LimitedReader{R: f, N: 32<<20 + 1}
	decoder := json.NewDecoder(limited)
	if err := decoder.Decode(&s); err != nil {
		return s, fmt.Errorf("decode official inventory snapshot: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF || limited.N <= 0 {
		return s, errors.New("invalid or oversized official inventory snapshot")
	}
	return s, nil
}

func (s repairSnapshot) policies(now time.Time, maxAge time.Duration) (map[string]gapfill.SymbolPolicy, error) {
	if len(s.Records) == 0 || len(s.Syncs) == 0 {
		return nil, errors.New("official inventory snapshot is empty; wait for a successful metadata sync")
	}
	syncs := make(map[string]market.SymbolSnapshotSync)
	for _, state := range s.Syncs {
		syncs[state.Exchange+":"+state.MarketType] = state
	}
	policies := make(map[string]gapfill.SymbolPolicy)
	for _, item := range s.Records {
		key := item.Exchange + ":" + item.Symbol
		if _, duplicate := policies[key]; duplicate {
			return nil, fmt.Errorf("ambiguous official inventory route %s", key)
		}
		reason := market.SymbolSnapshotRepairSkipReason(item, syncs[item.Exchange+":"+item.MarketType], now.UnixMilli(), maxAge.Milliseconds())
		policies[key] = gapfill.SymbolPolicy{Allowed: reason == "", Reason: reason, MarketType: item.MarketType, ListedAtMS: item.ListedAtMS, CapturedAtMS: item.CapturedAtMS}
	}
	return policies, nil
}

// eligibilityGate refreshes persisted metadata at most once a minute. Planning
// stays reproducible, but a symbol stopped/delisted after planning cannot keep
// receiving historical REST requests for the rest of a multi-hour run.
type eligibilityGate struct {
	mu       sync.Mutex
	store    snapshotReader
	maxAge   time.Duration
	loadedAt time.Time
	policies map[string]gapfill.SymbolPolicy
}

func (g *eligibilityGate) policy(ctx context.Context, exchange, symbol string) (gapfill.SymbolPolicy, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := time.Now()
	if g.policies == nil || now.Sub(g.loadedAt) >= time.Minute {
		s, err := captureSnapshot(ctx, g.store)
		if err != nil {
			return gapfill.SymbolPolicy{}, err
		}
		policies, err := s.policies(now, g.maxAge)
		if err != nil {
			return gapfill.SymbolPolicy{}, err
		}
		g.policies, g.loadedAt = policies, now
	}
	p, ok := g.policies[exchange+":"+symbol]
	if !ok {
		p.Reason = "missing_official_subscription_snapshot"
	}
	// CapturedAt can become stale during the cache interval.
	if p.Allowed && (p.CapturedAtMS > now.UnixMilli() || now.UnixMilli()-p.CapturedAtMS > g.maxAge.Milliseconds()) {
		p.Allowed, p.Reason = false, "snapshot_stale"
	}
	return p, nil
}
