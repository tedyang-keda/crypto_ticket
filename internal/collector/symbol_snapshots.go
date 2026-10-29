package collector

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"crypto-ticket/internal/market"
	"crypto-ticket/internal/storage"
)

type SymbolSnapshotFetcher interface {
	Name() string
	MarketType() string
	FetchSymbols(context.Context, *http.Client) ([]market.SymbolInfo, error)
}

type SymbolSnapshotSource struct {
	Fetcher             SymbolSnapshotFetcher
	SubscriptionEnabled bool
}

type symbolSnapshotRepository interface {
	storage.SymbolSnapshotStore
	ListSymbols(context.Context, string, *bool) ([]market.SymbolInfo, error)
}

// SymbolSnapshotWorker is independent of WebSocket lifetime and covers disabled
// collection markets too. A temporary disconnect must not stop inventory refresh.
type SymbolSnapshotWorker struct {
	sources  []SymbolSnapshotSource
	store    symbolSnapshotRepository
	client   *http.Client
	interval time.Duration
}

func NewSymbolSnapshotWorker(sources []SymbolSnapshotSource, store symbolSnapshotRepository, interval time.Duration) *SymbolSnapshotWorker {
	if interval <= 0 {
		interval = 120 * time.Second
	}
	return &SymbolSnapshotWorker{sources: sources, store: store, interval: interval, client: &http.Client{Timeout: 20 * time.Second}}
}

func (w *SymbolSnapshotWorker) Run(ctx context.Context) error {
	// Independent loops mean one slow/unavailable exchange cannot suppress the
	// other exchange's metadata refreshes. Exactly one request is active per scope.
	done := make(chan struct{}, len(w.sources))
	for _, source := range w.sources {
		source := source
		go func() {
			defer func() { done <- struct{}{} }()
			ticker := time.NewTicker(w.interval)
			defer ticker.Stop()
			for {
				if err := w.RefreshOnce(ctx, source); err != nil && ctx.Err() == nil {
					log.Printf("symbol snapshot refresh %s/%s failed; retaining previous inventory: %v", source.Fetcher.Name(), source.Fetcher.MarketType(), err)
				}
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
			}
		}()
	}
	for range w.sources {
		<-done
	}
	return ctx.Err()
}

func (w *SymbolSnapshotWorker) RefreshOnce(ctx context.Context, source SymbolSnapshotSource) error {
	if source.Fetcher == nil || w.store == nil {
		return fmt.Errorf("symbol snapshot source/store unavailable")
	}
	exchange, marketType := source.Fetcher.Name(), source.Fetcher.MarketType()
	fail := func(cause error) error {
		if ctx.Err() != nil {
			return cause
		}
		state := market.SymbolSnapshotSync{Exchange: exchange, MarketType: marketType, Status: "unavailable", LastAttemptMS: time.Now().UnixMilli(), LastError: cause.Error()}
		if err := w.store.RecordSymbolSnapshotFailure(ctx, state); err != nil {
			return fmt.Errorf("refresh: %v; persist failure: %w", cause, err)
		}
		return cause
	}
	items, err := source.Fetcher.FetchSymbols(ctx, w.client)
	if err != nil {
		return fail(err)
	}
	previous, err := w.store.ListSymbolSnapshots(ctx, exchange)
	if err != nil {
		return fail(err)
	}
	// Seed the first snapshot with historical registered symbols, so an item
	// absent from today's official list cannot remain an unclassified ghost.
	registry, err := w.store.ListSymbols(ctx, exchange, nil)
	if err != nil {
		return fail(err)
	}
	known := make(map[string]bool, len(previous))
	for _, item := range previous {
		known[strings.ToUpper(item.Symbol)] = true
	}
	for _, info := range registry {
		if known[strings.ToUpper(info.Symbol)] || !strings.EqualFold(info.MarketType, marketType) {
			continue
		}
		previous = append(previous, market.SymbolSnapshot{Exchange: exchange, Symbol: strings.ToUpper(info.Symbol), MarketType: marketType, OfficialStatus: info.Status, Presence: "unknown"})
	}
	batch, err := market.ReconcileSymbolSnapshot(exchange, marketType, source.SubscriptionEnabled, time.Now().UnixMilli(), items, previous)
	if err != nil {
		return fail(err)
	}
	if err := w.store.SaveSymbolSnapshot(ctx, batch); err != nil {
		return fail(err)
	}
	return nil
}
