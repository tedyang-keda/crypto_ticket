package guardian

import (
	"context"
	"fmt"
	"time"

	"crypto-ticket/internal/market"
)

type symbolSnapshotReader interface {
	ListSymbolSnapshots(context.Context, string) ([]market.SymbolSnapshot, error)
	ListSymbolSnapshotSyncs(context.Context) ([]market.SymbolSnapshotSync, error)
}

// A round reads lifecycle eligibility once per exchange, rather than loading
// the entire inventory for every candle or REST request. It is independent of
// the registry's last observation and never changes subscription policy.
type repairEligibility struct {
	required bool
	allowed  map[string]bool
	listings map[string]int64
}

func (e repairEligibility) listedAt(symbol market.SymbolInfo) int64 {
	if listed, ok := e.listings[fetcherKey(symbol.Exchange, symbol.MarketType)+":"+normalizeSymbol(symbol.Symbol)]; ok {
		return listed
	}
	return symbol.ListedAtMS
}

func (e repairEligibility) permits(symbol market.SymbolInfo) bool {
	return !e.required || e.allowed[fetcherKey(symbol.Exchange, symbol.MarketType)+":"+normalizeSymbol(symbol.Symbol)]
}

func (g *Guardian) loadRepairEligibility(ctx context.Context, exchangeName string) (repairEligibility, error) {
	e := repairEligibility{required: g.cfg.RequireSymbolSnapshots, allowed: make(map[string]bool), listings: make(map[string]int64)}
	if !e.required {
		return e, nil
	}
	reader, ok := g.store.(symbolSnapshotReader)
	if !ok {
		return e, fmt.Errorf("guardian requires a symbol snapshot reader")
	}
	items, err := reader.ListSymbolSnapshots(ctx, exchangeName)
	if err != nil {
		return e, fmt.Errorf("read guardian symbol snapshots: %w", err)
	}
	syncs, err := reader.ListSymbolSnapshotSyncs(ctx)
	if err != nil {
		return e, fmt.Errorf("read guardian snapshot sync state: %w", err)
	}
	byScope := make(map[string]market.SymbolSnapshotSync, len(syncs))
	for _, sync := range syncs {
		byScope[fetcherKey(sync.Exchange, sync.MarketType)] = sync
	}
	nowMS := g.now().UnixMilli()
	maxAge := g.cfg.SymbolMaxAge
	if maxAge <= 0 {
		maxAge = 10 * time.Minute
	}
	for _, item := range items {
		key := fetcherKey(item.Exchange, item.MarketType)
		if normalizeExchange(item.Exchange) == normalizeExchange(exchangeName) && market.SymbolSnapshotRepairSkipReason(item, byScope[key], nowMS, maxAge.Milliseconds()) == "" {
			e.allowed[key+":"+normalizeSymbol(item.Symbol)] = true
			e.listings[key+":"+normalizeSymbol(item.Symbol)] = item.ListedAtMS
		}
	}
	return e, nil
}
