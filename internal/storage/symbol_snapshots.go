package storage

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"crypto-ticket/internal/market"
)

func (m *MemoryHistoricalStore) SaveSymbolSnapshot(ctx context.Context, batch market.SymbolSnapshotBatch) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if batch.Exchange == "" || batch.MarketType == "" || batch.CapturedAtMS <= 0 || len(batch.Symbols) == 0 {
		return fmt.Errorf("invalid symbol snapshot batch")
	}
	for _, item := range batch.Symbols {
		if item.Exchange != batch.Exchange || item.MarketType != batch.MarketType || item.CapturedAtMS != batch.CapturedAtMS || item.Symbol == "" {
			return fmt.Errorf("symbol snapshot outside batch scope")
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := strings.ToLower(batch.Exchange + ":" + batch.MarketType)
	if previous := m.symbolSnapshotSyncs[key]; previous.LastAttemptMS > batch.CapturedAtMS {
		return fmt.Errorf("older symbol snapshot rejected")
	}
	for _, item := range batch.Symbols {
		item.OfficialRaw = append([]byte(nil), item.OfficialRaw...)
		m.symbolSnapshots[strings.ToLower(item.Exchange)+":"+strings.ToUpper(item.Symbol)] = item
	}
	m.symbolSnapshotSyncs[key] = market.SymbolSnapshotSync{Exchange: batch.Exchange, MarketType: batch.MarketType, Status: "ok", LastAttemptMS: batch.CapturedAtMS, LastSuccessMS: batch.CapturedAtMS, SymbolCount: len(batch.Symbols)}
	return nil
}

func (m *MemoryHistoricalStore) RecordSymbolSnapshotFailure(ctx context.Context, state market.SymbolSnapshotSync) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := strings.ToLower(state.Exchange + ":" + state.MarketType)
	previous := m.symbolSnapshotSyncs[key]
	if previous.LastAttemptMS > state.LastAttemptMS {
		return nil
	}
	state.Status, state.LastSuccessMS, state.SymbolCount = "unavailable", previous.LastSuccessMS, previous.SymbolCount
	m.symbolSnapshotSyncs[key] = state
	return nil
}

func (m *MemoryHistoricalStore) ListSymbolSnapshots(ctx context.Context, exchange string) ([]market.SymbolSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]market.SymbolSnapshot, 0)
	for _, item := range m.symbolSnapshots {
		if exchange != "" && !strings.EqualFold(item.Exchange, exchange) {
			continue
		}
		item.OfficialRaw = append([]byte(nil), item.OfficialRaw...)
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Exchange+":"+out[i].Symbol < out[j].Exchange+":"+out[j].Symbol })
	return out, nil
}

func (m *MemoryHistoricalStore) ListSymbolSnapshotSyncs(ctx context.Context) ([]market.SymbolSnapshotSync, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]market.SymbolSnapshotSync, 0, len(m.symbolSnapshotSyncs))
	for _, item := range m.symbolSnapshotSyncs {
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Exchange+":"+out[i].MarketType < out[j].Exchange+":"+out[j].MarketType
	})
	return out, nil
}
