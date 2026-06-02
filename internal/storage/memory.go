package storage

import (
	"context"
	"sort"
	"strings"
	"sync"

	"crypto-ticket/internal/market"
)

type MemoryHistoricalStore struct {
	mu      sync.RWMutex
	bars    map[string]map[int64]market.Bar
	symbols map[string]market.SymbolInfo
}

func NewMemoryHistoricalStore() *MemoryHistoricalStore {
	return &MemoryHistoricalStore{
		bars:    make(map[string]map[int64]market.Bar),
		symbols: make(map[string]market.SymbolInfo),
	}
}

func (m *MemoryHistoricalStore) EnsureSchema(context.Context) error {
	return nil
}

func (m *MemoryHistoricalStore) UpsertBars(_ context.Context, bars []market.Bar) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, bar := range bars {
		key := strings.ToLower(bar.Exchange) + ":" + strings.ToUpper(bar.Symbol) + ":" + bar.Timeframe
		if m.bars[key] == nil {
			m.bars[key] = make(map[int64]market.Bar)
		}
		m.bars[key][bar.StartMS] = bar
	}
	return nil
}

func (m *MemoryHistoricalStore) RecentBars(_ context.Context, query market.KlineQuery) ([]market.Bar, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := strings.ToLower(query.Exchange) + ":" + strings.ToUpper(query.Symbol) + ":" + query.Timeframe
	rows := m.bars[key]
	if len(rows) == 0 {
		return nil, nil
	}
	starts := make([]int64, 0, len(rows))
	for start := range rows {
		starts = append(starts, start)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
	limit := query.Limit
	if limit <= 0 || limit > len(starts) {
		limit = len(starts)
	}
	starts = starts[len(starts)-limit:]
	bars := make([]market.Bar, 0, len(starts))
	for _, start := range starts {
		bars = append(bars, rows[start])
	}
	return bars, nil
}

func (m *MemoryHistoricalStore) BarsInRange(_ context.Context, exchange string, symbol string, tf string, startMS int64, endMS int64) ([]market.Bar, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := strings.ToLower(exchange) + ":" + strings.ToUpper(symbol) + ":" + tf
	rows := m.bars[key]
	if len(rows) == 0 {
		return nil, nil
	}
	starts := make([]int64, 0, len(rows))
	for start := range rows {
		if start >= startMS && start <= endMS {
			starts = append(starts, start)
		}
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
	bars := make([]market.Bar, 0, len(starts))
	for _, start := range starts {
		bar := rows[start]
		if bar.IsFinal {
			bars = append(bars, bar)
		}
	}
	return bars, nil
}

func (m *MemoryHistoricalStore) UpsertSymbols(_ context.Context, symbols []market.SymbolInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, symbol := range symbols {
		m.symbols[strings.ToLower(symbol.Exchange)+":"+strings.ToUpper(symbol.Symbol)] = symbol
	}
	return nil
}

func (m *MemoryHistoricalStore) ListSymbols(_ context.Context, exchange string, activeOnly *bool) ([]market.SymbolInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []market.SymbolInfo
	for _, symbol := range m.symbols {
		if strings.ToLower(symbol.Exchange) != strings.ToLower(exchange) {
			continue
		}
		if activeOnly != nil && symbol.IsActive != *activeOnly {
			continue
		}
		out = append(out, symbol)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Symbol < out[j].Symbol })
	return out, nil
}
