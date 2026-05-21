package cache

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"crypto-ticket/internal/market"
)

type MemoryMarketCache struct {
	mu      sync.RWMutex
	quotes  map[string]market.Tick
	live    map[string]market.Bar
	history map[string]map[int64]market.Bar
}

func NewMemoryMarketCache() *MemoryMarketCache {
	return &MemoryMarketCache{
		quotes:  make(map[string]market.Tick),
		live:    make(map[string]market.Bar),
		history: make(map[string]map[int64]market.Bar),
	}
}

func (m *MemoryMarketCache) SetLatestTick(_ context.Context, tick market.Tick) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.quotes[quoteKey(tick.Exchange, tick.Symbol)] = tick
	return nil
}

func (m *MemoryMarketCache) GetLatestTick(_ context.Context, exchange string, symbol string) (*market.Tick, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	tick, ok := m.quotes[quoteKey(exchange, symbol)]
	if !ok {
		return nil, nil
	}
	return &tick, nil
}

func (m *MemoryMarketCache) SetLiveBar(_ context.Context, bar market.Bar) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.live[barKey(bar.Exchange, bar.Symbol, bar.Timeframe)] = bar
	return nil
}

func (m *MemoryMarketCache) GetLiveBar(_ context.Context, exchange string, symbol string, tf string) (*market.Bar, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	bar, ok := m.live[barKey(exchange, symbol, tf)]
	if !ok {
		return nil, nil
	}
	return &bar, nil
}

func (m *MemoryMarketCache) PutFinalBars(_ context.Context, bars []market.Bar, maxKeep int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, bar := range bars {
		key := barKey(bar.Exchange, bar.Symbol, bar.Timeframe)
		if m.history[key] == nil {
			m.history[key] = make(map[int64]market.Bar)
		}
		m.history[key][bar.StartMS] = bar
		if maxKeep > 0 && len(m.history[key]) > maxKeep {
			starts := make([]int64, 0, len(m.history[key]))
			for start := range m.history[key] {
				starts = append(starts, start)
			}
			sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
			for len(starts) > maxKeep {
				delete(m.history[key], starts[0])
				starts = starts[1:]
			}
		}
	}
	return nil
}

func (m *MemoryMarketCache) RecentBars(_ context.Context, query market.KlineQuery) ([]market.Bar, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	rows := m.history[barKey(query.Exchange, query.Symbol, query.Timeframe)]
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

func quoteKey(exchange string, symbol string) string {
	return fmt.Sprintf("%s:%s", strings.ToLower(exchange), strings.ToUpper(symbol))
}

func barKey(exchange string, symbol string, tf string) string {
	return fmt.Sprintf("%s:%s:%s", strings.ToLower(exchange), strings.ToUpper(symbol), tf)
}
