package cache

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"crypto-ticket/internal/market"
)

type MemoryMarketCache struct {
	mu      sync.RWMutex
	quotes  map[string]market.Tick
	live    map[string]market.Bar
	history map[string]map[int64]market.Bar
	rolling map[string]market.Bar
	actions map[string]int64
	dedupe  map[string]time.Time
}

func NewMemoryMarketCache() *MemoryMarketCache {
	return &MemoryMarketCache{
		quotes:  make(map[string]market.Tick),
		live:    make(map[string]market.Bar),
		history: make(map[string]map[int64]market.Bar),
		rolling: make(map[string]market.Bar),
		actions: make(map[string]int64),
		dedupe:  make(map[string]time.Time),
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

func (m *MemoryMarketCache) MarkTickSeen(_ context.Context, tick market.Tick, windowStartMS int64, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	for key, expiresAt := range m.dedupe {
		if now.After(expiresAt) {
			delete(m.dedupe, key)
		}
	}
	key := dedupeKey(tick, windowStartMS)
	if expiresAt, ok := m.dedupe[key]; ok && now.Before(expiresAt) {
		return false, nil
	}
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	m.dedupe[key] = now.Add(ttl)
	return true, nil
}

func (m *MemoryMarketCache) GetRollingBar(_ context.Context, exchange string, symbol string, startMS int64) (*market.Bar, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	bar, ok := m.rolling[rollingKey(exchange, symbol, startMS)]
	if !ok {
		return nil, nil
	}
	return &bar, nil
}

func (m *MemoryMarketCache) PutRollingBar(_ context.Context, bar market.Bar, actionAtMS int64, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := rollingKey(bar.Exchange, bar.Symbol, bar.StartMS)
	m.rolling[key] = bar
	m.actions[key] = actionAtMS
	return nil
}

func (m *MemoryMarketCache) DeleteRollingBar(_ context.Context, exchange string, symbol string, startMS int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := rollingKey(exchange, symbol, startMS)
	delete(m.rolling, key)
	delete(m.actions, key)
	return nil
}

func (m *MemoryMarketCache) DueRollingBars(_ context.Context, cutoffMS int64, limit int) ([]market.Bar, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if limit <= 0 {
		limit = 500
	}
	keys := make([]string, 0)
	for key, actionAtMS := range m.actions {
		if actionAtMS <= cutoffMS {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if m.actions[keys[i]] == m.actions[keys[j]] {
			return keys[i] < keys[j]
		}
		return m.actions[keys[i]] < m.actions[keys[j]]
	})
	if len(keys) > limit {
		keys = keys[:limit]
	}
	bars := make([]market.Bar, 0, len(keys))
	for _, key := range keys {
		if bar, ok := m.rolling[key]; ok {
			bars = append(bars, bar)
		}
	}
	return bars, nil
}

func (m *MemoryMarketCache) SetLive1mBar(ctx context.Context, bar market.Bar) error {
	bar.Timeframe = "1m"
	return m.SetLiveBar(ctx, bar)
}

func (m *MemoryMarketCache) GetLive1mBar(ctx context.Context, exchange string, symbol string) (*market.Bar, error) {
	return m.GetLiveBar(ctx, exchange, symbol, "1m")
}

func quoteKey(exchange string, symbol string) string {
	return fmt.Sprintf("%s:%s", strings.ToLower(exchange), strings.ToUpper(symbol))
}

func barKey(exchange string, symbol string, tf string) string {
	return fmt.Sprintf("%s:%s:%s", strings.ToLower(exchange), strings.ToUpper(symbol), tf)
}

func rollingKey(exchange string, symbol string, startMS int64) string {
	return fmt.Sprintf("%s:%s:%d", strings.ToLower(exchange), strings.ToUpper(symbol), startMS)
}

func dedupeKey(tick market.Tick, windowStartMS int64) string {
	id := strings.TrimSpace(tick.TradeID)
	if id == "" {
		id = fmt.Sprintf("%d:%0.12f:%0.12f:%s", tick.TsMS, tick.Price, tick.Size, strings.ToLower(tick.Side))
	}
	return fmt.Sprintf("%s:%s:%d:%s", strings.ToLower(tick.Exchange), strings.ToUpper(tick.Symbol), windowStartMS, id)
}
