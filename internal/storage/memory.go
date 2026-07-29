package storage

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"crypto-ticket/internal/market"
)

type MemoryHistoricalStore struct {
	mu             sync.RWMutex
	bars           map[string]map[int64]market.Bar
	symbols        map[string]market.SymbolInfo
	guardianStates map[string]market.KlineGuardianState
	guardianEvents []market.KlineGuardianEvent
	corporateJobs  map[string]market.CorporateActionJob
}

func NewMemoryHistoricalStore() *MemoryHistoricalStore {
	return &MemoryHistoricalStore{
		bars:           make(map[string]map[int64]market.Bar),
		symbols:        make(map[string]market.SymbolInfo),
		guardianStates: make(map[string]market.KlineGuardianState),
		corporateJobs:  make(map[string]market.CorporateActionJob),
	}
}

func (m *MemoryHistoricalStore) EnsureSchema(context.Context) error {
	return nil
}

func (m *MemoryHistoricalStore) Ping(context.Context) error {
	return nil
}

func (m *MemoryHistoricalStore) ContinuousOneMinuteSeries(_ context.Context, startMS int64, endMS int64, minimumHours int) ([]market.MarketSeries, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	hoursBySeries := make(map[string]map[int64]bool)
	for key, values := range m.bars {
		parts := strings.Split(key, ":")
		if len(parts) != 3 || parts[2] != "1m" {
			continue
		}
		for _, bar := range values {
			if !bar.IsFinal || bar.StartMS < startMS || bar.StartMS > endMS {
				continue
			}
			if hoursBySeries[key] == nil {
				hoursBySeries[key] = make(map[int64]bool)
			}
			hoursBySeries[key][bar.StartMS/3_600_000] = true
		}
	}
	var out []market.MarketSeries
	for key, hours := range hoursBySeries {
		if len(hours) < minimumHours {
			continue
		}
		parts := strings.Split(key, ":")
		info := m.symbols[parts[0]+":"+parts[1]]
		out = append(out, market.MarketSeries{Exchange: parts[0], Symbol: parts[1], MarketType: info.MarketType})
	}
	return out, nil
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
		bars = append(bars, market.DecorateBar(rows[start]))
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
			bars = append(bars, market.DecorateBar(bar))
		}
	}
	return bars, nil
}

func (m *MemoryHistoricalStore) DeleteBarsInRange(_ context.Context, exchange string, symbol string, tf string, startMS int64, endMS int64) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := strings.ToLower(exchange) + ":" + strings.ToUpper(symbol) + ":" + tf
	rows := m.bars[key]
	var deleted int64
	for start := range rows {
		if start >= startMS && start <= endMS {
			delete(rows, start)
			deleted++
		}
	}
	return deleted, nil
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

func (m *MemoryHistoricalStore) LoadKlineGuardianState(_ context.Context, exchange string, symbol string, tf string) (*market.KlineGuardianState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	state, ok := m.guardianStates[guardianStateKey(exchange, symbol, tf)]
	if !ok {
		return nil, nil
	}
	return &state, nil
}

func (m *MemoryHistoricalStore) UpsertKlineGuardianState(_ context.Context, state market.KlineGuardianState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	state.Exchange = strings.ToLower(state.Exchange)
	state.Symbol = strings.ToUpper(state.Symbol)
	m.guardianStates[guardianStateKey(state.Exchange, state.Symbol, state.Timeframe)] = state
	return nil
}

func (m *MemoryHistoricalStore) InsertKlineGuardianEvents(_ context.Context, events []market.KlineGuardianEvent) error {
	if len(events) == 0 {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, event := range events {
		event.ID = int64(len(m.guardianEvents) + 1)
		event.Exchange = strings.ToLower(event.Exchange)
		event.Symbol = strings.ToUpper(event.Symbol)
		m.guardianEvents = append(m.guardianEvents, event)
	}
	return nil
}

func guardianStateKey(exchange string, symbol string, tf string) string {
	return strings.ToLower(exchange) + ":" + strings.ToUpper(symbol) + ":" + tf
}

func (m *MemoryHistoricalStore) LoadCorporateActionJob(_ context.Context, exchange string, symbol string, effectiveMS int64) (*market.CorporateActionJob, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	job, ok := m.corporateJobs[corporateActionJobKey(exchange, symbol, effectiveMS)]
	if !ok {
		return nil, nil
	}
	return &job, nil
}

func (m *MemoryHistoricalStore) InsertCorporateActionJob(_ context.Context, job market.CorporateActionJob) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := corporateActionJobKey(job.Exchange, job.Symbol, job.EffectiveMS)
	if existing, ok := m.corporateJobs[key]; ok {
		job.ID = existing.ID
		if job.Status == "" {
			job.Status = existing.Status
		}
	}
	if job.ID == 0 {
		job.ID = int64(len(m.corporateJobs) + 1)
	}
	m.corporateJobs[key] = job
	return nil
}

func (m *MemoryHistoricalStore) ListDueCorporateActionJobs(_ context.Context, nowMS int64, limit int) ([]market.CorporateActionJob, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if limit <= 0 {
		limit = 10
	}
	out := make([]market.CorporateActionJob, 0, limit)
	for _, job := range m.corporateJobs {
		staleRunning := job.Status == "running" && job.UpdatedAtMS > 0 && nowMS-job.UpdatedAtMS >= int64((10*time.Minute)/time.Millisecond)
		if (job.Status != "pending" && job.Status != "retry" && !staleRunning) || (job.Status != "running" && job.NextRetryMS > 0 && job.NextRetryMS > nowMS) {
			continue
		}
		if staleRunning {
			job.Status = "retry"
		}
		out = append(out, job)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (m *MemoryHistoricalStore) UpdateCorporateActionJob(_ context.Context, job market.CorporateActionJob) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.corporateJobs[corporateActionJobKey(job.Exchange, job.Symbol, job.EffectiveMS)] = job
	return nil
}

func (m *MemoryHistoricalStore) ListCorporateActionFactors(_ context.Context, exchange string, symbol string) ([]market.CorporateActionJob, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []market.CorporateActionJob
	for _, job := range m.corporateJobs {
		if strings.EqualFold(job.Exchange, exchange) && strings.EqualFold(job.Symbol, symbol) &&
			(job.Status == "confirmed" || job.Status == "running" || job.Status == "retry" || job.Status == "completed") && job.Factor > 0 {
			out = append(out, job)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].EffectiveMS < out[j].EffectiveMS })
	return out, nil
}

func corporateActionJobKey(exchange string, symbol string, effectiveMS int64) string {
	return strings.ToLower(exchange) + ":" + strings.ToUpper(symbol) + ":" + fmt.Sprint(effectiveMS)
}
