package storage

import (
	"context"
	"math"
	"sort"
	"strings"
	"sync"

	"crypto-ticket/internal/market"
)

type MemoryHistoricalStore struct {
	mu                sync.RWMutex
	bars              map[string]map[int64]market.Bar
	symbols           map[string]market.SymbolInfo
	adjustmentFactors []market.AdjustmentFactor
	adjustedBars      map[string]map[int64]market.Bar
	guardianStates    map[string]market.KlineGuardianState
	guardianEvents    []market.KlineGuardianEvent
	corporateActions  map[string]market.CorporateActionEvent
}

func NewMemoryHistoricalStore() *MemoryHistoricalStore {
	return &MemoryHistoricalStore{
		bars:             make(map[string]map[int64]market.Bar),
		symbols:          make(map[string]market.SymbolInfo),
		adjustedBars:     make(map[string]map[int64]market.Bar),
		guardianStates:   make(map[string]market.KlineGuardianState),
		corporateActions: make(map[string]market.CorporateActionEvent),
	}
}

func (m *MemoryHistoricalStore) EnsureSchema(context.Context) error {
	return nil
}

func (m *MemoryHistoricalStore) UpsertBars(_ context.Context, bars []market.Bar) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, bar := range bars {
		if bar.SourceMarket == "" {
			bar.SourceMarket = market.SourceMarket(bar.Exchange, "")
		}
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
	mode := market.MustNormalizePriceMode(query.PriceMode)
	rows := m.bars[key]
	if mode != market.PriceModeRaw {
		if adjustedRows := m.adjustedBars[adjustedKey(query.Exchange, query.SourceMarket, query.Symbol, query.Timeframe, mode)]; len(adjustedRows) > 0 {
			rows = adjustedRows
		}
	}
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
		bar := market.DecorateBar(rows[start])
		if mode != market.PriceModeRaw && (bar.PriceMode != mode || bar.AdjustmentStatus != market.AdjustmentStatusAdjusted) {
			bar = m.applyFactorLocked(bar, query.SourceMarket, mode)
		} else if mode == market.PriceModeRaw {
			bar = market.MarkBarAdjustmentStatus(bar, mode, market.AdjustmentStatusRaw)
		}
		bars = append(bars, bar)
	}
	if mode != market.PriceModeRaw {
		bars = recalculateMemoryDerived(bars)
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

func (m *MemoryHistoricalStore) UpsertSymbols(_ context.Context, symbols []market.SymbolInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, symbol := range symbols {
		if symbol.SourceMarket == "" {
			symbol.SourceMarket = market.SourceMarket(symbol.Exchange, symbol.MarketType)
		}
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

func (m *MemoryHistoricalStore) AdjustmentFactorAt(_ context.Context, exchange string, sourceMarket string, symbol string, priceMode string, tsMS int64) (*market.AdjustmentFactor, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.adjustmentFactorAtLocked(exchange, sourceMarket, symbol, priceMode, tsMS), nil
}

func (m *MemoryHistoricalStore) ListAdjustmentFactors(_ context.Context, exchange string, sourceMarket string, symbol string, priceMode string) ([]market.AdjustmentFactor, error) {
	mode := market.MustNormalizePriceMode(priceMode)
	exchange = strings.ToLower(exchange)
	symbol = strings.ToUpper(symbol)
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]market.AdjustmentFactor, 0)
	for _, factor := range m.adjustmentFactors {
		if strings.ToLower(factor.Exchange) != exchange || strings.ToUpper(factor.Symbol) != symbol {
			continue
		}
		if sourceMarket != "" && factor.SourceMarket != "" && !strings.EqualFold(factor.SourceMarket, sourceMarket) {
			continue
		}
		if market.MustNormalizePriceMode(factor.AdjMode) != mode {
			continue
		}
		out = append(out, factor)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].EffectiveFromMS < out[j].EffectiveFromMS })
	return out, nil
}

func (m *MemoryHistoricalStore) ReplaceAdjustmentFactors(_ context.Context, exchange string, sourceMarket string, symbol string, priceMode string, factors []market.AdjustmentFactor) error {
	mode := market.MustNormalizePriceMode(priceMode)
	lowerExchange := strings.ToLower(exchange)
	upperSymbol := strings.ToUpper(symbol)
	m.mu.Lock()
	defer m.mu.Unlock()
	kept := m.adjustmentFactors[:0:0]
	for _, factor := range m.adjustmentFactors {
		if strings.ToLower(factor.Exchange) == lowerExchange &&
			strings.ToUpper(factor.Symbol) == upperSymbol &&
			market.MustNormalizePriceMode(factor.AdjMode) == mode &&
			(sourceMarket == "" || factor.SourceMarket == "" || strings.EqualFold(factor.SourceMarket, sourceMarket)) {
			continue // drop existing factors for this key/mode
		}
		kept = append(kept, factor)
	}
	for _, factor := range factors {
		factor.Exchange = strings.ToLower(factor.Exchange)
		factor.Symbol = strings.ToUpper(factor.Symbol)
		factor.AdjMode = market.MustNormalizePriceMode(factor.AdjMode)
		kept = append(kept, factor)
	}
	m.adjustmentFactors = kept
	return nil
}

func (m *MemoryHistoricalStore) UpsertAdjustmentFactors(_ context.Context, factors []market.AdjustmentFactor) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, factor := range factors {
		factor.Exchange = strings.ToLower(factor.Exchange)
		factor.Symbol = strings.ToUpper(factor.Symbol)
		factor.AdjMode = market.MustNormalizePriceMode(factor.AdjMode)
		replaced := false
		for i := range m.adjustmentFactors {
			current := m.adjustmentFactors[i]
			if current.Provider == factor.Provider &&
				current.ProviderVersion == factor.ProviderVersion &&
				current.Exchange == factor.Exchange &&
				current.SourceMarket == factor.SourceMarket &&
				current.Symbol == factor.Symbol &&
				current.AdjMode == factor.AdjMode &&
				current.EffectiveFromMS == factor.EffectiveFromMS &&
				current.EffectiveToMS == factor.EffectiveToMS {
				m.adjustmentFactors[i] = factor
				replaced = true
				break
			}
		}
		if !replaced {
			m.adjustmentFactors = append(m.adjustmentFactors, factor)
		}
	}
	return nil
}

func (m *MemoryHistoricalStore) UpsertAdjustedBars(_ context.Context, bars []market.Bar) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, bar := range bars {
		mode := market.MustNormalizePriceMode(bar.PriceMode)
		if mode == market.PriceModeRaw {
			continue
		}
		key := adjustedKey(bar.Exchange, bar.SourceMarket, bar.Symbol, bar.Timeframe, mode)
		if m.adjustedBars[key] == nil {
			m.adjustedBars[key] = make(map[int64]market.Bar)
		}
		m.adjustedBars[key][bar.StartMS] = market.DecorateBar(bar)
	}
	return nil
}

func (m *MemoryHistoricalStore) UpsertCorporateActionEvent(_ context.Context, event market.CorporateActionEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	event.Exchange = strings.ToLower(strings.TrimSpace(event.Exchange))
	event.Symbol = strings.ToUpper(strings.TrimSpace(event.Symbol))
	m.corporateActions[event.ActionID] = event
	return nil
}

func (m *MemoryHistoricalStore) ListOpenCorporateActionEvents(_ context.Context) ([]market.CorporateActionEvent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]market.CorporateActionEvent, 0)
	for _, event := range m.corporateActions {
		if event.State == market.CorporateActionStateFactor || event.State == market.CorporateActionStateManualReview {
			continue
		}
		out = append(out, event)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FirstSeenMS < out[j].FirstSeenMS })
	return out, nil
}

func (m *MemoryHistoricalStore) applyFactorLocked(bar market.Bar, sourceMarket string, priceMode string) market.Bar {
	factor := m.adjustmentFactorAtLocked(bar.Exchange, firstNonEmpty(sourceMarket, bar.SourceMarket), bar.Symbol, priceMode, market.BarAdjustmentTimestamp(bar))
	if factor == nil {
		return market.MarkBarAdjustmentStatus(bar, priceMode, market.AdjustmentStatusMissing)
	}
	return market.ApplyFactorToBar(bar, *factor)
}

func (m *MemoryHistoricalStore) adjustmentFactorAtLocked(exchange string, sourceMarket string, symbol string, priceMode string, tsMS int64) *market.AdjustmentFactor {
	mode := market.MustNormalizePriceMode(priceMode)
	if mode == market.PriceModeRaw {
		return nil
	}
	exchange = strings.ToLower(exchange)
	symbol = strings.ToUpper(symbol)
	for _, factor := range m.adjustmentFactors {
		if strings.ToLower(factor.Exchange) != exchange || strings.ToUpper(factor.Symbol) != symbol {
			continue
		}
		if factor.SourceMarket != "" && sourceMarket != "" && !strings.EqualFold(factor.SourceMarket, sourceMarket) {
			continue
		}
		if market.MustNormalizePriceMode(factor.AdjMode) != mode {
			continue
		}
		if factor.EffectiveFromMS <= tsMS && (factor.EffectiveToMS == 0 || tsMS <= factor.EffectiveToMS) {
			factorCopy := factor
			return &factorCopy
		}
	}
	return nil
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

func adjustedKey(exchange string, sourceMarket string, symbol string, tf string, mode string) string {
	return strings.ToLower(exchange) + ":" + sourceMarket + ":" + strings.ToUpper(symbol) + ":" + tf + ":" + market.MustNormalizePriceMode(mode)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func recalculateMemoryDerived(bars []market.Bar) []market.Bar {
	var previousClose float64
	for i := range bars {
		bars[i].PrevClose = previousClose
		if previousClose > 0 {
			bars[i].Chg = roundMemoryPercent((bars[i].ClosePrice - previousClose) / previousClose * 100)
		} else {
			bars[i].Chg = 0
		}
		if bars[i].LowPrice > 0 {
			bars[i].Amp = roundMemoryPercent((bars[i].HighPrice - bars[i].LowPrice) / bars[i].LowPrice * 100)
		} else {
			bars[i].Amp = 0
		}
		previousClose = bars[i].ClosePrice
		bars[i] = market.DecorateBar(bars[i])
	}
	return bars
}

func roundMemoryPercent(value float64) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0
	}
	return math.Round(value*1_000_000) / 1_000_000
}
