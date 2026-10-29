package storage

import (
	"context"
	"fmt"
	"strings"

	"crypto-ticket/internal/market"
)

// VerifiedMinuteBar keeps the REST verification generation separate from
// exchange timestamps, whose meanings differ between REST and WebSocket.
type VerifiedMinuteBar struct {
	Bar          market.Bar
	VerifiedAtMS int64
}

type MinuteVerificationStore interface {
	LoadMinuteVerifications(ctx context.Context, exchange, symbol string, startMS, endMS int64) ([]VerifiedMinuteBar, error)
	UpsertMinuteRevisions(ctx context.Context, bars []market.Bar, generationMS int64) ([]market.Bar, error)
	MarkMinutesVerified(ctx context.Context, bars []market.Bar, generationMS int64) error
}

func minuteVerificationKey(exchange, symbol string) string {
	return strings.ToLower(exchange) + ":" + strings.ToUpper(symbol) + ":1m"
}

func (m *MemoryHistoricalStore) LoadMinuteVerifications(_ context.Context, exchange, symbol string, startMS, endMS int64) ([]VerifiedMinuteBar, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := minuteVerificationKey(exchange, symbol)
	var result []VerifiedMinuteBar
	for start, bar := range m.bars[key] {
		if start >= startMS && start <= endMS && bar.IsFinal {
			result = append(result, VerifiedMinuteBar{Bar: market.DecorateBar(bar), VerifiedAtMS: m.minuteVerifiedAt[key][start]})
		}
	}
	return result, nil
}

func (m *MemoryHistoricalStore) UpsertMinuteRevisions(_ context.Context, bars []market.Bar, generationMS int64) ([]market.Bar, error) {
	if generationMS <= 0 {
		return nil, fmt.Errorf("minute verification generation must be positive")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	accepted := make([]market.Bar, 0, len(bars))
	for _, bar := range bars {
		if bar.Timeframe != "1m" || !bar.IsFinal {
			return nil, fmt.Errorf("minute revision requires a final 1m bar")
		}
		key := minuteVerificationKey(bar.Exchange, bar.Symbol)
		if m.minuteVerifiedAt[key][bar.StartMS] > generationMS {
			continue
		}
		if m.bars[key] == nil {
			m.bars[key] = make(map[int64]market.Bar)
		}
		m.bars[key][bar.StartMS] = bar
		accepted = append(accepted, bar)
	}
	return accepted, nil
}

func (m *MemoryHistoricalStore) MarkMinutesVerified(_ context.Context, bars []market.Bar, generationMS int64) error {
	if generationMS <= 0 {
		return fmt.Errorf("minute verification generation must be positive")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, bar := range bars {
		key := minuteVerificationKey(bar.Exchange, bar.Symbol)
		if existing, ok := m.bars[key][bar.StartMS]; !ok || !market.SameOfficialMinuteValues(existing, bar) {
			return fmt.Errorf("minute values changed before verification: %s:%d", key, bar.StartMS)
		}
		if m.minuteVerifiedAt[key] == nil {
			m.minuteVerifiedAt[key] = make(map[int64]int64)
		}
		if generationMS > m.minuteVerifiedAt[key][bar.StartMS] {
			m.minuteVerifiedAt[key][bar.StartMS] = generationMS
		}
	}
	return nil
}
