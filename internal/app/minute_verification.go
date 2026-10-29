package app

import (
	"context"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"time"

	"crypto-ticket/internal/market"
	"crypto-ticket/internal/storage"
)

// lockBarSymbols serializes every timeframe of a symbol across WS ingestion,
// official corrections and derived repairs. Callers must not reacquire it from
// internal cascade helpers. The fixed shards keep memory bounded.
func (s *MarketService) lockBarSymbols(bars []market.Bar) func() {
	indexes := make(map[int]bool)
	for _, bar := range bars {
		h := fnv.New32a()
		_, _ = h.Write([]byte(strings.ToLower(bar.Exchange) + ":" + strings.ToUpper(bar.Symbol)))
		indexes[int(h.Sum32()%uint32(len(s.symbolLocks)))] = true
	}
	ordered := make([]int, 0, len(indexes))
	for index := range indexes {
		ordered = append(ordered, index)
	}
	sort.Ints(ordered)
	for _, index := range ordered {
		s.symbolLocks[index].Lock()
	}
	return func() {
		for i := len(ordered) - 1; i >= 0; i-- {
			s.symbolLocks[ordered[i]].Unlock()
		}
	}
}

// acceptUnverifiedFinal is called with the symbol lock held. A late WS final
// cannot replace a REST-verified minute. Conflicts remain durable work even
// after the ordinary recent-minute window has advanced.
func (s *MarketService) acceptUnverifiedFinal(ctx context.Context, bar market.Bar) (bool, error) {
	if bar.Timeframe != "1m" || !bar.IsFinal {
		return true, nil
	}
	verifications, ok := s.store.(storage.MinuteVerificationStore)
	if !ok {
		return true, nil
	}
	rows, err := verifications.LoadMinuteVerifications(ctx, bar.Exchange, bar.Symbol, bar.StartMS, bar.StartMS)
	if err != nil {
		return false, err
	}
	for _, row := range rows {
		if row.VerifiedAtMS <= 0 {
			continue
		}
		if market.SameOfficialMinuteValues(bar, row.Bar) {
			return false, nil
		}
		tasks, ok := s.store.(storage.MinuteAuditTaskStore)
		if !ok {
			return false, fmt.Errorf("verified minute conflict requires durable audit task storage")
		}
		nowMS := market.NowMS()
		_, err := tasks.EnqueueMinuteAuditTask(ctx, market.MinuteAuditTask{
			Exchange: bar.Exchange, Symbol: bar.Symbol, Kind: "conflict",
			StartMS: bar.StartMS, EndMS: bar.StartMS, ExpectedStartMS: bar.StartMS,
			ExpectData: true, ForceReplay: true, NextAttemptMS: nowMS,
			CreatedAtMS: nowMS, UpdatedAtMS: nowMS,
		})
		return false, err
	}
	return true, nil
}

// VerifyFinalBars is the explicit Guardian calibration boundary. Source=rest
// alone does not grant permission to overwrite a verified minute. generationMS
// is captured before the official request starts, never from exchange LastTickMS.
func (s *MarketService) VerifyFinalBars(ctx context.Context, bars []market.Bar, generationMS int64) error {
	if len(bars) == 0 {
		return nil
	}
	if generationMS <= 0 {
		return fmt.Errorf("minute verification generation must be positive")
	}
	store, ok := s.store.(storage.MinuteVerificationStore)
	if !ok {
		return fmt.Errorf("minute verification storage is unavailable")
	}
	ordered := append([]market.Bar(nil), bars...)
	for i := range ordered {
		ordered[i] = normalizeBar(ordered[i])
		if ordered[i].Timeframe != "1m" || !ordered[i].IsFinal || !validBar(ordered[i]) {
			return fmt.Errorf("verification requires valid final 1m bars")
		}
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Exchange != ordered[j].Exchange {
			return ordered[i].Exchange < ordered[j].Exchange
		}
		if ordered[i].Symbol != ordered[j].Symbol {
			return ordered[i].Symbol < ordered[j].Symbol
		}
		return ordered[i].StartMS < ordered[j].StartMS
	})
	unlock := s.lockBarSymbols(ordered)
	defer unlock()
	for start := 0; start < len(ordered); {
		end := start + 1
		for end < len(ordered) && ordered[end].Exchange == ordered[start].Exchange && ordered[end].Symbol == ordered[start].Symbol {
			end++
		}
		if err := s.verifySymbolMinutes(ctx, store, ordered[start:end], generationMS); err != nil {
			return err
		}
		start = end
	}
	return nil
}

func (s *MarketService) verifySymbolMinutes(ctx context.Context, store storage.MinuteVerificationStore, bars []market.Bar, generationMS int64) error {
	key := bars[0].Exchange + ":" + bars[0].Symbol
	if latest, ok := s.latestVerification.Load(key); ok && latest.(int64) > generationMS {
		return fmt.Errorf("stale minute verification for %s: request=%d latest=%d", key, generationMS, latest.(int64))
	}
	s.latestVerification.Store(key, generationMS)
	rows, err := store.LoadMinuteVerifications(ctx, bars[0].Exchange, bars[0].Symbol, bars[0].StartMS, bars[len(bars)-1].StartMS)
	if err != nil {
		return err
	}
	existing := make(map[int64]storage.VerifiedMinuteBar, len(rows))
	for _, row := range rows {
		existing[row.Bar.StartMS] = row
	}
	toMark := make([]market.Bar, 0, len(bars))
	for _, bar := range bars {
		old, exists := existing[bar.StartMS]
		if old.VerifiedAtMS > generationMS {
			return fmt.Errorf("stale minute verification for %s at %d: request=%d verified=%d", key, bar.StartMS, generationMS, old.VerifiedAtMS)
		}
		if exists && market.SameOfficialMinuteValues(old.Bar, bar) {
			if old.VerifiedAtMS == 0 {
				toMark = append(toMark, bar)
			}
			continue
		}
		bar.Source = "rest"
		enriched, err := s.prepareFinalBar(ctx, bar)
		if err != nil {
			return err
		}
		started := time.Now()
		accepted, err := store.UpsertMinuteRevisions(ctx, []market.Bar{enriched}, generationMS)
		s.observeStorage("verified_minute_upsert", started, err)
		if err != nil {
			return err
		}
		if len(accepted) == 0 {
			return fmt.Errorf("stale minute revision rejected for %s at %d", key, bar.StartMS)
		}
		for _, revised := range accepted {
			if s.telemetry != nil {
				s.telemetry.BarPersisted(revised)
			}
			s.publishBar(revised)
		}
		if err := s.rollupFinalBars(ctx, accepted, true); err != nil {
			return err
		}
		toMark = append(toMark, accepted...)
	}
	// Mark only after every changed source has completed its cascade. A failed
	// or interrupted chain is replayed by the persistent minute audit task.
	if len(toMark) > 0 {
		return store.MarkMinutesVerified(ctx, toMark, generationMS)
	}
	return nil
}
