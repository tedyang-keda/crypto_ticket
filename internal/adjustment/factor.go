// Package adjustment derives corporate-action adjustment factors from observed
// market data and persists them for the serving layer. It is layer B of the
// corporate-action handling: given a rebase/split candidate surfaced by the
// instruments monitor (layer A), it locates the price discontinuity in the raw
// bars and turns it into a backward-adjustment factor.
//
// OKX does not expose a machine-readable rebase ratio, so the magnitude is
// recovered empirically from the price gap across the event boundary:
//
//	ratio            = close_before / open_after      (≈ the split / rebase ratio)
//	price_multiplier = open_after  / close_before     (scales history onto the post-event basis)
//	volume_multiplier= close_before / open_after      (= ratio; pre-event share counts scale up)
package adjustment

import (
	"math"
	"sort"

	"crypto-ticket/internal/market"
)

// Derivation is the outcome of locating a corporate-action discontinuity.
type Derivation struct {
	BoundaryMS       int64   // start of the first post-event bar
	CloseBefore      float64 // last close before the boundary
	OpenAfter        float64 // first open at/after the boundary
	Ratio            float64 // close_before / open_after
	PriceMultiplier  float64 // backward-adjustment price factor for pre-event bars
	VolumeMultiplier float64 // backward-adjustment volume factor for pre-event bars
}

// DeriveBackwardFactor scans final bars (ascending by StartMS) for the sharpest
// adjacent price discontinuity and, when it clears minMovePct, returns the
// backward-adjustment factor implied by that gap. The instruments monitor has
// already confirmed a corporate action for this symbol, so the bars only need
// to locate the boundary and magnitude — a strong prior that lets us pick the
// single largest jump rather than guess whether a jump is a corporate action.
//
// minMovePct is expressed as a fraction (0.05 == 5%).
func DeriveBackwardFactor(bars []market.Bar, minMovePct float64) (Derivation, bool) {
	if len(bars) < 2 {
		return Derivation{}, false
	}
	if minMovePct < 0 {
		minMovePct = 0
	}

	bestMove := -1.0
	var best Derivation
	found := false
	for i := 1; i < len(bars); i++ {
		before := bars[i-1].ClosePrice
		after := bars[i].OpenPrice
		if before <= 0 || after <= 0 {
			continue
		}
		move := math.Abs(before-after) / before
		if move < minMovePct || move <= bestMove {
			continue
		}
		bestMove = move
		best = Derivation{
			BoundaryMS:       bars[i].StartMS,
			CloseBefore:      before,
			OpenAfter:        after,
			Ratio:            before / after,
			PriceMultiplier:  after / before,
			VolumeMultiplier: before / after,
		}
		found = true
	}
	return best, found
}

// DeriveRenameFactor computes the backward-adjustment factor across an instId
// rename, where the discontinuity is between two symbols: the predecessor's
// last close before the boundary and the successor's first open at/after it.
// Unlike an in-symbol rebase, a rename may carry no price change (ratio ≈ 1),
// in which case the factor is the identity and still enables history stitching.
func DeriveRenameFactor(predecessorBars []market.Bar, successorBars []market.Bar) (Derivation, bool) {
	succOpen, boundaryMS, ok := firstOpen(successorBars)
	if !ok || succOpen <= 0 {
		return Derivation{}, false
	}
	predClose, ok := lastCloseBefore(predecessorBars, boundaryMS)
	if !ok || predClose <= 0 {
		return Derivation{}, false
	}
	return Derivation{
		BoundaryMS:       boundaryMS,
		CloseBefore:      predClose,
		OpenAfter:        succOpen,
		Ratio:            predClose / succOpen,
		PriceMultiplier:  succOpen / predClose,
		VolumeMultiplier: predClose / succOpen,
	}, true
}

func firstOpen(bars []market.Bar) (open float64, startMS int64, ok bool) {
	best := int64(0)
	found := false
	for _, b := range bars {
		if b.OpenPrice <= 0 {
			continue
		}
		if !found || b.StartMS < best {
			best = b.StartMS
			open = b.OpenPrice
			found = true
		}
	}
	return open, best, found
}

func lastCloseBefore(bars []market.Bar, boundaryMS int64) (float64, bool) {
	best := int64(-1)
	var closePrice float64
	for _, b := range bars {
		if b.ClosePrice <= 0 || b.StartMS >= boundaryMS {
			continue
		}
		if b.StartMS > best {
			best = b.StartMS
			closePrice = b.ClosePrice
		}
	}
	return closePrice, best >= 0
}

// LedgerEvent is one corporate action in a symbol's ordered history, holding
// the incremental (per-event) backward-adjustment multipliers.
type LedgerEvent struct {
	EffectiveMS      int64
	PriceMultiplier  float64 // incremental, = open_after / close_before
	VolumeMultiplier float64 // incremental, = close_before / open_after
	EventType        string
}

// EventFromDerivation converts a single derived discontinuity into a ledger
// event.
func EventFromDerivation(d Derivation, eventType string) LedgerEvent {
	return LedgerEvent{
		EffectiveMS:      d.BoundaryMS,
		PriceMultiplier:  d.PriceMultiplier,
		VolumeMultiplier: d.VolumeMultiplier,
		EventType:        eventType,
	}
}

// CumulativeBackwardSegments composes an ordered ledger of corporate actions
// into the non-overlapping, cumulative factor windows the serving layer
// expects. For events e_1 < ... < e_n the backward-adjusted multiplier for a
// bar at time t is the product of every incremental multiplier for events after
// t, so each segment carries the suffix product:
//
//	[0, e_1)        -> ∏ all events
//	[e_i, e_{i+1})  -> ∏ events after e_i
//	[e_n, ∞)        -> 1
//
// The single-event case reduces to the two-segment form used by layer B.
// Events are sorted and de-duplicated by EffectiveMS (last write wins).
func CumulativeBackwardSegments(base market.AdjustmentFactor, events []LedgerEvent) []market.AdjustmentFactor {
	ordered := dedupeSortedEvents(events)
	if len(ordered) == 0 {
		return nil
	}

	n := len(ordered)
	// Suffix products: priceSuffix[i] = ∏_{k>=i} price, volSuffix[i] = ∏_{k>=i} vol.
	priceSuffix := make([]float64, n+1)
	volSuffix := make([]float64, n+1)
	priceSuffix[n] = 1
	volSuffix[n] = 1
	for i := n - 1; i >= 0; i-- {
		priceSuffix[i] = priceSuffix[i+1] * safeMultiplier(ordered[i].PriceMultiplier)
		volSuffix[i] = volSuffix[i+1] * safeMultiplier(ordered[i].VolumeMultiplier)
	}

	segments := make([]market.AdjustmentFactor, 0, n+1)
	for i := 0; i <= n; i++ {
		seg := base
		seg.AdjMode = market.PriceModeBackwardAdjusted
		if i == 0 {
			seg.EffectiveFromMS = 0
		} else {
			seg.EffectiveFromMS = ordered[i-1].EffectiveMS
		}
		if i == n {
			seg.EffectiveToMS = 0 // open-ended latest regime
		} else {
			seg.EffectiveToMS = ordered[i].EffectiveMS - 1
		}
		seg.PriceMultiplier = priceSuffix[i]
		seg.VolumeMultiplier = volSuffix[i]
		if i < n {
			seg.EventType = firstNonEmpty(ordered[i].EventType, base.EventType)
		}
		segments = append(segments, seg)
	}
	return segments
}

// ReconstructLedger recovers the incremental event ledger from previously
// stored cumulative backward segments, so a new event can be composed with
// prior ones without a separate persisted ledger. Each segment boundary
// (EffectiveFromMS > 0) is an event whose incremental multiplier is the ratio
// of adjacent segments' cumulative multipliers.
func ReconstructLedger(existing []market.AdjustmentFactor) []LedgerEvent {
	segments := make([]market.AdjustmentFactor, 0, len(existing))
	for _, f := range existing {
		if market.MustNormalizePriceMode(f.AdjMode) == market.PriceModeBackwardAdjusted {
			segments = append(segments, f)
		}
	}
	sort.Slice(segments, func(i, j int) bool { return segments[i].EffectiveFromMS < segments[j].EffectiveFromMS })

	events := make([]LedgerEvent, 0, len(segments))
	for i := 1; i < len(segments); i++ {
		prev := segments[i-1]
		cur := segments[i]
		events = append(events, LedgerEvent{
			EffectiveMS:      cur.EffectiveFromMS,
			PriceMultiplier:  safeMultiplier(prev.PriceMultiplier) / safeMultiplier(cur.PriceMultiplier),
			VolumeMultiplier: safeMultiplier(prev.VolumeMultiplier) / safeMultiplier(cur.VolumeMultiplier),
			EventType:        cur.EventType,
		})
	}
	return events
}

// HasEventAt reports whether the ledger already contains an event at the given
// boundary (idempotency guard for re-derivation).
func HasEventAt(events []LedgerEvent, boundaryMS int64) bool {
	for _, e := range events {
		if e.EffectiveMS == boundaryMS {
			return true
		}
	}
	return false
}

func dedupeSortedEvents(events []LedgerEvent) []LedgerEvent {
	if len(events) == 0 {
		return nil
	}
	ordered := append([]LedgerEvent(nil), events...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].EffectiveMS < ordered[j].EffectiveMS })
	deduped := ordered[:0]
	for i, e := range ordered {
		if i > 0 && e.EffectiveMS == deduped[len(deduped)-1].EffectiveMS {
			deduped[len(deduped)-1] = e // last write wins
			continue
		}
		deduped = append(deduped, e)
	}
	return deduped
}

func safeMultiplier(value float64) float64 {
	if value == 0 || math.IsNaN(value) || math.IsInf(value, 0) {
		return 1
	}
	return value
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
