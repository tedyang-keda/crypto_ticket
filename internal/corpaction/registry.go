// Package corpaction holds the real-time corporate-action state shared between
// the detection/derivation pipeline and the serving path. It is layer C of the
// corporate-action handling: while a symbol is mid-rebase (before a factor is
// confirmed) or on the exact bar that straddles a confirmed boundary, the
// serving path must mark the bar live_raw and suppress its spurious change so
// the discontinuity is not treated as real volatility.
package corpaction

import (
	"math"
	"strings"
	"sync"
	"time"
)

// Config tunes how long windows stay active and what counts as a
// corporate-action-scale move during the pending phase.
type Config struct {
	// PendingTTL bounds the heuristic (magnitude-based) window between rebase
	// detection and factor confirmation. Kept short so genuine large moves are
	// not suppressed for long.
	PendingTTL time.Duration
	// ResolvedTTL bounds how long a confirmed boundary keeps neutralizing the
	// crossing bar (long enough to cover the largest rolled-up timeframe).
	ResolvedTTL time.Duration
	// NeutralizePct is the |change| fraction (0.15 == 15%) treated as a
	// corporate action during the pending phase.
	NeutralizePct float64
}

func (c Config) withDefaults() Config {
	if c.PendingTTL <= 0 {
		c.PendingTTL = 30 * time.Minute
	}
	if c.ResolvedTTL <= 0 {
		c.ResolvedTTL = 26 * time.Hour
	}
	if c.NeutralizePct <= 0 {
		c.NeutralizePct = 0.15
	}
	return c
}

type window struct {
	openedMS     int64
	lastActiveMS int64
	boundaryMS   int64 // 0 until a factor is confirmed
}

// Registry tracks active corporate-action windows per exchange:symbol. It is
// safe for concurrent use.
type Registry struct {
	cfg     Config
	mu      sync.Mutex
	windows map[string]window
}

// NewRegistry builds a Registry with the given configuration.
func NewRegistry(cfg Config) *Registry {
	return &Registry{
		cfg:     cfg.withDefaults(),
		windows: make(map[string]window),
	}
}

// MarkActive opens (or refreshes) a pending window at detection time. It never
// clears an already-known boundary.
func (r *Registry) MarkActive(exchange string, symbol string, openedMS int64) {
	k := key(exchange, symbol)
	r.mu.Lock()
	defer r.mu.Unlock()
	w := r.windows[k]
	if w.openedMS == 0 || openedMS < w.openedMS {
		w.openedMS = openedMS
	}
	if openedMS > w.lastActiveMS {
		w.lastActiveMS = openedMS
	}
	r.windows[k] = w
}

// Touch extends a pending window while the durable corporate-action lifecycle
// is still active. It does not change the original open time.
func (r *Registry) Touch(exchange string, symbol string, activeMS int64) {
	k := key(exchange, symbol)
	r.mu.Lock()
	defer r.mu.Unlock()
	w, ok := r.windows[k]
	if !ok || w.boundaryMS > 0 {
		return
	}
	if activeMS > w.lastActiveMS {
		w.lastActiveMS = activeMS
	}
	r.windows[k] = w
}

// Resolve records the confirmed boundary for a symbol, switching its window
// from the pending heuristic to exact crossing-bar suppression.
func (r *Registry) Resolve(exchange string, symbol string, boundaryMS int64) {
	k := key(exchange, symbol)
	r.mu.Lock()
	defer r.mu.Unlock()
	w := r.windows[k]
	w.boundaryMS = boundaryMS
	if w.openedMS == 0 {
		w.openedMS = boundaryMS
	}
	w.lastActiveMS = boundaryMS
	r.windows[k] = w
}

// Clear removes any window for a symbol.
func (r *Registry) Clear(exchange string, symbol string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.windows, key(exchange, symbol))
}

// AssessBar reports how a bar's derived metrics should be treated:
//   - liveRaw: mark the bar as raw across an unadjusted corporate action.
//   - neutralize: suppress the spurious change so it is not read as volatility.
//
// A confirmed boundary neutralizes exactly the bar whose span contains it; the
// pending phase falls back to a magnitude heuristic. Expired windows are pruned
// lazily and treated as inactive.
func (r *Registry) AssessBar(exchange string, symbol string, barStartMS int64, barEndMS int64, chgPct float64, nowMS int64) (liveRaw bool, neutralize bool) {
	k := key(exchange, symbol)
	r.mu.Lock()
	defer r.mu.Unlock()
	w, ok := r.windows[k]
	if !ok {
		return false, false
	}
	if w.boundaryMS > 0 {
		if nowMS > w.boundaryMS+r.cfg.ResolvedTTL.Milliseconds() {
			delete(r.windows, k)
			return false, false
		}
		// The crossing bar is the one whose span contains the boundary: its
		// close is post-event while its predecessor's close is pre-event.
		if w.boundaryMS >= barStartMS && w.boundaryMS <= barEndMS {
			return true, true
		}
		return false, false
	}
	activeMS := w.lastActiveMS
	if activeMS == 0 {
		activeMS = w.openedMS
	}
	if nowMS > activeMS+r.cfg.PendingTTL.Milliseconds() {
		delete(r.windows, k)
		return false, false
	}
	if math.Abs(chgPct) >= r.cfg.NeutralizePct*100 {
		return true, true
	}
	return true, false
}

// Active reports whether a (non-expired) window exists for a symbol.
func (r *Registry) Active(exchange string, symbol string, nowMS int64) bool {
	k := key(exchange, symbol)
	r.mu.Lock()
	defer r.mu.Unlock()
	w, ok := r.windows[k]
	if !ok {
		return false
	}
	if w.boundaryMS > 0 {
		return nowMS <= w.boundaryMS+r.cfg.ResolvedTTL.Milliseconds()
	}
	activeMS := w.lastActiveMS
	if activeMS == 0 {
		activeMS = w.openedMS
	}
	return nowMS <= activeMS+r.cfg.PendingTTL.Milliseconds()
}

func key(exchange string, symbol string) string {
	return strings.ToLower(strings.TrimSpace(exchange)) + "|" + strings.ToUpper(strings.TrimSpace(symbol))
}
