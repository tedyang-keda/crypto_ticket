package adjustment

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"strings"
	"sync"
	"time"

	"crypto-ticket/internal/market"
)

const (
	// ProviderName identifies factors produced by the empirical price-gap
	// deriver, distinguishing them from vendor-supplied factors.
	ProviderName = "okx_rebase_gap"
	// BinanceProviderName identifies factors whose ratio comes from Binance's
	// official adjustment announcement. Klines only locate the boundary.
	BinanceProviderName = "binance_official_announcement"
	// OKXProviderName identifies historical factors whose ratio comes from an
	// official OKX announcement. Official candles only locate the boundary.
	OKXProviderName = "okx_official_announcement"
	// ProviderVersion tracks the derivation algorithm so factors can be
	// recomputed if the method changes.
	ProviderVersion = "v1"

	deriverTimeframe = "1m"
)

// BarSource loads the raw bars used to locate a corporate-action boundary.
type BarSource interface {
	BarsInRange(ctx context.Context, exchange string, symbol string, timeframe string, startMS int64, endMS int64) ([]market.Bar, error)
}

// FactorStore reads the existing factor timeline (to compose cumulatively) and
// atomically replaces it.
type FactorStore interface {
	ListAdjustmentFactors(ctx context.Context, exchange string, sourceMarket string, symbol string, priceMode string) ([]market.AdjustmentFactor, error)
	ReplaceAdjustmentFactors(ctx context.Context, exchange string, sourceMarket string, symbol string, priceMode string, factors []market.AdjustmentFactor) error
}

type EventStore interface {
	UpsertCorporateActionEvent(ctx context.Context, event market.CorporateActionEvent) error
	ListOpenCorporateActionEvents(ctx context.Context) ([]market.CorporateActionEvent, error)
}

// Registry is the real-time corporate-action window store (layer C). The
// deriver opens a pending window when a candidate is queued and resolves it to
// the confirmed boundary once a factor is written. It is optional.
type Registry interface {
	MarkActive(exchange string, symbol string, openedMS int64)
	Touch(exchange string, symbol string, activeMS int64)
	Resolve(exchange string, symbol string, boundaryMS int64)
}

// AliasResolver reports a symbol's renamed predecessor (layer D). When an
// in-symbol gap is absent (a fresh post-rename instId), the deriver falls back
// to a cross-symbol rename factor using the predecessor's bars. Optional.
type AliasResolver interface {
	Lookup(exchange string, successor string) (predecessor string, sourceMarket string, boundaryMS int64, ok bool)
}

// AnnouncementVerifier cross-checks a derived corporate-action against the
// exchange's official announcements: whether one exists for the symbol near the
// boundary (confirming it IS a corporate action) and, best-effort, the
// announced ratio for numeric verification. Optional.
type AnnouncementVerifier interface {
	VerifyCorporateAction(ctx context.Context, exchange string, sourceMarket string, symbol string, boundaryMS int64) (found bool, ratio float64, hasRatio bool)
}

// OfficialKlineSource fetches authoritative bars from the exchange REST API.
// When set, the deriver derives factors from official data rather than the
// local store, and flags bars where the local store has diverged from the
// exchange. Optional.
type OfficialKlineSource interface {
	FetchOfficialKlines(ctx context.Context, exchange string, sourceMarket string, symbol string, timeframe string, startMS int64, endMS int64) ([]market.Bar, error)
}

// Config tunes derivation timing and thresholds. Zero values fall back to
// sensible defaults.
type Config struct {
	// ConfirmDelay is how long to wait after a candidate event before deriving,
	// giving OKX's rebase suspension window time to complete and the first
	// post-event bar to settle.
	ConfirmDelay time.Duration
	// Interval is the worker tick period.
	Interval time.Duration
	// Lookback bounds how far before the event to scan for the boundary.
	Lookback time.Duration
	// MinMovePct is the minimum adjacent price move (fraction) treated as a
	// corporate-action discontinuity rather than ordinary volatility.
	MinMovePct float64
	// MaxAttempts caps retries before a candidate is abandoned.
	MaxAttempts int
	// MaxWait bounds the full lifecycle, including a long exchange trading halt.
	MaxWait time.Duration
	// OfficialDivergencePct is the |close| difference fraction (0.02 == 2%)
	// beyond which a local bar is reported as diverged from official data.
	OfficialDivergencePct float64
	// AnnouncementTolerancePct is the |ratio| difference fraction (0.05 == 5%)
	// beyond which a derived ratio is treated as contradicting the announcement.
	AnnouncementTolerancePct float64
	// RequireAnnouncement, when true, rejects a factor if no matching official
	// announcement is found (strict). Default false (lenient: only a positive
	// ratio contradiction rejects).
	RequireAnnouncement bool
}

func (c Config) withDefaults() Config {
	if c.ConfirmDelay <= 0 {
		c.ConfirmDelay = 10 * time.Minute
	}
	if c.Interval <= 0 {
		c.Interval = time.Minute
	}
	if c.Lookback <= 0 {
		c.Lookback = 6 * time.Hour
	}
	if c.MinMovePct <= 0 {
		c.MinMovePct = 0.05
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 10
	}
	if c.MaxWait <= 0 {
		c.MaxWait = 48 * time.Hour
	}
	if c.OfficialDivergencePct <= 0 {
		c.OfficialDivergencePct = 0.02
	}
	if c.AnnouncementTolerancePct <= 0 {
		c.AnnouncementTolerancePct = 0.05
	}
	return c
}

type pendingEvent struct {
	actionID     string
	exchange     string
	sourceMarket string
	symbol       string
	marketType   string
	eventType    string
	firstSeenMS  int64
	lastEventMS  int64
	resumeMS     int64
	state        string
	attempts     int
	raw          json.RawMessage
}

// Deriver consumes corporate-action candidate events (as an
// instrument.EventSink) and derives adjustment factors once the event boundary
// has settled. Enqueue is fast and lock-guarded; the heavy bar scan and
// persistence run on the worker loop.
type Deriver struct {
	source   BarSource
	store    FactorStore
	events   EventStore
	registry Registry
	aliases  AliasResolver
	official OfficialKlineSource
	verifier AnnouncementVerifier
	cfg      Config
	mu       sync.Mutex
	pending  map[string]*pendingEvent
}

// New builds a Deriver reading bars from source and persisting factors to store.
func New(source BarSource, store FactorStore, cfg Config) *Deriver {
	d := &Deriver{
		source:  source,
		store:   store,
		cfg:     cfg.withDefaults(),
		pending: make(map[string]*pendingEvent),
	}
	if events, ok := store.(EventStore); ok {
		d.events = events
	}
	return d
}

// SetRegistry attaches the real-time corporate-action window registry (layer C).
// Optional; when unset the deriver only produces factors.
func (d *Deriver) SetRegistry(registry Registry) {
	d.registry = registry
}

// SetAliasResolver attaches the rename-alias resolver (layer D). Optional;
// enables cross-symbol rename factor derivation.
func (d *Deriver) SetAliasResolver(aliases AliasResolver) {
	d.aliases = aliases
}

// SetOfficialSource attaches the authoritative REST kline source. Optional;
// when set the deriver derives from official data and flags local divergence.
func (d *Deriver) SetOfficialSource(official OfficialKlineSource) {
	d.official = official
}

// SetAnnouncementVerifier attaches the announcement cross-check. Optional; when
// set, a derived ratio that contradicts the official announcement is rejected.
func (d *Deriver) SetAnnouncementVerifier(verifier AnnouncementVerifier) {
	d.verifier = verifier
}

// HandleInstrumentEvent implements instrument.EventSink: it enqueues the
// candidate for deferred derivation, deduplicating by symbol.
func (d *Deriver) HandleInstrumentEvent(ctx context.Context, event market.InstrumentChangeEvent) error {
	symbol := strings.ToUpper(strings.TrimSpace(event.Symbol))
	if symbol == "" {
		return nil
	}
	key := pendingKey(event.Exchange, event.SourceMarket, symbol)
	nowMS := event.EventTSMS
	if nowMS <= 0 {
		nowMS = market.NowMS()
	}
	d.mu.Lock()
	if current, exists := d.pending[key]; exists {
		current.lastEventMS = nowMS
		current.state = lifecycleState(event.EventType)
		current.raw = append(current.raw[:0], event.CurrentJSON...)
		if event.EventType == market.InstrumentEventResumed {
			current.resumeMS = nowMS
		}
		if d.registry != nil {
			d.registry.Touch(event.Exchange, symbol, nowMS)
		}
		snapshot := *current
		d.mu.Unlock()
		return d.persistPending(ctx, snapshot, "", 0, 0)
	}
	pe := &pendingEvent{
		actionID:     corporateActionID(event.Exchange, event.SourceMarket, symbol, nowMS),
		exchange:     strings.ToLower(strings.TrimSpace(event.Exchange)),
		sourceMarket: event.SourceMarket,
		symbol:       symbol,
		eventType:    event.EventType,
		firstSeenMS:  nowMS,
		lastEventMS:  nowMS,
		state:        lifecycleState(event.EventType),
		raw:          append(json.RawMessage(nil), event.CurrentJSON...),
	}
	if event.EventType == market.InstrumentEventResumed {
		pe.resumeMS = nowMS
	}
	d.pending[key] = pe
	if d.registry != nil {
		// Open a pending real-time window immediately so bars mid-rebase are
		// flagged before the factor is confirmed.
		d.registry.MarkActive(event.Exchange, symbol, nowMS)
	}
	snapshot := *pe
	d.mu.Unlock()
	log.Printf("adjustment deriver queued symbol=%s event=%s", symbol, event.EventType)
	return d.persistPending(ctx, snapshot, "", 0, 0)
}

// Run drives the derivation loop until ctx is cancelled.
func (d *Deriver) Run(ctx context.Context) error {
	if err := d.restorePending(ctx); err != nil {
		return err
	}
	ticker := time.NewTicker(d.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			d.processPending(ctx, market.NowMS())
		}
	}
}

func (d *Deriver) processPending(ctx context.Context, nowMS int64) {
	d.expireLongRunning(ctx, nowMS)
	for _, pe := range d.readyEvents(nowMS) {
		if ctx.Err() != nil {
			return
		}
		done, countAttempt := d.deriveOne(ctx, pe, nowMS)
		d.mu.Lock()
		if done {
			delete(d.pending, pendingKey(pe.exchange, pe.sourceMarket, pe.symbol))
		} else if countAttempt {
			current, ok := d.pending[pendingKey(pe.exchange, pe.sourceMarket, pe.symbol)]
			if !ok {
				d.mu.Unlock()
				continue
			}
			current.attempts++
			current.lastEventMS = nowMS
			if current.attempts >= d.cfg.MaxAttempts {
				log.Printf("adjustment deriver gave up symbol=%s after %d attempts", pe.symbol, current.attempts)
				snapshot := *current
				delete(d.pending, pendingKey(pe.exchange, pe.sourceMarket, pe.symbol))
				d.mu.Unlock()
				_ = d.persistPending(ctx, snapshot, market.CorporateActionStateManualReview, 0, 0)
				continue
			}
			snapshot := *current
			d.mu.Unlock()
			_ = d.persistPending(ctx, snapshot, "", 0, 0)
			continue
		}
		d.mu.Unlock()
	}
}

// readyEvents returns a snapshot of pending events whose confirmation delay has
// elapsed, so the scan runs without holding the lock.
func (d *Deriver) readyEvents(nowMS int64) []pendingEvent {
	confirmMS := d.cfg.ConfirmDelay.Milliseconds()
	d.mu.Lock()
	defer d.mu.Unlock()
	ready := make([]pendingEvent, 0, len(d.pending))
	for _, pe := range d.pending {
		if d.registry != nil {
			d.registry.Touch(pe.exchange, pe.symbol, nowMS)
		}
		if pe.exchange == "binance" && pe.state != market.CorporateActionStateResumed {
			continue
		}
		if nowMS-pe.firstSeenMS >= confirmMS {
			ready = append(ready, *pe)
		}
	}
	return ready
}

// deriveOne attempts to derive and persist the factor for one candidate.
// It returns true when the candidate is resolved (success, duplicate, or a
// prior-factor conflict) and should be dropped; false to retry later.
func (d *Deriver) deriveOne(ctx context.Context, pe pendingEvent, nowMS int64) (bool, bool) {
	startMS := pe.firstSeenMS - d.cfg.Lookback.Milliseconds()
	bars := d.loadBars(ctx, pe.exchange, pe.sourceMarket, pe.symbol, startMS, nowMS)
	var derivation Derivation
	var ok bool
	if pe.exchange == "binance" && pe.resumeMS > 0 {
		derivation, ok = deriveAtResumeBoundary(bars, pe.resumeMS)
	} else {
		derivation, ok = DeriveBackwardFactor(bars, d.cfg.MinMovePct)
	}
	if !ok {
		// No in-symbol gap: a freshly renamed instId only has post-rename bars,
		// so fall back to a cross-symbol rename factor if a predecessor exists.
		derivation, ok = d.deriveRename(ctx, pe, bars)
		if !ok {
			return false, true
		}
	}

	// Compose the new event with any prior corporate actions on this symbol,
	// recovering the incremental ledger from the stored cumulative segments so
	// multiple splits/dividends adjust the history correctly (layer D).
	existing, err := d.store.ListAdjustmentFactors(ctx, pe.exchange, pe.sourceMarket, pe.symbol,
		market.PriceModeBackwardAdjusted)
	if err != nil {
		log.Printf("adjustment deriver factor lookup failed symbol=%s: %v", pe.symbol, err)
		return false, true
	}
	ledger := ReconstructLedger(existing)
	if HasEventAt(ledger, derivation.BoundaryMS) {
		log.Printf("adjustment deriver already has factor symbol=%s boundary=%d", pe.symbol, derivation.BoundaryMS)
		_ = d.persistPending(ctx, pe, market.CorporateActionStateFactor, derivation.BoundaryMS, 0)
		return true, false
	}

	// Cross-check the derived ratio against the official announcement; a
	// positive contradiction blocks the write for manual review.
	accepted, retry, announcedRatio := d.verifyAgainstAnnouncement(ctx, pe, derivation)
	if !accepted {
		if !retry {
			_ = d.persistPending(ctx, pe, market.CorporateActionStateManualReview, derivation.BoundaryMS, 0)
		}
		return !retry, false
	}
	if announcedRatio > 0 {
		derivation.Ratio = announcedRatio
		derivation.PriceMultiplier = 1 / announcedRatio
		derivation.VolumeMultiplier = announcedRatio
	}

	ledger = append(ledger, EventFromDerivation(derivation, pe.eventType))

	base := market.AdjustmentFactor{
		Provider:        providerForExchange(pe.exchange),
		ProviderVersion: ProviderVersion,
		Exchange:        pe.exchange,
		SourceMarket:    pe.sourceMarket,
		Symbol:          pe.symbol,
		EventType:       pe.eventType,
		Raw:             derivationRaw(derivation),
	}
	segments := CumulativeBackwardSegments(base, ledger)
	if err := d.store.ReplaceAdjustmentFactors(ctx, pe.exchange, pe.sourceMarket, pe.symbol,
		market.PriceModeBackwardAdjusted, segments); err != nil {
		log.Printf("adjustment deriver replace failed symbol=%s: %v", pe.symbol, err)
		return false, true
	}
	if d.registry != nil {
		// Switch the real-time window from the pending heuristic to exact
		// crossing-bar suppression now that the boundary is known.
		d.registry.Resolve(pe.exchange, pe.symbol, derivation.BoundaryMS)
	}
	_ = d.persistPending(ctx, pe, market.CorporateActionStateFactor, derivation.BoundaryMS, announcedRatio)
	log.Printf("adjustment deriver wrote factor symbol=%s boundary=%d events=%d ratio=%.4f price_mult=%.6f vol_mult=%.4f",
		pe.symbol, derivation.BoundaryMS, len(ledger), derivation.Ratio, derivation.PriceMultiplier, derivation.VolumeMultiplier)
	return true, false
}

// deriveAtResumeBoundary locates Binance's first active one-minute bar after
// the contract returns to continuous trading. Its observed ratio is evidence
// only; verifyAgainstAnnouncement replaces it with the official scale factor.
func deriveAtResumeBoundary(bars []market.Bar, resumeMS int64) (Derivation, bool) {
	if len(bars) < 2 || resumeMS <= 0 {
		return Derivation{}, false
	}
	minuteMS := int64(time.Minute / time.Millisecond)
	resumeMinute := resumeMS - resumeMS%minuteMS
	for i := 1; i < len(bars); i++ {
		after := bars[i]
		if after.StartMS < resumeMinute || after.Volume <= 0 || after.OpenPrice <= 0 {
			continue
		}
		for j := i - 1; j >= 0; j-- {
			before := bars[j]
			if before.StartMS >= after.StartMS || before.ClosePrice <= 0 || before.Volume <= 0 {
				continue
			}
			ratio := before.ClosePrice / after.OpenPrice
			if ratio <= 0 || math.IsNaN(ratio) || math.IsInf(ratio, 0) {
				return Derivation{}, false
			}
			return Derivation{
				BoundaryMS: after.StartMS, CloseBefore: before.ClosePrice, OpenAfter: after.OpenPrice,
				Ratio: ratio, PriceMultiplier: 1 / ratio, VolumeMultiplier: ratio,
			}, true
		}
	}
	return Derivation{}, false
}

// verifyAgainstAnnouncement returns false when the derived ratio contradicts
// the official announcement (or, in strict mode, when no announcement is
// found). It is lenient by default: a missing announcement or an unparseable
// ratio does not block the write, since announcement matching/parsing is
// best-effort and announcements may lag the event.
func (d *Deriver) verifyAgainstAnnouncement(ctx context.Context, pe pendingEvent, derivation Derivation) (accepted bool, retry bool, authoritativeRatio float64) {
	if d.verifier == nil {
		return pe.exchange != "binance", pe.exchange == "binance", 0
	}
	found, ratio, hasRatio := d.verifier.VerifyCorporateAction(ctx, pe.exchange, pe.sourceMarket, pe.symbol, derivation.BoundaryMS)
	if pe.exchange == "binance" {
		if !found || !hasRatio || ratio <= 0 {
			log.Printf("adjustment deriver waiting symbol=%s: Binance announcement ratio unavailable", pe.symbol)
			return false, true, 0
		}
		log.Printf("adjustment deriver using authoritative Binance ratio symbol=%s announced=%.4f observed_gap=%.4f",
			pe.symbol, ratio, derivation.Ratio)
		return true, false, ratio
	}
	if hasRatio && ratio > 0 {
		rel := math.Abs(derivation.Ratio-ratio) / ratio
		if rel > d.cfg.AnnouncementTolerancePct {
			log.Printf("adjustment deriver REJECT symbol=%s derived_ratio=%.4f announced_ratio=%.4f rel=%.3f > tol=%.3f (manual review)",
				pe.symbol, derivation.Ratio, ratio, rel, d.cfg.AnnouncementTolerancePct)
			return false, false, 0
		}
		log.Printf("adjustment deriver ratio verified symbol=%s derived=%.4f announced=%.4f", pe.symbol, derivation.Ratio, ratio)
		return true, false, 0
	}
	if !found {
		if d.cfg.RequireAnnouncement {
			log.Printf("adjustment deriver REJECT symbol=%s: no matching announcement (strict mode)", pe.symbol)
			return false, false, 0
		}
		log.Printf("adjustment deriver unverified symbol=%s: no matching announcement, proceeding", pe.symbol)
		return true, false, 0
	}
	log.Printf("adjustment deriver announcement confirmed symbol=%s (ratio unparsed), proceeding", pe.symbol)
	return true, false, 0
}

func (d *Deriver) expireLongRunning(ctx context.Context, nowMS int64) {
	maxWaitMS := d.cfg.MaxWait.Milliseconds()
	d.mu.Lock()
	var expired []pendingEvent
	for key, pe := range d.pending {
		if nowMS-pe.firstSeenMS < maxWaitMS {
			continue
		}
		log.Printf("adjustment deriver MANUAL_REVIEW symbol=%s event=%s after waiting %s", pe.symbol, pe.eventType, d.cfg.MaxWait)
		expired = append(expired, *pe)
		delete(d.pending, key)
	}
	d.mu.Unlock()
	for _, pe := range expired {
		_ = d.persistPending(ctx, pe, market.CorporateActionStateManualReview, 0, 0)
	}
}

func lifecycleState(eventType string) string {
	switch eventType {
	case market.InstrumentEventResumed:
		return market.CorporateActionStateResumed
	case market.InstrumentEventCancelOnly:
		return market.CorporateActionStateCancelOnly
	case market.InstrumentEventSuspended:
		return market.CorporateActionStateHalt
	default:
		return market.CorporateActionStateDiscovered
	}
}

func (d *Deriver) restorePending(ctx context.Context) error {
	if d.events == nil {
		return nil
	}
	events, err := d.events.ListOpenCorporateActionEvents(ctx)
	if err != nil {
		return fmt.Errorf("restore corporate action events: %w", err)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, event := range events {
		key := pendingKey(event.Exchange, event.SourceMarket, event.Symbol)
		if _, exists := d.pending[key]; exists {
			continue
		}
		d.pending[key] = &pendingEvent{
			actionID: event.ActionID, exchange: strings.ToLower(event.Exchange), sourceMarket: event.SourceMarket,
			symbol: strings.ToUpper(event.Symbol), eventType: event.EventType, firstSeenMS: event.FirstSeenMS,
			lastEventMS: event.LastEventMS, resumeMS: event.ResumeMS, state: event.State, attempts: event.Attempts,
			raw: append(json.RawMessage(nil), event.Raw...),
		}
		if d.registry != nil {
			d.registry.MarkActive(event.Exchange, event.Symbol, event.FirstSeenMS)
			d.registry.Touch(event.Exchange, event.Symbol, market.NowMS())
		}
	}
	if len(events) > 0 {
		log.Printf("adjustment deriver restored pending events=%d", len(events))
	}
	return nil
}

func (d *Deriver) persistPending(ctx context.Context, pe pendingEvent, state string, boundaryMS int64, announcedRatio float64) error {
	if d.events == nil {
		return nil
	}
	if state == "" {
		state = pe.state
	}
	event := market.CorporateActionEvent{
		ActionID: pe.actionID, Exchange: pe.exchange, SourceMarket: pe.sourceMarket, Symbol: pe.symbol,
		EventType: pe.eventType, State: state, FirstSeenMS: pe.firstSeenMS, LastEventMS: pe.lastEventMS,
		ResumeMS: pe.resumeMS, BoundaryMS: boundaryMS, AnnouncedRatio: announcedRatio, Attempts: pe.attempts,
		Raw: append(json.RawMessage(nil), pe.raw...), UpdatedAtMS: market.NowMS(),
	}
	if err := d.events.UpsertCorporateActionEvent(ctx, event); err != nil {
		log.Printf("adjustment deriver persist lifecycle failed symbol=%s state=%s: %v", pe.symbol, state, err)
		return err
	}
	return nil
}

func corporateActionID(exchange string, sourceMarket string, symbol string, firstSeenMS int64) string {
	return fmt.Sprintf("%s|%s|%s|%d", strings.ToLower(strings.TrimSpace(exchange)), sourceMarket,
		strings.ToUpper(strings.TrimSpace(symbol)), firstSeenMS)
}

func providerForExchange(exchange string) string {
	if strings.EqualFold(strings.TrimSpace(exchange), "binance") {
		return BinanceProviderName
	}
	return ProviderName
}

// deriveRename computes a cross-symbol rename factor from the predecessor's
// last close and the successor's first open. Returns false when no predecessor
// is known or the boundary cannot be located.
func (d *Deriver) deriveRename(ctx context.Context, pe pendingEvent, successorBars []market.Bar) (Derivation, bool) {
	if d.aliases == nil {
		return Derivation{}, false
	}
	predecessor, predSource, _, ok := d.aliases.Lookup(pe.exchange, pe.symbol)
	if !ok {
		return Derivation{}, false
	}
	if predSource == "" {
		predSource = pe.sourceMarket
	}
	predBars := d.loadBars(ctx, pe.exchange, predSource, predecessor,
		pe.firstSeenMS-d.cfg.Lookback.Milliseconds(), market.NowMS())
	derivation, ok := DeriveRenameFactor(predBars, successorBars)
	if !ok {
		return Derivation{}, false
	}
	log.Printf("adjustment deriver rename %s <- %s boundary=%d ratio=%.4f",
		pe.symbol, predecessor, derivation.BoundaryMS, derivation.Ratio)
	return derivation, true
}

// loadBars returns the bars used for derivation. When an official REST source
// is configured it prefers authoritative exchange data over the local store,
// and reports any local bars that have diverged from official — so the factor
// is computed from trusted data and store drift is surfaced. It falls back to
// the local store if official data is unavailable.
func (d *Deriver) loadBars(ctx context.Context, exchange string, sourceMarket string, symbol string, startMS int64, endMS int64) []market.Bar {
	var local []market.Bar
	if d.source != nil {
		if bars, err := d.source.BarsInRange(ctx, exchange, symbol, deriverTimeframe, startMS, endMS); err != nil {
			log.Printf("adjustment deriver load local bars failed symbol=%s: %v", symbol, err)
		} else {
			local = bars
		}
	}
	if d.official == nil {
		return local
	}
	official, err := d.official.FetchOfficialKlines(ctx, exchange, sourceMarket, symbol, deriverTimeframe, startMS, endMS)
	if err != nil {
		log.Printf("adjustment deriver official fetch failed symbol=%s: %v (using local)", symbol, err)
		return local
	}
	if len(official) < 2 {
		return local
	}
	d.reportDivergence(symbol, local, official)
	return official
}

// reportDivergence logs how many local bars disagree with official data beyond
// the configured tolerance, at the boundaries the derivation cares about.
func (d *Deriver) reportDivergence(symbol string, local []market.Bar, official []market.Bar) {
	if len(local) == 0 {
		return
	}
	localByStart := make(map[int64]float64, len(local))
	for _, b := range local {
		localByStart[b.StartMS] = b.ClosePrice
	}
	diverged := 0
	for _, ob := range official {
		lc, ok := localByStart[ob.StartMS]
		if !ok || ob.ClosePrice <= 0 {
			continue
		}
		if math.Abs(lc-ob.ClosePrice)/ob.ClosePrice > d.cfg.OfficialDivergencePct {
			diverged++
		}
	}
	if diverged > 0 {
		log.Printf("adjustment deriver local/official divergence symbol=%s bars=%d (deriving from official)", symbol, diverged)
	}
}

// PendingCount reports how many candidates are awaiting derivation (for tests
// and observability).
func (d *Deriver) PendingCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.pending)
}

func derivationRaw(d Derivation) json.RawMessage {
	body, err := json.Marshal(map[string]any{
		"method":       "price_gap",
		"boundary_ms":  d.BoundaryMS,
		"close_before": d.CloseBefore,
		"open_after":   d.OpenAfter,
		"ratio":        d.Ratio,
	})
	if err != nil {
		return nil
	}
	return body
}

func pendingKey(exchange string, sourceMarket string, symbol string) string {
	return strings.ToLower(strings.TrimSpace(exchange)) + "|" + sourceMarket + "|" + strings.ToUpper(strings.TrimSpace(symbol))
}
