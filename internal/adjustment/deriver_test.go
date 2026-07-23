package adjustment

import (
	"context"
	"math"
	"testing"
	"time"

	"crypto-ticket/internal/market"
	"crypto-ticket/internal/storage"
)

type fakeStore struct {
	bars     []market.Bar
	existing []market.AdjustmentFactor
	replaced []market.AdjustmentFactor
	replaces int
}

func (f *fakeStore) BarsInRange(_ context.Context, _ string, _ string, _ string, _ int64, _ int64) ([]market.Bar, error) {
	return f.bars, nil
}

func (f *fakeStore) ListAdjustmentFactors(_ context.Context, _ string, _ string, _ string, _ string) ([]market.AdjustmentFactor, error) {
	return f.existing, nil
}

func (f *fakeStore) ReplaceAdjustmentFactors(_ context.Context, _ string, _ string, _ string, _ string, factors []market.AdjustmentFactor) error {
	f.replaces++
	f.replaced = factors
	f.existing = factors // subsequent derivations compose on top of this
	return nil
}

type fakeOfficial struct {
	bars  []market.Bar
	calls int
}

func (f *fakeOfficial) FetchOfficialKlines(_ context.Context, _ string, _ string, _ string, _ string, _ int64, _ int64) ([]market.Bar, error) {
	f.calls++
	return f.bars, nil
}

type fakeVerifier struct {
	found    bool
	ratio    float64
	hasRatio bool
	calls    int
}

func (f *fakeVerifier) VerifyCorporateAction(_ context.Context, _ string, _ string, _ string, _ int64) (bool, float64, bool) {
	f.calls++
	return f.found, f.ratio, f.hasRatio
}

func splitBars() []market.Bar {
	return []market.Bar{
		bar(0, 2000, 2000),
		bar(60_000, 2000, 2000),
		bar(120_000, 100, 100),
		bar(180_000, 100, 100),
	}
}

func newDeriver(store *fakeStore) *Deriver {
	return New(store, store, Config{ConfirmDelay: time.Minute, MinMovePct: 0.05})
}

func rebaseEvent() market.InstrumentChangeEvent {
	return market.InstrumentChangeEvent{
		Exchange:     "okx",
		SourceMarket: "okx:SWAP",
		Symbol:       "MUU-USDT-SWAP",
		EventType:    market.InstrumentEventRebase,
	}
}

func TestDeriverEnqueueDedup(t *testing.T) {
	d := newDeriver(&fakeStore{bars: splitBars()})
	_ = d.HandleInstrumentEvent(context.Background(), rebaseEvent())
	_ = d.HandleInstrumentEvent(context.Background(), rebaseEvent())
	if d.PendingCount() != 1 {
		t.Fatalf("expected 1 pending after dedup, got %d", d.PendingCount())
	}
}

func TestDeriverWaitsForConfirmDelay(t *testing.T) {
	store := &fakeStore{bars: splitBars()}
	d := newDeriver(store)
	_ = d.HandleInstrumentEvent(context.Background(), rebaseEvent())

	// Immediately: confirm delay not elapsed, nothing derived.
	d.processPending(context.Background(), market.NowMS())
	if store.replaces != 0 {
		t.Fatalf("should not derive before confirm delay, replaces=%d", store.replaces)
	}
	if d.PendingCount() != 1 {
		t.Fatalf("candidate should remain pending, got %d", d.PendingCount())
	}
}

func TestDeriverWritesFactorAfterConfirm(t *testing.T) {
	store := &fakeStore{bars: splitBars()}
	d := newDeriver(store)
	_ = d.HandleInstrumentEvent(context.Background(), rebaseEvent())

	future := market.NowMS() + (2 * time.Minute).Milliseconds()
	d.processPending(context.Background(), future)

	if store.replaces != 1 || len(store.replaced) != 2 {
		t.Fatalf("expected one replace of two segments, replaces=%d segments=%d", store.replaces, len(store.replaced))
	}
	if d.PendingCount() != 0 {
		t.Fatalf("candidate should be resolved, pending=%d", d.PendingCount())
	}
	pre := store.replaced[0]
	if pre.Symbol != "MUU-USDT-SWAP" || pre.Provider != ProviderName || pre.PriceMultiplier != 0.05 {
		t.Fatalf("unexpected pre-segment: %+v", pre)
	}
}

func TestDeriverPrefersOfficialData(t *testing.T) {
	// Local store is flat (no gap) and would yield no factor; the official
	// source carries the real split, so the factor must come from official.
	store := &fakeStore{bars: []market.Bar{bar(0, 100, 100), bar(60_000, 100, 100)}}
	official := &fakeOfficial{bars: splitBars()}
	d := newDeriver(store)
	d.SetOfficialSource(official)

	_ = d.HandleInstrumentEvent(context.Background(), rebaseEvent())
	d.processPending(context.Background(), market.NowMS()+(2*time.Minute).Milliseconds())

	if official.calls == 0 {
		t.Fatal("official source should have been consulted")
	}
	if store.replaces != 1 || len(store.replaced) != 2 {
		t.Fatalf("factor should be derived from official split data, replaces=%d segments=%d", store.replaces, len(store.replaced))
	}
	if store.replaced[0].PriceMultiplier != 0.05 {
		t.Fatalf("factor should reflect official 20:1 ratio, got price_mult=%f", store.replaced[0].PriceMultiplier)
	}
}

func runDeriveWithVerifier(t *testing.T, verifier *fakeVerifier, strict bool) *fakeStore {
	t.Helper()
	store := &fakeStore{bars: splitBars()} // 20:1 gap -> derived ratio 20
	d := New(store, store, Config{ConfirmDelay: time.Minute, MinMovePct: 0.05, RequireAnnouncement: strict})
	d.SetAnnouncementVerifier(verifier)
	_ = d.HandleInstrumentEvent(context.Background(), rebaseEvent())
	d.processPending(context.Background(), market.NowMS()+(2*time.Minute).Milliseconds())
	if verifier.calls == 0 {
		t.Fatal("verifier should have been consulted")
	}
	return store
}

func TestDeriverAnnouncementMatchWrites(t *testing.T) {
	// Announced 20:1 agrees with the derived ratio 20 -> write.
	store := runDeriveWithVerifier(t, &fakeVerifier{found: true, ratio: 20, hasRatio: true}, false)
	if store.replaces != 1 {
		t.Fatalf("matching ratio should be written, replaces=%d", store.replaces)
	}
}

func TestDeriverAnnouncementMismatchRejects(t *testing.T) {
	// Announced 2:1 contradicts the derived ratio 20 -> reject, no write.
	store := runDeriveWithVerifier(t, &fakeVerifier{found: true, ratio: 2, hasRatio: true}, false)
	if store.replaces != 0 {
		t.Fatalf("contradicting ratio must be rejected, replaces=%d", store.replaces)
	}
}

func TestDeriverNoAnnouncementLenientWrites(t *testing.T) {
	// No announcement found, lenient mode -> still write.
	store := runDeriveWithVerifier(t, &fakeVerifier{found: false}, false)
	if store.replaces != 1 {
		t.Fatalf("lenient mode should write when unverified, replaces=%d", store.replaces)
	}
}

func TestDeriverNoAnnouncementStrictRejects(t *testing.T) {
	// No announcement found, strict mode -> reject.
	store := runDeriveWithVerifier(t, &fakeVerifier{found: false}, true)
	if store.replaces != 0 {
		t.Fatalf("strict mode should reject when unverified, replaces=%d", store.replaces)
	}
}

func TestDeriverComposesSecondEventCumulatively(t *testing.T) {
	// A prior 2:1 split at t=60000 is already recorded as cumulative segments.
	base := market.AdjustmentFactor{Provider: ProviderName, Exchange: "okx", SourceMarket: "okx:SWAP", Symbol: "MUU-USDT-SWAP"}
	prior := CumulativeBackwardSegments(base, []LedgerEvent{{EffectiveMS: 60_000, PriceMultiplier: 0.5, VolumeMultiplier: 2}})
	store := &fakeStore{bars: splitBars(), existing: prior} // new split adds boundary at 120000

	d := newDeriver(store)
	_ = d.HandleInstrumentEvent(context.Background(), rebaseEvent())
	d.processPending(context.Background(), market.NowMS()+(2*time.Minute).Milliseconds())

	if store.replaces != 1 || len(store.replaced) != 3 {
		t.Fatalf("two events should yield three cumulative segments, replaces=%d segments=%d", store.replaces, len(store.replaced))
	}
	// Oldest segment carries the product of both splits: 0.5 * 0.05 = 0.025.
	oldest := store.replaced[0]
	if oldest.EffectiveFromMS != 0 || math.Abs(oldest.PriceMultiplier-0.025) > 1e-9 {
		t.Fatalf("oldest segment should compound both events, got %+v", oldest)
	}
}

func TestDeriverIdempotentWhenFactorAlreadyPresent(t *testing.T) {
	// Existing segments already contain an event at the boundary (120000).
	base := market.AdjustmentFactor{Provider: ProviderName, Exchange: "okx", SourceMarket: "okx:SWAP", Symbol: "MUU-USDT-SWAP"}
	prior := CumulativeBackwardSegments(base, []LedgerEvent{{EffectiveMS: 120_000, PriceMultiplier: 0.05, VolumeMultiplier: 20}})
	store := &fakeStore{bars: splitBars(), existing: prior}

	d := newDeriver(store)
	_ = d.HandleInstrumentEvent(context.Background(), rebaseEvent())
	d.processPending(context.Background(), market.NowMS()+(2*time.Minute).Milliseconds())

	if store.replaces != 0 {
		t.Fatalf("existing matching factor should be treated as done, replaces=%d", store.replaces)
	}
	if d.PendingCount() != 0 {
		t.Fatalf("candidate should be resolved, pending=%d", d.PendingCount())
	}
}

func TestBinanceWaitsForResumeWithoutConsumingAttempts(t *testing.T) {
	store := &fakeStore{bars: splitBars()}
	d := New(store, store, Config{ConfirmDelay: time.Minute, MaxAttempts: 1, MaxWait: 24 * time.Hour})
	event := market.InstrumentChangeEvent{
		Exchange: "binance", SourceMarket: "binance:um_futures", Symbol: "KORUUSDT",
		EventType: market.InstrumentEventSuspended, EventTSMS: market.NowMS(),
	}
	_ = d.HandleInstrumentEvent(context.Background(), event)
	d.processPending(context.Background(), event.EventTSMS+(2*time.Hour).Milliseconds())
	if d.PendingCount() != 1 || store.replaces != 0 {
		t.Fatalf("halted Binance event must remain pending without derivation: pending=%d replaces=%d", d.PendingCount(), store.replaces)
	}
	for _, pe := range d.pending {
		if pe.attempts != 0 {
			t.Fatalf("halt wait must not consume attempts, got %d", pe.attempts)
		}
	}
}

func TestBinanceUsesAnnouncementRatioAsAuthoritative(t *testing.T) {
	nowMS := market.NowMS()
	resumeMS := nowMS + (9 * time.Hour).Milliseconds()
	resumeMinute := resumeMS - resumeMS%60_000
	before := bar(resumeMinute-60_000, 481.11, 481.11)
	before.Volume = 5
	after := bar(resumeMinute, 22.68, 22.68)
	after.Volume = 10
	store := &fakeStore{bars: []market.Bar{before, after}}
	d := New(store, store, Config{ConfirmDelay: time.Minute, MinMovePct: 0.05})
	d.SetAnnouncementVerifier(&fakeVerifier{found: true, ratio: 20, hasRatio: true})
	_ = d.HandleInstrumentEvent(context.Background(), market.InstrumentChangeEvent{
		Exchange: "binance", SourceMarket: "binance:um_futures", Symbol: "KORUUSDT",
		EventType: market.InstrumentEventSuspended, EventTSMS: nowMS,
	})
	_ = d.HandleInstrumentEvent(context.Background(), market.InstrumentChangeEvent{
		Exchange: "binance", SourceMarket: "binance:um_futures", Symbol: "KORUUSDT",
		EventType: market.InstrumentEventResumed, EventTSMS: resumeMS,
	})
	d.processPending(context.Background(), nowMS+(9*time.Hour+2*time.Minute).Milliseconds())
	if store.replaces != 1 || len(store.replaced) != 2 {
		t.Fatalf("expected Binance factor write, replaces=%d factors=%d", store.replaces, len(store.replaced))
	}
	pre := store.replaced[0]
	if pre.Provider != BinanceProviderName || math.Abs(pre.PriceMultiplier-0.05) > 1e-9 || math.Abs(pre.VolumeMultiplier-20) > 1e-9 {
		t.Fatalf("announcement ratio must override observed gap: %+v", pre)
	}
}

func TestDeriveAtResumeBoundaryUsesFirstNonzeroBar(t *testing.T) {
	resumeMS := int64(600_123)
	bars := []market.Bar{
		{StartMS: 540_000, ClosePrice: 100, Volume: 2},
		{StartMS: 600_000, OpenPrice: 5, ClosePrice: 5, Volume: 0},
		{StartMS: 660_000, OpenPrice: 4.8, ClosePrice: 4.9, Volume: 3},
	}
	got, ok := deriveAtResumeBoundary(bars, resumeMS)
	if !ok || got.BoundaryMS != 660_000 || math.Abs(got.Ratio-(100.0/4.8)) > 1e-9 {
		t.Fatalf("unexpected resume boundary: %+v ok=%v", got, ok)
	}
}

func TestDeriverRestoresDurableBinanceLifecycle(t *testing.T) {
	store := storage.NewMemoryHistoricalStore()
	nowMS := market.NowMS()
	first := New(store, store, Config{})
	_ = first.HandleInstrumentEvent(context.Background(), market.InstrumentChangeEvent{
		Exchange: "binance", SourceMarket: "binance:um_futures", Symbol: "KORUUSDT",
		EventType: market.InstrumentEventSuspended, EventTSMS: nowMS,
	})
	_ = first.HandleInstrumentEvent(context.Background(), market.InstrumentChangeEvent{
		Exchange: "binance", SourceMarket: "binance:um_futures", Symbol: "KORUUSDT",
		EventType: market.InstrumentEventResumed, EventTSMS: nowMS + (9 * time.Hour).Milliseconds(),
	})

	restarted := New(store, store, Config{})
	if err := restarted.restorePending(context.Background()); err != nil {
		t.Fatal(err)
	}
	if restarted.PendingCount() != 1 {
		t.Fatalf("restart should restore one open lifecycle, got %d", restarted.PendingCount())
	}
	for _, pe := range restarted.pending {
		if pe.state != market.CorporateActionStateResumed || pe.resumeMS == 0 || pe.actionID == "" {
			t.Fatalf("unexpected restored lifecycle: %+v", pe)
		}
	}
}
