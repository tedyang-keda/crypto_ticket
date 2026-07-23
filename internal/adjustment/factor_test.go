package adjustment

import (
	"math"
	"testing"

	"crypto-ticket/internal/market"
)

func bar(startMS int64, open float64, closePrice float64) market.Bar {
	return market.Bar{StartMS: startMS, OpenPrice: open, ClosePrice: closePrice, IsFinal: true}
}

func TestDeriveBackwardFactorSplit(t *testing.T) {
	bars := []market.Bar{
		bar(0, 2000, 2000),
		bar(60_000, 2000, 2000),
		bar(120_000, 100, 100), // 20:1 split boundary
		bar(180_000, 100, 100),
	}
	d, ok := DeriveBackwardFactor(bars, 0.05)
	if !ok {
		t.Fatal("expected a derivation")
	}
	if d.BoundaryMS != 120_000 {
		t.Fatalf("boundary = %d, want 120000", d.BoundaryMS)
	}
	if math.Abs(d.Ratio-20) > 1e-9 {
		t.Fatalf("ratio = %f, want 20", d.Ratio)
	}
	if math.Abs(d.PriceMultiplier-0.05) > 1e-9 {
		t.Fatalf("price multiplier = %f, want 0.05", d.PriceMultiplier)
	}
	if math.Abs(d.VolumeMultiplier-20) > 1e-9 {
		t.Fatalf("volume multiplier = %f, want 20", d.VolumeMultiplier)
	}
}

func TestDeriveBackwardFactorIgnoresNoise(t *testing.T) {
	bars := []market.Bar{
		bar(0, 100, 101),
		bar(60_000, 101, 100),
		bar(120_000, 100, 102),
	}
	if _, ok := DeriveBackwardFactor(bars, 0.05); ok {
		t.Fatal("ordinary volatility should not be treated as a corporate action")
	}
}

func TestDeriveBackwardFactorNeedsTwoBars(t *testing.T) {
	if _, ok := DeriveBackwardFactor([]market.Bar{bar(0, 100, 100)}, 0.05); ok {
		t.Fatal("single bar cannot yield a boundary")
	}
}

func TestCumulativeBackwardSegmentsSingleEvent(t *testing.T) {
	base := market.AdjustmentFactor{Exchange: "okx", Symbol: "MUU-USDT-SWAP"}
	segments := CumulativeBackwardSegments(base, []LedgerEvent{
		{EffectiveMS: 120_000, PriceMultiplier: 0.05, VolumeMultiplier: 20},
	})
	if len(segments) != 2 {
		t.Fatalf("expected 2 segments, got %d", len(segments))
	}
	pre, post := segments[0], segments[1]
	if pre.EffectiveFromMS != 0 || pre.EffectiveToMS != 119_999 {
		t.Fatalf("pre window = [%d,%d], want [0,119999]", pre.EffectiveFromMS, pre.EffectiveToMS)
	}
	if pre.PriceMultiplier != 0.05 || pre.VolumeMultiplier != 20 {
		t.Fatalf("pre multipliers = %f/%f", pre.PriceMultiplier, pre.VolumeMultiplier)
	}
	if pre.AdjMode != market.PriceModeBackwardAdjusted {
		t.Fatalf("pre adj mode = %q", pre.AdjMode)
	}
	if post.EffectiveFromMS != 120_000 || post.EffectiveToMS != 0 {
		t.Fatalf("post window = [%d,%d], want [120000,0]", post.EffectiveFromMS, post.EffectiveToMS)
	}
	if post.PriceMultiplier != 1 || post.VolumeMultiplier != 1 {
		t.Fatalf("post multipliers should be unit, got %f/%f", post.PriceMultiplier, post.VolumeMultiplier)
	}
}

func TestCumulativeBackwardSegmentsMultiEvent(t *testing.T) {
	base := market.AdjustmentFactor{Exchange: "okx", Symbol: "MUU-USDT-SWAP"}
	// Two splits: 2:1 at 60000, then 20:1 at 120000.
	segments := CumulativeBackwardSegments(base, []LedgerEvent{
		{EffectiveMS: 120_000, PriceMultiplier: 0.05, VolumeMultiplier: 20},
		{EffectiveMS: 60_000, PriceMultiplier: 0.5, VolumeMultiplier: 2}, // out of order on purpose
	})
	if len(segments) != 3 {
		t.Fatalf("expected 3 segments, got %d", len(segments))
	}
	// [0,60000) compounds both: price 0.5*0.05=0.025, vol 2*20=40.
	if segments[0].EffectiveFromMS != 0 || segments[0].EffectiveToMS != 59_999 ||
		math.Abs(segments[0].PriceMultiplier-0.025) > 1e-9 || math.Abs(segments[0].VolumeMultiplier-40) > 1e-9 {
		t.Fatalf("segment0 = %+v", segments[0])
	}
	// [60000,120000) carries only the later split: price 0.05, vol 20.
	if segments[1].EffectiveFromMS != 60_000 || segments[1].EffectiveToMS != 119_999 ||
		math.Abs(segments[1].PriceMultiplier-0.05) > 1e-9 {
		t.Fatalf("segment1 = %+v", segments[1])
	}
	// [120000,∞) is the current regime: unit.
	if segments[2].EffectiveFromMS != 120_000 || segments[2].EffectiveToMS != 0 ||
		segments[2].PriceMultiplier != 1 {
		t.Fatalf("segment2 = %+v", segments[2])
	}
}

func TestDeriveRenameFactor(t *testing.T) {
	// SPACEX last close 2504 before the rename; SPCX first open 200 after it.
	pred := []market.Bar{bar(0, 2500, 2504), bar(60_000, 2504, 2504)}
	succ := []market.Bar{bar(120_000, 200, 205), bar(180_000, 205, 210)}
	d, ok := DeriveRenameFactor(pred, succ)
	if !ok {
		t.Fatal("expected a rename derivation")
	}
	if d.BoundaryMS != 120_000 {
		t.Fatalf("boundary = %d, want 120000", d.BoundaryMS)
	}
	if math.Abs(d.Ratio-12.52) > 1e-9 {
		t.Fatalf("ratio = %f, want 12.52", d.Ratio)
	}
	if math.Abs(d.PriceMultiplier-(200.0/2504.0)) > 1e-9 {
		t.Fatalf("price multiplier = %f", d.PriceMultiplier)
	}
}

func TestDeriveRenameFactorNeedsBothSides(t *testing.T) {
	succ := []market.Bar{bar(120_000, 200, 205)}
	if _, ok := DeriveRenameFactor(nil, succ); ok {
		t.Fatal("missing predecessor bars should fail")
	}
	if _, ok := DeriveRenameFactor([]market.Bar{bar(0, 2500, 2504)}, nil); ok {
		t.Fatal("missing successor bars should fail")
	}
}

func TestReconstructLedgerRoundTrip(t *testing.T) {
	base := market.AdjustmentFactor{Exchange: "okx", Symbol: "MUU-USDT-SWAP"}
	events := []LedgerEvent{
		{EffectiveMS: 60_000, PriceMultiplier: 0.5, VolumeMultiplier: 2},
		{EffectiveMS: 120_000, PriceMultiplier: 0.05, VolumeMultiplier: 20},
	}
	segments := CumulativeBackwardSegments(base, events)
	recovered := ReconstructLedger(segments)
	if len(recovered) != 2 {
		t.Fatalf("expected 2 recovered events, got %d", len(recovered))
	}
	for i, want := range events {
		got := recovered[i]
		if got.EffectiveMS != want.EffectiveMS ||
			math.Abs(got.PriceMultiplier-want.PriceMultiplier) > 1e-9 ||
			math.Abs(got.VolumeMultiplier-want.VolumeMultiplier) > 1e-9 {
			t.Fatalf("event %d = %+v, want %+v", i, got, want)
		}
	}
}
