package retention

import (
	"testing"
	"time"
)

func TestRuleForTimeframeBuckets(t *testing.T) {
	cases := map[string]Rule{
		"1m":  {Timeframe: "1m", KeepDays: 15},
		"5m":  {Timeframe: "5m", KeepDays: 90},
		"30m": {Timeframe: "30m", KeepDays: 90},
		"1H":  {Timeframe: "1H", KeepDays: 180},
		"12H": {Timeframe: "12H", KeepDays: 180},
		"1D":  {Timeframe: "1D", KeepForever: true},
		"3M":  {Timeframe: "3M", KeepForever: true},
	}
	for tf, expected := range cases {
		got := RuleFor(tf)
		if got != expected {
			t.Fatalf("tf=%s expected %+v got %+v", tf, expected, got)
		}
	}
}

func TestCutoffMS(t *testing.T) {
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	cutoff, ok := CutoffMS(RuleFor("1m"), now)
	if !ok {
		t.Fatal("expected cutoff")
	}
	expected := time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC).UnixMilli()
	if cutoff != expected {
		t.Fatalf("expected %d got %d", expected, cutoff)
	}
	if _, ok := CutoffMS(RuleFor("1D"), now); ok {
		t.Fatal("expected keep-forever rule to have no cutoff")
	}
}
