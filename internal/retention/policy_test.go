package retention

import (
	"testing"
	"time"
)

func TestRuleForTimeframeBuckets(t *testing.T) {
	cases := map[string]Rule{
		"1m":  {Timeframe: "1m", KeepDays: 7},
		"5m":  {Timeframe: "5m", KeepDays: 30},
		"15m": {Timeframe: "15m", KeepDays: 30},
		"30m": {Timeframe: "30m", KeepDays: 30},
		"1H":  {Timeframe: "1H", KeepDays: 180},
		"12H": {Timeframe: "12H", KeepDays: 180},
		"1D":  {Timeframe: "1D", KeepBars: 300},
		"3M":  {Timeframe: "3M", KeepBars: 300},
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
	expected := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC).UnixMilli()
	if cutoff != expected {
		t.Fatalf("expected %d got %d", expected, cutoff)
	}
	if _, ok := CutoffMS(RuleFor("1D"), now); ok {
		t.Fatal("expected count-retained rule to have no time cutoff")
	}
}

func TestBarWindowStartMSUsesBucketCalendar(t *testing.T) {
	end := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC).UnixMilli()
	weekly := time.Date(2020, 10, 19, 0, 0, 0, 0, time.UTC).UnixMilli()
	if got := BarWindowStartMS("1W", end, 300); got != weekly {
		t.Fatalf("unexpected weekly window start %d, want %d", got, weekly)
	}
	daily := time.Date(2025, 9, 29, 0, 0, 0, 0, time.UTC).UnixMilli()
	if got := BarWindowStartMS("1D", end, 300); got != daily {
		t.Fatalf("unexpected daily window start %d, want %d", got, daily)
	}
	monthly := time.Date(2001, 7, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	if got := BarWindowStartMS("1M", end, 300); got != monthly {
		t.Fatalf("unexpected monthly window start %d, want %d", got, monthly)
	}
}
