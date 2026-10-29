package gapfill

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"crypto-ticket/internal/timeframe"
)

func fullReport(t *testing.T, asOf time.Time, pairs ...map[string]any) string {
	t.Helper()
	b, err := json.Marshal(map[string]any{"generated_at": asOf.Format(time.RFC3339), "as_of": asOf.Format(time.RFC3339), "complete": true, "results": pairs})
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestLifecyclePlanIncludesMinuteInternalAndClosedTailTargets(t *testing.T) {
	start := testStart("15m")
	latest := start + 4*15*timeframe.MinuteMS
	series := map[string]any{"symbol": "BTCUSDT", "canonical_bars": 2, "first_canonical_ms": start, "last_canonical_ms": start + 2*15*timeframe.MinuteMS, "internal_missing_bars": 1, "gap_ranges": []any{map[string]any{"first_missing_ms": start + 15*timeframe.MinuteMS, "last_missing_ms": start + 15*timeframe.MinuteMS, "missing_bars": 1}}}
	report := fullReport(t, time.UnixMilli(timeframe.EndMS(latest, "15m")+120001), map[string]any{"exchange": "binance", "timeframe": "15m", "latest_completed_ms": latest, "series": []any{series}})
	plan, err := BuildPlanWithOptions(strings.NewReader(report), PlanOptions{Policies: map[string]SymbolPolicy{"binance:BTCUSDT": {Allowed: true}}, IncludeTail: true, IncludeNoData: true, EnforceRetention: true})
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, task := range plan.Tasks {
		counts[task.Kind] += len(task.TargetStarts)
		if _, err := validateTask(task, latest+2*timeframe.HourMS); err != nil {
			t.Fatal(err)
		}
	}
	if counts["internal"] != 1 || counts["tail"] != 2 || counts["no_data"] != 0 {
		t.Fatalf("unexpected kinds: %+v", counts)
	}
}

func TestLifecyclePlanRejectsUnsubscribedAndUnknownSymbolsBeforeExpanding(t *testing.T) {
	start := testStart("1H")
	series := []any{}
	for _, symbol := range []string{"DELISTED", "STOPPED", "UNKNOWN"} {
		series = append(series, map[string]any{"symbol": symbol, "internal_missing_bars": 1, "gap_ranges": []any{map[string]any{"first_missing_ms": start, "last_missing_ms": start, "missing_bars": 1}}})
	}
	report := fullReport(t, time.UnixMilli(start+3*timeframe.HourMS), map[string]any{"exchange": "okx", "timeframe": "1H", "series": series})
	plan, err := BuildPlanWithOptions(strings.NewReader(report), PlanOptions{Policies: map[string]SymbolPolicy{"okx:DELISTED": {Reason: "official_delisted"}, "okx:STOPPED": {Reason: "contract_filtered"}}, IncludeTail: true, IncludeNoData: true})
	if err != nil || len(plan.Tasks) != 0 || len(plan.Skipped) != 3 {
		t.Fatalf("plan=%+v error=%v", plan, err)
	}
	if plan.Skipped[2].Reason != "missing_official_subscription_snapshot" {
		t.Fatalf("unknown metadata was not excluded: %+v", plan.Skipped)
	}
}

func TestNoDataPlanUsesOfficialListingOrLowerHistoryNeverFirstSeen(t *testing.T) {
	start := testStart("1D")
	latest := start + 4*timeframe.DayMS
	asOf := time.UnixMilli(timeframe.EndMS(latest, "1D") + 120001)
	report := fullReport(t, asOf,
		map[string]any{"exchange": "okx", "timeframe": "1H", "latest_completed_ms": latest, "series": []any{map[string]any{"symbol": "LOWER", "canonical_bars": 1, "first_canonical_ms": start + 2*timeframe.HourMS, "last_canonical_ms": start + 2*timeframe.HourMS}}},
		map[string]any{"exchange": "okx", "timeframe": "1D", "latest_completed_ms": latest, "series": []any{map[string]any{"symbol": "LISTED", "registry_first_seen_ms": start - 999*timeframe.DayMS}, map[string]any{"symbol": "LOWER"}, map[string]any{"symbol": "UNKNOWN", "registry_first_seen_ms": start}}})
	plan, err := BuildPlanWithOptions(strings.NewReader(report), PlanOptions{Policies: map[string]SymbolPolicy{"okx:LISTED": {Allowed: true, ListedAtMS: start + timeframe.DayMS + timeframe.HourMS}, "okx:LOWER": {Allowed: true}, "okx:UNKNOWN": {Allowed: true}}, IncludeNoData: true, EnforceRetention: true})
	if err != nil {
		t.Fatal(err)
	}
	first := map[string]int64{}
	for _, task := range plan.Tasks {
		first[task.Symbol] = task.TargetStarts[0]
	}
	if first["LISTED"] != start+timeframe.DayMS || first["LOWER"] != start || first["UNKNOWN"] != 0 {
		t.Fatalf("incorrect history boundaries: %+v", first)
	}
	if len(plan.Skipped) != 1 || plan.Skipped[0].Reason != "no_data_without_authoritative_or_lower_period_boundary" {
		t.Fatalf("unbounded history should be explicit: %+v", plan.Skipped)
	}
}

func TestLifecyclePlanClampsCountAndDayRetention(t *testing.T) {
	for _, tf := range []string{"15m", "1H", "1D", "1M", "3M"} {
		t.Run(tf, func(t *testing.T) {
			asOf := time.Date(2026, 10, 16, 4, 0, 0, 0, time.UTC)
			latest := timeframe.PreviousStartMS(timeframe.FloorStartMS(asOf.UnixMilli(), tf), tf)
			report := fullReport(t, asOf, map[string]any{"exchange": "binance", "timeframe": tf, "latest_completed_ms": latest, "series": []any{map[string]any{"symbol": "BTCUSDT"}}})
			plan, err := BuildPlanWithOptions(strings.NewReader(report), PlanOptions{Policies: map[string]SymbolPolicy{"binance:BTCUSDT": {Allowed: true, ListedAtMS: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()}}, IncludeNoData: true, EnforceRetention: true})
			if err != nil {
				t.Fatal(err)
			}
			count := 0
			for _, task := range plan.Tasks {
				count += len(task.TargetStarts)
			}
			expected := map[string]int{"15m": 30 * 24 * 4, "1H": 180 * 24, "1D": 300, "1M": 300, "3M": 107}[tf]
			if count != expected {
				t.Fatalf("count=%d want=%d", count, expected)
			}
		})
	}
}

func TestLifecyclePlanRejectsIncompleteAuditAndMissingSeries(t *testing.T) {
	for _, raw := range []string{`{"complete":false}`, `{"generated_at":"2026-10-16T04:00:00Z","results":[{"exchange":"okx","timeframe":"1H","issue_series":[]}]}`} {
		if _, err := BuildPlanWithOptions(strings.NewReader(raw), PlanOptions{IncludeTail: true}); err == nil {
			t.Fatalf("accepted incomplete manifest: %s", raw)
		}
	}
}

func TestLeadingPlanRequiresOfficialListingAndPreservesNativePartialFirstBucket(t *testing.T) {
	asOf := time.Date(2026, 10, 16, 4, 0, 0, 0, time.UTC)
	first := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	listed := time.Date(2026, 6, 20, 13, 0, 0, 0, time.UTC).UnixMilli()
	report := fullReport(t, asOf, map[string]any{"exchange": "binance", "timeframe": "1M", "latest_completed_ms": first + 31*timeframe.DayMS, "series": []any{map[string]any{"symbol": "KNOWN", "canonical_bars": 1, "first_canonical_ms": first, "last_canonical_ms": first}, map[string]any{"symbol": "UNKNOWN", "canonical_bars": 1, "first_canonical_ms": first, "last_canonical_ms": first, "registry_first_seen_ms": listed}}})
	plan, err := BuildPlanWithOptions(strings.NewReader(report), PlanOptions{Policies: map[string]SymbolPolicy{"binance:KNOWN": {Allowed: true, ListedAtMS: listed}, "binance:UNKNOWN": {Allowed: true}}, IncludeLeading: true, EnforceRetention: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Tasks) != 1 || plan.Tasks[0].Symbol != "KNOWN" || plan.Tasks[0].Kind != "leading" || len(plan.Tasks[0].TargetStarts) != 2 || plan.Tasks[0].TargetStarts[0] != time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC).UnixMilli() {
		t.Fatalf("unexpected authoritative leading plan: %+v", plan)
	}
}

func TestDerivedLeadingPlanExcludesIncompleteFirstQuarterAndClampsRetention(t *testing.T) {
	asOf := time.Date(2026, 10, 16, 4, 0, 0, 0, time.UTC)
	first := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	listed := time.Date(2025, 7, 20, 13, 0, 0, 0, time.UTC).UnixMilli()
	report := fullReport(t, asOf, map[string]any{"exchange": "binance", "timeframe": "3M", "latest_completed_ms": time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC).UnixMilli(), "series": []any{map[string]any{"symbol": "BTCUSDT", "canonical_bars": 1, "first_canonical_ms": first, "last_canonical_ms": first}}})
	plan, err := BuildPlanWithOptions(strings.NewReader(report), PlanOptions{Policies: map[string]SymbolPolicy{"binance:BTCUSDT": {Allowed: true, ListedAtMS: listed}}, IncludeLeading: true, IncludeTail: true, EnforceRetention: true})
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, task := range plan.Tasks {
		counts[task.Kind] += len(task.TargetStarts)
		if task.Kind == "leading" && task.TargetStarts[0] != time.Date(2025, 10, 1, 0, 0, 0, 0, time.UTC).UnixMilli() {
			t.Fatalf("partial derived first quarter included: %+v", task)
		}
	}
	if counts["leading"] != 2 || counts["tail"] != 1 {
		t.Fatalf("incorrect leading+tail counts: %+v", counts)
	}
}
