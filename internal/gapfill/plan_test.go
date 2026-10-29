package gapfill

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"crypto-ticket/internal/timeframe"
)

func reportJSON(exchangeName, tf, symbol string, misaligned int, ranges [][3]int64) string {
	gaps := make([]map[string]int64, 0, len(ranges))
	for _, gap := range ranges {
		gaps = append(gaps, map[string]int64{"first_missing_ms": gap[0], "last_missing_ms": gap[1], "missing_bars": gap[2]})
	}
	value := map[string]any{"generated_at": "2026-10-15T04:26:02Z", "results": []any{map[string]any{"exchange": exchangeName, "timeframe": tf, "issue_series": []any{map[string]any{"symbol": symbol, "misaligned_bars": misaligned, "gap_ranges": gaps}}}}}
	encoded, _ := json.Marshal(value)
	return string(encoded)
}

func testStart(tf string) int64 {
	return timeframe.FloorStartMS(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli(), tf)
}

func TestBuildPlanMergesDeduplicatesTargetsWithoutFillingHealthyBars(t *testing.T) {
	start := testStart("1H")
	report := reportJSON("binance", "1H", "btcusdt", 0, [][3]int64{{start, start + timeframe.HourMS, 2}, {start + timeframe.HourMS, start + timeframe.HourMS, 1}, {start + 3*timeframe.HourMS, start + 3*timeframe.HourMS, 1}})
	plan, err := BuildPlan(strings.NewReader(report))
	if err != nil {
		t.Fatal(err)
	}
	if plan.GeneratedAt != "2026-10-15T04:26:02Z" || len(plan.Tasks) != 1 || len(plan.Skipped) != 0 {
		t.Fatalf("unexpected plan: %+v", plan)
	}
	task := plan.Tasks[0]
	if task.Symbol != "BTCUSDT" || task.SourceTimeframe != "1H" || len(task.TargetStarts) != 3 || task.TargetStarts[2] != start+3*timeframe.HourMS {
		t.Fatalf("unexpected task: %+v", task)
	}
	if task.EndMS != start+4*timeframe.HourMS-1 {
		t.Fatalf("target bounds not complete: %+v", task)
	}
}

func TestBuildPlanSplitsSourceSpan(t *testing.T) {
	for _, test := range []struct {
		exchange string
		maximum  int
	}{{"binance", 400}, {"okx", 80}} {
		t.Run(test.exchange, func(t *testing.T) {
			start := testStart("1H")
			count := int64(test.maximum + 1)
			plan, err := BuildPlan(strings.NewReader(reportJSON(test.exchange, "1H", "BTCUSDT", 0, [][3]int64{{start, start + (count-1)*timeframe.HourMS, count}})))
			if err != nil {
				t.Fatal(err)
			}
			if len(plan.Tasks) != 2 || len(plan.Tasks[0].TargetStarts) != test.maximum || len(plan.Tasks[1].TargetStarts) != 1 {
				t.Fatalf("bad task split: %+v", plan.Tasks)
			}
		})
	}
}

func TestBuildPlanUsesSourceCoverageForDerivedBounds(t *testing.T) {
	start := testStart("2W")
	last := start + 5*14*timeframe.DayMS
	plan, err := BuildPlan(strings.NewReader(reportJSON("okx", "2W", "BTC-USDT-SWAP", 0, [][3]int64{{start, last, 6}})))
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Tasks) != 2 || len(plan.Tasks[0].TargetStarts) != 5 || plan.Tasks[0].SourceTimeframe != "1D" || sourceSpan(plan.Tasks[0].StartMS, plan.Tasks[0].EndMS, "1D", 80) != 70 {
		t.Fatalf("source span split wrong: %+v", plan.Tasks)
	}
}

func TestBuildPlanUsesMonthlySourcesForBinanceQuarter(t *testing.T) {
	start := testStart("3M")
	plan, err := BuildPlan(strings.NewReader(reportJSON("binance", "3M", "BTCUSDT", 0, [][3]int64{{start, start, 1}})))
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Tasks) != 1 || plan.Tasks[0].SourceTimeframe != "1M" || sourceSpan(plan.Tasks[0].StartMS, plan.Tasks[0].EndMS, "1M", 400) != 3 {
		t.Fatalf("wrong quarterly source: %+v", plan)
	}
}

func TestBuildPlanSkipsAllDisabledThreeDaySeries(t *testing.T) {
	start := testStart("3D")
	plan, err := BuildPlan(strings.NewReader(reportJSON("binance", "3D", "BTCUSDT", 2, [][3]int64{{start + timeframe.DayMS, start + timeframe.DayMS, 1}, {start + 3*timeframe.DayMS, start + 3*timeframe.DayMS, 1}})))
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Tasks) != 0 || len(plan.Skipped) != 1 || plan.Skipped[0].MissingBars != 2 || plan.Skipped[0].Reason != "disabled_timeframe" {
		t.Fatalf("mixed phase was not skipped: %+v", plan)
	}
	clean, err := BuildPlan(strings.NewReader(reportJSON("binance", "3D", "TEST", 0, [][3]int64{{start, start, 1}})))
	if err != nil || len(clean.Tasks) != 0 || len(clean.Skipped) != 1 || clean.Skipped[0].Reason != "disabled_timeframe" {
		t.Fatalf("canonical Binance 3D was not disabled: %+v %v", clean, err)
	}
	okx, err := BuildPlan(strings.NewReader(reportJSON("okx", "3D", "TEST", 0, [][3]int64{{start, start, 1}})))
	if err != nil || len(okx.Tasks) != 0 || len(okx.Skipped) != 1 || okx.Skipped[0].Reason != "disabled_timeframe" {
		t.Fatalf("OKX 3D was not disabled: %+v %v", okx, err)
	}
}

func TestBuildPlanReadsLegacyDisabledDatesWithoutExpandingOrValidatingThem(t *testing.T) {
	for _, exchange := range []string{"binance", "okx"} {
		report := reportJSON(exchange, "3D", "TEST", 4, [][3]int64{{123, 124, 100}})
		plan, err := BuildPlan(strings.NewReader(report))
		if err != nil || len(plan.Tasks) != 0 || len(plan.Skipped) != 1 || plan.Skipped[0].MissingBars != 100 || plan.Skipped[0].Reason != "disabled_timeframe" {
			t.Fatalf("legacy disabled detail blocked report loading: %+v err=%v", plan, err)
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(report), &payload); err != nil {
			t.Fatal(err)
		}
		series := payload["results"].([]any)[0].(map[string]any)["issue_series"].([]any)[0].(map[string]any)
		series["internal_missing_bars"] = 7
		encoded, _ := json.Marshal(payload)
		plan, err = BuildPlan(strings.NewReader(string(encoded)))
		if err != nil || len(plan.Skipped) != 1 || plan.Skipped[0].MissingBars != 7 {
			t.Fatalf("reported disabled count should take precedence without validating gap counts: %+v err=%v", plan, err)
		}
	}
}

func TestBuildPlanRejectsInvalidRangesAndOversizeManifestCounts(t *testing.T) {
	start := testStart("1H")
	tests := []string{
		"{",
		reportJSON("unknown", "1H", "TEST", 0, [][3]int64{{start, start, 1}}),
		reportJSON("binance", "1h", "TEST", 0, [][3]int64{{start, start, 1}}),
		reportJSON("binance", "1m", "TEST", 0, [][3]int64{{start, start, 1}}),
		reportJSON("binance", "1H", "", 0, [][3]int64{{start, start, 1}}),
		reportJSON("binance", "1H", "TEST", 0, [][3]int64{{start + 1, start + 1, 1}}),
		reportJSON("binance", "1H", "TEST", 0, [][3]int64{{start, start, 2}}),
		reportJSON("binance", "1H", "TEST", 0, [][3]int64{{start, start + timeframe.HourMS, 1}}),
		reportJSON("binance", "1H", "TEST", 0, [][3]int64{{start, start, maxPlanTargets + 1}}),
		reportJSON("binance", "1H", "TEST", 0, [][3]int64{{start, start, 1}}) + " {}",
	}
	for index, report := range tests {
		t.Run(fmt.Sprint(index), func(t *testing.T) {
			if _, err := BuildPlan(strings.NewReader(report)); err == nil {
				t.Fatalf("expected bad manifest rejection: %s", report)
			}
		})
	}
}

func TestSourceTimeframeMappings(t *testing.T) {
	for _, test := range []struct{ exchange, target, source string }{
		{"binance", "2D", "1D"}, {"binance", "3D", "1D"}, {"binance", "5D", "1D"}, {"binance", "2W", "1D"}, {"binance", "3M", "1M"},
		{"okx", "5D", "1D"}, {"okx", "2W", "1D"}, {"okx", "3M", "3M"}, {"okx", "3D", "3D"}, {"binance", "1W", "1W"},
	} {
		if got := sourceTimeframe(test.exchange, test.target); got != test.source {
			t.Fatalf("%s/%s source %s want %s", test.exchange, test.target, got, test.source)
		}
	}
}
