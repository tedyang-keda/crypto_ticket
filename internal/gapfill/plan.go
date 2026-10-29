// Package gapfill plans and executes bounded, insert-only repairs of audited
// historical gaps. It never discovers extra repair targets from fetched data.
package gapfill

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"crypto-ticket/internal/retention"
	"crypto-ticket/internal/timeframe"
)

type Task struct {
	ID              string  `json:"id"`
	Exchange        string  `json:"exchange"`
	Symbol          string  `json:"symbol"`
	Timeframe       string  `json:"timeframe"`
	SourceTimeframe string  `json:"source_timeframe"`
	StartMS         int64   `json:"start_ms"`
	EndMS           int64   `json:"end_ms"`
	TargetStarts    []int64 `json:"target_starts"`
	Kind            string  `json:"kind,omitempty"`
}

type Plan struct {
	GeneratedAt string    `json:"generated_at"`
	Tasks       []Task    `json:"tasks"`
	Skipped     []Skipped `json:"skipped"`
}

type Skipped struct {
	Exchange    string `json:"exchange"`
	Symbol      string `json:"symbol"`
	Timeframe   string `json:"timeframe"`
	Reason      string `json:"reason"`
	MissingBars int    `json:"missing_bars"`
}

// SymbolPolicy is a frozen, externally verified lifecycle/subscription decision.
// The planner does not interpret registry first_seen as an official listing date.
type SymbolPolicy struct {
	Allowed      bool   `json:"allowed"`
	Reason       string `json:"reason,omitempty"`
	MarketType   string `json:"market_type"`
	ListedAtMS   int64  `json:"listed_at_ms,omitempty"`
	CapturedAtMS int64  `json:"captured_at_ms"`
}

type PlanOptions struct {
	// Nil retains legacy internal-gap-only behavior. A non-nil map is fail-closed:
	// symbols absent from the official/subscription snapshot are never repaired.
	Policies         map[string]SymbolPolicy
	IncludeTail      bool
	IncludeNoData    bool
	IncludeLeading   bool
	EnforceRetention bool
}

type auditGap struct {
	FirstMS int64 `json:"first_missing_ms"`
	LastMS  int64 `json:"last_missing_ms"`
	Count   int   `json:"missing_bars"`
}

type auditSeries struct {
	Symbol              string     `json:"symbol"`
	MisalignedBars      int        `json:"misaligned_bars"`
	InternalMissingBars *int       `json:"internal_missing_bars"`
	CanonicalBars       int64      `json:"canonical_bars"`
	FirstCanonicalMS    int64      `json:"first_canonical_ms"`
	LastCanonicalMS     int64      `json:"last_canonical_ms"`
	LatestCompletedMS   int64      `json:"latest_completed_ms"`
	GapRanges           []auditGap `json:"gap_ranges"`
}

type auditReport struct {
	GeneratedAt string `json:"generated_at"`
	AsOf        string `json:"as_of"`
	Complete    *bool  `json:"complete"`
	Results     []struct {
		Exchange          string        `json:"exchange"`
		Timeframe         string        `json:"timeframe"`
		LatestCompletedMS int64         `json:"latest_completed_ms"`
		Series            []auditSeries `json:"series"`
		IssueSeries       []auditSeries `json:"issue_series"`
	} `json:"results"`
}

type seriesKey struct {
	exchange, symbol, timeframe, kind string
}

const (
	maxReportBytes = 128 << 20
	maxPlanTargets = 2_000_000
)

// BuildPlan uses only the audit's explicit missing timestamps. Nearby gaps may
// share a fetch window, but healthy timestamps between them never become targets.
// EndMS is inclusive and covers the complete last target bucket, including all
// required daily/monthly source bars for derived timeframes.
func BuildPlan(reader io.Reader) (Plan, error) {
	return BuildPlanWithOptions(reader, PlanOptions{})
}

func BuildPlanWithOptions(reader io.Reader, options PlanOptions) (Plan, error) {
	var report auditReport
	limited := &io.LimitedReader{R: reader, N: maxReportBytes + 1}
	decoder := json.NewDecoder(limited)
	if err := decoder.Decode(&report); err != nil {
		return Plan{}, fmt.Errorf("decode gap report: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return Plan{}, fmt.Errorf("decode gap report: extra JSON value")
		}
		return Plan{}, fmt.Errorf("decode gap report trailer: %w", err)
	}
	if limited.N <= 0 {
		return Plan{}, fmt.Errorf("gap report exceeds %d bytes", maxReportBytes)
	}
	if report.Complete != nil && !*report.Complete {
		return Plan{}, fmt.Errorf("cannot repair an incomplete audit")
	}
	asOfText := report.AsOf
	if asOfText == "" {
		asOfText = report.GeneratedAt
	}
	asOf, asOfErr := time.Parse(time.RFC3339Nano, asOfText)
	if (options.IncludeTail || options.IncludeNoData || options.IncludeLeading || options.EnforceRetention) && asOfErr != nil {
		return Plan{}, fmt.Errorf("invalid audit watermark: %w", asOfErr)
	}
	plan := Plan{GeneratedAt: report.GeneratedAt, Tasks: []Task{}, Skipped: []Skipped{}}
	if plan.GeneratedAt == "" {
		plan.GeneratedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	targets := make(map[seriesKey]map[int64]bool)
	totalTargets := 0
	// Existing canonical lower-period starts are evidence of available history,
	// not proof of listing. They only bound empty higher-period series.
	lowerStarts := make(map[string]map[string]int64)
	for _, result := range report.Results {
		for _, series := range result.Series {
			if series.CanonicalBars > 0 && series.FirstCanonicalMS > 0 && series.MisalignedBars == 0 {
				key := strings.ToLower(result.Exchange) + ":" + strings.ToUpper(series.Symbol)
				if lowerStarts[key] == nil {
					lowerStarts[key] = map[string]int64{}
				}
				lowerStarts[key][result.Timeframe] = series.FirstCanonicalMS
			}
		}
	}
	for _, result := range report.Results {
		ex := strings.ToLower(strings.TrimSpace(result.Exchange))
		if ex != "binance" && ex != "okx" {
			return Plan{}, fmt.Errorf("unsupported gap exchange %q", result.Exchange)
		}
		tf, err := timeframe.Normalize(result.Timeframe)
		if err != nil || timeframe.Index(tf) < timeframe.Index("15m") {
			return Plan{}, fmt.Errorf("unsupported gap timeframe %q", result.Timeframe)
		}
		seriesList := result.IssueSeries
		if options.IncludeTail || options.IncludeNoData || options.IncludeLeading {
			seriesList = result.Series
			if seriesList == nil {
				return Plan{}, fmt.Errorf("full series are required for tail/no-data planning: %s/%s", ex, tf)
			}
		}
		for _, series := range seriesList {
			seriesTargets := make(map[int64]bool)
			symbol := strings.ToUpper(strings.TrimSpace(series.Symbol))
			if symbol == "" {
				return Plan{}, fmt.Errorf("empty gap symbol for %s/%s", ex, tf)
			}
			if errors.Is(timeframe.ValidateForExchange(ex, tf), timeframe.ErrDisabledTimeframe) {
				// Frozen reports may contain obsolete 3D phases or inconsistent
				// detailed ranges. Preserve the reported skip count, without
				// expanding or validating dates that can never be repair targets.
				missing := 0
				if series.InternalMissingBars != nil && *series.InternalMissingBars >= 0 {
					missing = *series.InternalMissingBars
				} else {
					for _, gap := range series.GapRanges {
						if gap.Count > 0 {
							if gap.Count > int(^uint(0)>>1)-missing {
								return Plan{}, fmt.Errorf("disabled gap count overflows %s/%s/%s", ex, symbol, tf)
							}
							missing += gap.Count
						}
					}
				}
				plan.Skipped = append(plan.Skipped, Skipped{Exchange: ex, Symbol: symbol, Timeframe: tf, Reason: "disabled_timeframe", MissingBars: missing})
				continue
			}
			policy := options.Policies[ex+":"+symbol]
			if options.Policies != nil && !policy.Allowed {
				reason := policy.Reason
				if reason == "" {
					reason = "missing_official_subscription_snapshot"
				}
				missing := 0
				if series.InternalMissingBars != nil {
					missing = *series.InternalMissingBars
				}
				plan.Skipped = append(plan.Skipped, Skipped{Exchange: ex, Symbol: symbol, Timeframe: tf, Reason: reason, MissingBars: missing})
				continue
			}
			latest := series.LatestCompletedMS
			if latest <= 0 {
				latest = result.LatestCompletedMS
			}
			minimum := int64(0)
			if options.EnforceRetention {
				rule := retention.RuleFor(tf)
				if cutoff, ok := retention.CutoffMS(rule, asOf); ok {
					minimum = ceilStartMS(cutoff, tf)
				} else if rule.KeepBars > 0 && latest > 0 {
					minimum = retention.BarWindowStartMS(tf, timeframe.EndMS(latest, tf), rule.KeepBars)
				}
			}
			if policy.ListedAtMS > 0 && historyBoundaryStartMS(policy.ListedAtMS, ex, tf) > minimum {
				minimum = historyBoundaryStartMS(policy.ListedAtMS, ex, tf)
			}
			addTarget := func(start int64, kind string) error {
				if minimum > 0 && start < minimum {
					return nil
				}
				key := seriesKey{exchange: ex, symbol: symbol, timeframe: tf, kind: kind}
				if targets[key] == nil {
					targets[key] = map[int64]bool{}
				}
				if !targets[key][start] {
					totalTargets++
					if totalTargets > maxPlanTargets {
						return fmt.Errorf("gap report exceeds %d unique targets", maxPlanTargets)
					}
					targets[key][start] = true
				}
				return nil
			}
			outsideBoundary := 0
			for _, gap := range series.GapRanges {
				if gap.Count <= 0 || gap.Count > maxPlanTargets || gap.FirstMS <= 0 || gap.LastMS < gap.FirstMS {
					return Plan{}, fmt.Errorf("invalid gap %s/%s/%s: first=%d last=%d count=%d", ex, symbol, tf, gap.FirstMS, gap.LastMS, gap.Count)
				}
				if timeframe.FloorStartMS(gap.FirstMS, tf) != gap.FirstMS || timeframe.FloorStartMS(gap.LastMS, tf) != gap.LastMS {
					return Plan{}, fmt.Errorf("unaligned gap %s/%s/%s", ex, symbol, tf)
				}
				cursor := gap.FirstMS
				for count := 0; count < gap.Count; count++ {
					if cursor > gap.LastMS {
						return Plan{}, fmt.Errorf("gap count exceeds range %s/%s/%s", ex, symbol, tf)
					}
					if !seriesTargets[cursor] && cursor < minimum {
						outsideBoundary++
					}
					if err := addTarget(cursor, "internal"); err != nil {
						return Plan{}, err
					}
					seriesTargets[cursor] = true
					if count == gap.Count-1 && cursor != gap.LastMS {
						return Plan{}, fmt.Errorf("gap count does not cover range %s/%s/%s", ex, symbol, tf)
					}
					next := timeframe.NextStartMS(cursor, tf)
					if next <= cursor {
						return Plan{}, fmt.Errorf("non-increasing gap %s/%s/%s", ex, symbol, tf)
					}
					cursor = next
				}
			}
			if series.InternalMissingBars != nil && len(seriesTargets) != *series.InternalMissingBars {
				return Plan{}, fmt.Errorf("gap detail count mismatch %s/%s/%s: expected=%d actual=%d", ex, symbol, tf, *series.InternalMissingBars, len(seriesTargets))
			}
			if outsideBoundary > 0 {
				plan.Skipped = append(plan.Skipped, Skipped{Exchange: ex, Symbol: symbol, Timeframe: tf, Reason: "outside_retention_or_listing_boundary", MissingBars: outsideBoundary})
			}
			if options.IncludeLeading && policy.ListedAtMS > 0 && series.CanonicalBars > 0 && series.FirstCanonicalMS > 0 {
				if latest <= 0 || timeframe.FloorStartMS(latest, tf) != latest || timeframe.EndMS(latest, tf) >= asOf.UnixMilli() {
					return Plan{}, fmt.Errorf("invalid latest closed bucket %s/%s/%s", ex, symbol, tf)
				}
				first := historyBoundaryStartMS(policy.ListedAtMS, ex, tf)
				if first < minimum {
					first = minimum
				}
				last := timeframe.PreviousStartMS(series.FirstCanonicalMS, tf)
				if last > latest {
					last = latest
				}
				for cursor := first; cursor <= last; cursor = timeframe.NextStartMS(cursor, tf) {
					if err := addTarget(cursor, "leading"); err != nil {
						return Plan{}, err
					}
				}
			}
			first, kind := int64(0), ""
			if options.IncludeTail && series.CanonicalBars > 0 && series.LastCanonicalMS > 0 && series.LastCanonicalMS < latest {
				first, kind = timeframe.NextStartMS(series.LastCanonicalMS, tf), "tail"
			}
			if options.IncludeNoData && series.CanonicalBars == 0 {
				first, kind = policy.ListedAtMS, "no_data"
				if first == 0 {
					for lowerTF, start := range lowerStarts[ex+":"+symbol] {
						if timeframe.Index(lowerTF) < timeframe.Index(tf) && (first == 0 || start < first) {
							first = start
						}
					}
				}
				if first == 0 {
					plan.Skipped = append(plan.Skipped, Skipped{Exchange: ex, Symbol: symbol, Timeframe: tf, Reason: "no_data_without_authoritative_or_lower_period_boundary"})
					continue
				}
				first = historyBoundaryStartMS(first, ex, tf)
			}
			if kind != "" {
				if latest <= 0 || timeframe.FloorStartMS(latest, tf) != latest || timeframe.EndMS(latest, tf) >= asOf.UnixMilli() {
					return Plan{}, fmt.Errorf("invalid latest closed bucket %s/%s/%s", ex, symbol, tf)
				}
				if first < minimum {
					first = minimum
				}
				for cursor := first; cursor <= latest; cursor = timeframe.NextStartMS(cursor, tf) {
					if err := addTarget(cursor, kind); err != nil {
						return Plan{}, err
					}
				}
			}
		}
	}
	keys := make([]seriesKey, 0, len(targets))
	for key := range targets {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].exchange != keys[j].exchange {
			return keys[i].exchange < keys[j].exchange
		}
		if keys[i].symbol != keys[j].symbol {
			return keys[i].symbol < keys[j].symbol
		}
		if keys[i].timeframe != keys[j].timeframe {
			return timeframe.Index(keys[i].timeframe) < timeframe.Index(keys[j].timeframe)
		}
		return keys[i].kind < keys[j].kind
	})
	for _, key := range keys {
		starts := sortedStarts(targets[key])
		source := sourceTimeframe(key.exchange, key.timeframe)
		maximum := maxSourceBars(key.exchange, source)
		for len(starts) > 0 {
			last := 0
			for last+1 < len(starts) && sourceSpan(starts[0], timeframe.EndMS(starts[last+1], key.timeframe), source, maximum) <= maximum {
				last++
			}
			if sourceSpan(starts[0], timeframe.EndMS(starts[last], key.timeframe), source, maximum) > maximum {
				return Plan{}, fmt.Errorf("target exceeds source span limit %s/%s/%s", key.exchange, key.symbol, key.timeframe)
			}
			selected := append([]int64(nil), starts[:last+1]...)
			task := Task{Exchange: key.exchange, Symbol: key.symbol, Timeframe: key.timeframe, SourceTimeframe: source, StartMS: selected[0], EndMS: timeframe.EndMS(selected[len(selected)-1], key.timeframe), TargetStarts: selected}
			task.Kind = key.kind
			task.ID = fmt.Sprintf("%s:%s:%s:%d:%d", task.Exchange, task.Symbol, task.Timeframe, task.StartMS, task.EndMS)
			plan.Tasks = append(plan.Tasks, task)
			starts = starts[last+1:]
		}
	}
	return plan, nil
}

func ceilStartMS(start int64, tf string) int64 {
	floor := timeframe.FloorStartMS(start, tf)
	if floor < start {
		return timeframe.NextStartMS(floor, tf)
	}
	return floor
}

// Native official candles may legitimately cover only part of the first listed
// trading bucket. Preserve that bucket; complete-source requirements remain
// conservative for locally derived intervals.
func historyBoundaryStartMS(start int64, exchange, tf string) int64 {
	if sourceTimeframe(exchange, tf) == tf {
		return timeframe.FloorStartMS(start, tf)
	}
	return ceilStartMS(start, tf)
}

func maxSourceBars(exchangeName, source string) int {
	if exchangeName == "okx" {
		return 80
	}
	// COIN-M history requests may span at most 200 days. Applying this
	// conservative bound to both Binance markets avoids guessing routes here.
	switch source {
	case "1D":
		return 198
	case "1W":
		return 26
	case "1M":
		return 6
	}
	return 400
}

func sourceTimeframe(exchange, target string) string {
	if exchange == "binance" {
		switch target {
		case "2D", "3D", "5D", "2W":
			return "1D"
		case "3M":
			return "1M"
		}
	}
	if exchange == "okx" && (target == "5D" || target == "2W") {
		return "1D"
	}
	return target
}

func sortedStarts(starts map[int64]bool) []int64 {
	out := make([]int64, 0, len(starts))
	for start := range starts {
		out = append(out, start)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func sourceSpan(start, end int64, source string, maximum int) int {
	count := 0
	for cursor := timeframe.FloorStartMS(start, source); cursor <= end; cursor = timeframe.NextStartMS(cursor, source) {
		count++
		if count > maximum {
			break
		}
	}
	return count
}
