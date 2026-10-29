package gapfill

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	"crypto-ticket/internal/aggregator"
	"crypto-ticket/internal/exchange"
	"crypto-ticket/internal/market"
	"crypto-ticket/internal/timeframe"
)

// Store may return final rows only. InsertMissingBars must atomically skip every
// existing primary key (including non-final rows) without updating any columns.
// In particular it must not delegate to the normal upsert method. The final-row
// verification leaves a hidden non-final conflict explicitly unresolved.
type Store interface {
	BarsInRange(context.Context, string, string, string, int64, int64) ([]market.Bar, error)
	InsertMissingBars(context.Context, []market.Bar) (int, error)
}

type Result struct {
	TaskID         string `json:"task_id"`
	Exchange       string `json:"exchange"`
	Symbol         string `json:"symbol"`
	Timeframe      string `json:"timeframe"`
	Kind           string `json:"kind,omitempty"`
	Status         string `json:"status"`
	Error          string `json:"error,omitempty"`
	Requested      int    `json:"requested"`
	AlreadyPresent int    `json:"already_present"`
	Fetched        int    `json:"fetched"`
	Inserted       int    `json:"inserted"`
	Available      int    `json:"available"`
	Expired        int    `json:"expired,omitempty"`
	Skipped        int    `json:"skipped,omitempty"`
	// Unverified marks keys requiring a fresh DB read, not confirmed missing.
	// MissingStarts remains the retry candidate list and can include committed
	// inserts after a verification failure.
	Unverified    bool    `json:"unverified,omitempty"`
	MissingStarts []int64 `json:"missing_starts"`
}

// Repair fetches only one bounded task and never writes outside TargetStarts.
// With apply=false it does not write; MissingStarts still lists the real database
// gaps rather than reporting a fetched but unpersisted row as repaired.
func Repair(ctx context.Context, store Store, fetcher exchange.RESTKlineFetcher, client *http.Client, task Task, apply bool) (Result, error) {
	result := Result{TaskID: task.ID, Exchange: task.Exchange, Symbol: task.Symbol, Timeframe: task.Timeframe, Kind: task.Kind, Status: "error", MissingStarts: []int64{}, Unverified: true}
	fail := func(err error) (Result, error) {
		result.Status, result.Error = "error", err.Error()
		return result, err
	}
	starts, err := validateTask(task, time.Now().UnixMilli())
	if err != nil {
		return fail(err)
	}
	result.Requested = len(starts)
	first, last := starts[0], starts[len(starts)-1]
	local, err := store.BarsInRange(ctx, task.Exchange, task.Symbol, task.Timeframe, first, last)
	if err != nil {
		return fail(fmt.Errorf("read target keys: %w", err))
	}
	occupied, present := storedTargetKeys(task, local, starts)
	result.Unverified = false
	for _, start := range starts {
		if occupied[start] {
			result.AlreadyPresent++
		}
		if !present[start] {
			result.MissingStarts = append(result.MissingStarts, start)
		}
	}
	if len(result.MissingStarts) == 0 {
		result.Status = "already_present"
		return result, nil
	}
	need := make(map[int64]bool)
	for _, start := range result.MissingStarts {
		if !occupied[start] {
			need[start] = true
		}
	}
	if len(need) == 0 {
		result.Status = "conflict"
		return result, nil
	}
	official, err := fetcher.FetchKlines(ctx, client, exchange.KlineRequest{Symbol: task.Symbol, Timeframe: task.SourceTimeframe, StartMS: task.StartMS, EndMS: task.EndMS, Limit: 0, Adjustment: exchange.KlineAdjustmentRaw})
	if err != nil {
		return fail(fmt.Errorf("fetch official source: %w", err))
	}
	result.Fetched = len(official)
	nowMS := time.Now().UnixMilli()
	sources := make(map[int64]market.Bar)
	invalidSource := make(map[int64]bool)
	for _, bar := range official {
		if bar.StartMS < task.StartMS || bar.StartMS > task.EndMS {
			continue
		}
		if !validBar(bar, task.Exchange, task.Symbol, task.SourceTimeframe, nowMS) {
			invalidSource[bar.StartMS] = true
			continue
		}
		if _, duplicate := sources[bar.StartMS]; duplicate {
			invalidSource[bar.StartMS] = true
			continue
		}
		sources[bar.StartMS] = bar
	}
	for start := range invalidSource {
		delete(sources, start)
	}
	available := make([]market.Bar, 0, len(need))
	for _, start := range starts {
		if !need[start] {
			continue
		}
		var candidate market.Bar
		if task.SourceTimeframe == task.Timeframe {
			var found bool
			candidate, found = sources[start]
			if !found {
				continue
			}
		} else {
			inputs := make([]market.Bar, 0)
			end := timeframe.EndMS(start, task.Timeframe)
			cursor := start
			for cursor <= end {
				bar, found := sources[cursor]
				if !found || bar.EndMS > end {
					break
				}
				inputs = append(inputs, bar)
				cursor = timeframe.NextStartMS(cursor, task.SourceTimeframe)
			}
			if len(inputs) == 0 || cursor != end+1 {
				continue
			}
			rolled := aggregator.RollupBars(task.Timeframe, inputs, true, "gapfill_official_source", nowMS)
			if rolled == nil {
				continue
			}
			candidate = *rolled
		}
		if !validBar(candidate, task.Exchange, task.Symbol, task.Timeframe, nowMS) || candidate.StartMS != start {
			continue
		}
		candidate.Source, candidate.Reason, candidate.UpdatedAtMS = "rest_gapfill", "audit_missing_insert", nowMS
		available = append(available, candidate)
	}
	if err := decorateMissingBars(ctx, store, task, available, local); err != nil {
		return fail(fmt.Errorf("read previous close: %w", err))
	}
	result.Available = len(available)
	if !apply {
		result.Status = "dry_run"
		if len(available) == 0 {
			result.Status = "no_data"
		}
		return result, nil
	}
	if len(available) > 0 {
		result.Unverified = true
		result.Inserted, err = store.InsertMissingBars(ctx, available)
		if err != nil {
			return fail(fmt.Errorf("insert missing bars: %w", err))
		}
	}
	verified, err := store.BarsInRange(ctx, task.Exchange, task.Symbol, task.Timeframe, first, last)
	if err != nil {
		return fail(fmt.Errorf("verify target keys: %w", err))
	}
	_, present = storedTargetKeys(task, verified, starts)
	result.Unverified = false
	result.MissingStarts = result.MissingStarts[:0]
	for _, start := range starts {
		if !present[start] {
			result.MissingStarts = append(result.MissingStarts, start)
		}
	}
	switch {
	case len(result.MissingStarts) == 0:
		result.Status = "repaired"
	case len(available) == 0 && len(present) == 0:
		result.Status = "no_data"
	default:
		result.Status = "partial"
	}
	return result, nil
}

func validateTask(task Task, nowMS int64) ([]int64, error) {
	if task.Exchange != "binance" && task.Exchange != "okx" {
		return nil, fmt.Errorf("unsupported task exchange %q", task.Exchange)
	}
	if err := timeframe.ValidateForExchange(task.Exchange, task.Timeframe); err != nil {
		return nil, err
	}
	if task.Symbol == "" || task.Symbol != strings.ToUpper(strings.TrimSpace(task.Symbol)) {
		return nil, fmt.Errorf("invalid task symbol %q", task.Symbol)
	}
	tf, err := timeframe.Normalize(task.Timeframe)
	if err != nil || timeframe.Index(tf) < timeframe.Index("15m") {
		return nil, fmt.Errorf("unsupported task timeframe %q", task.Timeframe)
	}
	if task.SourceTimeframe != sourceTimeframe(task.Exchange, tf) {
		return nil, fmt.Errorf("invalid source %q for %s/%s", task.SourceTimeframe, task.Exchange, tf)
	}
	if task.StartMS <= 0 || task.EndMS < task.StartMS || len(task.TargetStarts) == 0 {
		return nil, fmt.Errorf("empty or invalid task range")
	}
	set := make(map[int64]bool, len(task.TargetStarts))
	for _, start := range task.TargetStarts {
		if start < task.StartMS || timeframe.EndMS(start, tf) > task.EndMS || start != timeframe.FloorStartMS(start, tf) || timeframe.EndMS(start, tf) >= nowMS {
			return nil, fmt.Errorf("invalid or unfinished target start_ms=%d", start)
		}
		set[start] = true
	}
	starts := sortedStarts(set)
	if task.StartMS != starts[0] || task.EndMS != timeframe.EndMS(starts[len(starts)-1], tf) {
		return nil, fmt.Errorf("task range does not match complete target bounds")
	}
	maximum := maxSourceBars(task.Exchange, task.SourceTimeframe)
	if task.StartMS != timeframe.FloorStartMS(task.StartMS, task.SourceTimeframe) || sourceSpan(task.StartMS, task.EndMS, task.SourceTimeframe, maximum) > maximum {
		return nil, fmt.Errorf("task source range exceeds limit or is unaligned")
	}
	return starts, nil
}

func storedTargetKeys(task Task, bars []market.Bar, starts []int64) (map[int64]bool, map[int64]bool) {
	wanted := make(map[int64]bool, len(starts))
	for _, start := range starts {
		wanted[start] = true
	}
	occupied, final := make(map[int64]bool), make(map[int64]bool)
	for _, bar := range bars {
		if bar.Exchange == task.Exchange && bar.Symbol == task.Symbol && bar.Timeframe == task.Timeframe && wanted[bar.StartMS] {
			occupied[bar.StartMS] = true
			if bar.IsFinal {
				final[bar.StartMS] = true
			}
		}
	}
	return occupied, final
}

func validBar(bar market.Bar, ex, symbol, tf string, nowMS int64) bool {
	if bar.Exchange != ex || bar.Symbol != symbol || bar.Timeframe != tf || !bar.IsFinal || bar.StartMS <= 0 || bar.StartMS != timeframe.FloorStartMS(bar.StartMS, tf) || bar.EndMS != timeframe.EndMS(bar.StartMS, tf) || bar.EndMS >= nowMS {
		return false
	}
	for _, value := range []float64{bar.OpenPrice, bar.HighPrice, bar.LowPrice, bar.ClosePrice} {
		if value <= 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			return false
		}
	}
	for _, value := range []float64{bar.Volume, bar.QuoteVolume, bar.ContractVolume} {
		if value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			return false
		}
	}
	return bar.TradeCount >= 0 && bar.HighPrice >= math.Max(bar.OpenPrice, bar.ClosePrice) && bar.LowPrice <= math.Min(bar.OpenPrice, bar.ClosePrice) && bar.HighPrice >= bar.LowPrice
}

func decorateMissingBars(ctx context.Context, store Store, task Task, bars []market.Bar, local []market.Bar) error {
	sort.Slice(bars, func(i, j int) bool { return bars[i].StartMS < bars[j].StartMS })
	byStart := make(map[int64]market.Bar, len(local)+len(bars))
	for _, bar := range local {
		if bar.IsFinal {
			byStart[bar.StartMS] = bar
		}
	}
	for index := range bars {
		previousStart := timeframe.PreviousStartMS(bars[index].StartMS, task.Timeframe)
		previous, found := byStart[previousStart]
		if !found && previousStart > 0 {
			prior, err := store.BarsInRange(ctx, task.Exchange, task.Symbol, task.Timeframe, previousStart, previousStart)
			if err != nil {
				return err
			}
			for _, bar := range prior {
				if bar.Exchange == task.Exchange && bar.Symbol == task.Symbol && bar.Timeframe == task.Timeframe && bar.StartMS == previousStart && bar.IsFinal {
					previous, found = bar, true
					break
				}
			}
		}
		close := float64(0)
		if found && previous.ClosePrice > 0 && !math.IsNaN(previous.ClosePrice) && !math.IsInf(previous.ClosePrice, 0) {
			close = previous.ClosePrice
		}
		bars[index] = aggregator.ApplyDerived(bars[index], close)
		byStart[bars[index].StartMS] = bars[index]
	}
	return nil
}
