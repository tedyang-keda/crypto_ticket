package monitoring

import (
	"context"
	"log"
	"math"
	"sort"
	"strings"
	"time"

	"crypto-ticket/internal/aggregator"
	"crypto-ticket/internal/market"
	"crypto-ticket/internal/timeframe"
)

type IntegrityStore interface {
	ListSymbols(ctx context.Context, exchange string, activeOnly *bool) ([]market.SymbolInfo, error)
	BarsInRange(ctx context.Context, exchange string, symbol string, timeframe string, startMS int64, endMS int64) ([]market.Bar, error)
}

type IntegrityConfig struct {
	Enabled       bool
	Exchanges     []string
	Timeframes    []string
	Interval      time.Duration
	SymbolsPerRun int
	Buckets       int
	Grace         time.Duration
	Tolerance     float64
}

type IntegrityAuditor struct {
	store    IntegrityStore
	registry *Registry
	cfg      IntegrityConfig
	cursor   int
	now      func() time.Time
}

func NewIntegrityAuditor(store IntegrityStore, registry *Registry, cfg IntegrityConfig) *IntegrityAuditor {
	if cfg.Interval <= 0 {
		cfg.Interval = 10 * time.Minute
	}
	if cfg.SymbolsPerRun <= 0 {
		cfg.SymbolsPerRun = 50
	}
	if cfg.Buckets <= 0 {
		cfg.Buckets = 3
	}
	if cfg.Grace <= 0 {
		cfg.Grace = 2 * time.Minute
	}
	if cfg.Tolerance <= 0 {
		cfg.Tolerance = 1e-8
	}
	if len(cfg.Timeframes) == 0 {
		cfg.Timeframes = []string{"15m", "30m", "1H", "4H", "1D", "2D", "1W"}
	}
	return &IntegrityAuditor{store: store, registry: registry, cfg: cfg, now: time.Now}
}

func (a *IntegrityAuditor) Run(ctx context.Context) error {
	if !a.cfg.Enabled {
		<-ctx.Done()
		return ctx.Err()
	}
	ticker := time.NewTicker(a.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if _, err := a.AuditOnce(ctx); err != nil {
				log.Printf("monitoring integrity audit failed: %v", err)
			}
		}
	}
}

func (a *IntegrityAuditor) AuditOnce(ctx context.Context) (IntegritySummary, error) {
	targets, err := a.targets(ctx)
	if err != nil {
		return IntegritySummary{}, err
	}
	summary := IntegritySummary{CheckedAt: a.now(), ByTimeframe: make(map[string][3]int)}
	if len(targets) == 0 {
		a.registry.SetIntegrity(summary)
		return summary, nil
	}
	count := a.cfg.SymbolsPerRun
	if count > len(targets) {
		count = len(targets)
	}
	affected := make(map[string]bool)
	for i := 0; i < count; i++ {
		target := targets[(a.cursor+i)%len(targets)]
		summary.CheckedSymbols++
		for _, tf := range a.cfg.Timeframes {
			issues, err := a.auditSeries(ctx, target, tf)
			if err != nil {
				return summary, err
			}
			if len(issues) > 0 {
				affected[target.Exchange+":"+target.Symbol] = true
			}
			byTimeframe := summary.ByTimeframe[tf]
			for _, issue := range issues {
				summary.Issues = append(summary.Issues, issue)
				switch issue.Type {
				case "missing":
					summary.Missing++
					byTimeframe[0]++
				case "mismatch":
					summary.Mismatch++
					byTimeframe[1]++
				case "invalid_ohlc":
					summary.InvalidOHLC++
					byTimeframe[2]++
				}
			}
			summary.ByTimeframe[tf] = byTimeframe
		}
	}
	a.cursor = (a.cursor + count) % len(targets)
	for item := range affected {
		summary.Affected = append(summary.Affected, item)
	}
	sort.Strings(summary.Affected)
	a.registry.SetIntegrity(summary)
	return summary, nil
}

func (a *IntegrityAuditor) targets(ctx context.Context) ([]market.SymbolInfo, error) {
	active := true
	seen := make(map[string]bool)
	var out []market.SymbolInfo
	for _, exchangeName := range a.cfg.Exchanges {
		exchangeName = strings.ToLower(strings.TrimSpace(exchangeName))
		if exchangeName == "" || seen[exchangeName] {
			continue
		}
		seen[exchangeName] = true
		symbols, err := a.store.ListSymbols(ctx, exchangeName, &active)
		if err != nil {
			return nil, err
		}
		out = append(out, symbols...)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Exchange != out[j].Exchange {
			return out[i].Exchange < out[j].Exchange
		}
		return out[i].Symbol < out[j].Symbol
	})
	return out, nil
}

func (a *IntegrityAuditor) auditSeries(ctx context.Context, target market.SymbolInfo, tf string) ([]IntegrityIssue, error) {
	eligible := a.now().Add(-a.cfg.Grace).UnixMilli()
	start := timeframe.FloorStartMS(eligible, tf)
	if timeframe.EndMS(start, tf) > eligible {
		start = timeframe.PreviousStartMS(start, tf)
	}
	var issues []IntegrityIssue
	for i := 0; i < a.cfg.Buckets; i++ {
		bucketStart := start
		for step := 0; step < i; step++ {
			bucketStart = timeframe.PreviousStartMS(bucketStart, tf)
		}
		bucketEnd := timeframe.EndMS(bucketStart, tf)
		targetBars, queryErr := a.store.BarsInRange(ctx, target.Exchange, target.Symbol, tf, bucketStart, bucketStart)
		if queryErr != nil {
			return nil, queryErr
		}
		if len(targetBars) > 0 && !validOHLC(targetBars[len(targetBars)-1]) {
			issues = append(issues, newIntegrityIssue(target, tf, bucketStart, "invalid_ohlc"))
		}
		sourceTF := aggregator.RollupSourceTimeframe(tf)
		if sourceTF == "" {
			continue
		}
		sourceBars, queryErr := a.store.BarsInRange(ctx, target.Exchange, target.Symbol, sourceTF, bucketStart, bucketEnd)
		if queryErr != nil {
			return nil, queryErr
		}
		if !completeSource(sourceBars, sourceTF, bucketStart, bucketEnd) {
			continue
		}
		expected := aggregator.RollupBars(tf, sourceBars, true, "monitoring_integrity", a.now().UnixMilli())
		if expected == nil {
			continue
		}
		if len(targetBars) == 0 {
			issues = append(issues, newIntegrityIssue(target, tf, bucketStart, "missing"))
			continue
		}
		if barsDifferForIntegrity(targetBars[len(targetBars)-1], *expected, a.cfg.Tolerance) {
			issues = append(issues, newIntegrityIssue(target, tf, bucketStart, "mismatch"))
		}
	}
	return issues, nil
}

func newIntegrityIssue(target market.SymbolInfo, tf string, startMS int64, issueType string) IntegrityIssue {
	return IntegrityIssue{
		Exchange: target.Exchange, Symbol: target.Symbol, Timeframe: tf, StartMS: startMS, Type: issueType,
	}
}

func completeSource(bars []market.Bar, sourceTF string, targetStart int64, targetEnd int64) bool {
	if len(bars) == 0 {
		return false
	}
	sorted := append([]market.Bar(nil), bars...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].StartMS < sorted[j].StartMS })
	expected := targetStart
	for _, bar := range sorted {
		if !bar.IsFinal || bar.StartMS != expected {
			return false
		}
		expected = timeframe.NextStartMS(bar.StartMS, sourceTF)
	}
	return expected > targetEnd && sorted[len(sorted)-1].EndMS >= targetEnd
}

func validOHLC(bar market.Bar) bool {
	return bar.OpenPrice > 0 && bar.HighPrice > 0 && bar.LowPrice > 0 && bar.ClosePrice > 0 &&
		bar.HighPrice >= math.Max(bar.OpenPrice, bar.ClosePrice) && bar.LowPrice <= math.Min(bar.OpenPrice, bar.ClosePrice) &&
		bar.HighPrice >= bar.LowPrice
}

func barsDifferForIntegrity(actual market.Bar, expected market.Bar, tolerance float64) bool {
	return relativeDifference(actual.OpenPrice, expected.OpenPrice) > tolerance ||
		relativeDifference(actual.HighPrice, expected.HighPrice) > tolerance ||
		relativeDifference(actual.LowPrice, expected.LowPrice) > tolerance ||
		relativeDifference(actual.ClosePrice, expected.ClosePrice) > tolerance ||
		relativeDifference(actual.Volume, expected.Volume) > tolerance ||
		relativeDifference(actual.QuoteVolume, expected.QuoteVolume) > tolerance ||
		relativeDifference(actual.ContractVolume, expected.ContractVolume) > tolerance ||
		actual.TradeCount != expected.TradeCount
}

func relativeDifference(left float64, right float64) float64 {
	scale := math.Max(math.Abs(left), math.Abs(right))
	if scale == 0 {
		return 0
	}
	return math.Abs(left-right) / scale
}
