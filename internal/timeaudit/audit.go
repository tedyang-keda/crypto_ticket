// Package timeaudit performs timestamp-only candle continuity checks. It has no
// database, network, or file dependencies and does not infer listing lifetimes.
package timeaudit

import (
	"fmt"
	"sort"
	"time"

	"crypto-ticket/internal/timeframe"
)

const FinalityGrace = 2 * time.Minute

var DefaultTimeframes = []string{"15m", "30m", "1H", "2H", "4H", "6H", "12H", "1D", "2D", "5D", "1W", "2W", "1M", "3M"}

type Metadata struct {
	Exchange       string `json:"exchange"`
	Symbol         string `json:"symbol"`
	MarketType     string `json:"market_type"`
	SourceMarket   string `json:"source_market,omitempty"`
	InstrumentType string `json:"instrument_type,omitempty"`
	AssetClass     string `json:"asset_class,omitempty"`
	RuleType       string `json:"rule_type,omitempty"`
	LifecyclePhase string `json:"lifecycle_phase,omitempty"`
	Active         bool   `json:"registry_active"`
	InRegistry     bool   `json:"in_registry"`
	FirstSeenMS    int64  `json:"registry_first_seen_ms,omitempty"`
	LastSeenMS     int64  `json:"registry_last_seen_ms,omitempty"`
	LastStatus     string `json:"registry_last_status,omitempty"`
}

type Row struct {
	Symbol         string
	StartMS, EndMS int64
}

type Gap struct {
	Exchange        string `json:"exchange"`
	Symbol          string `json:"symbol"`
	Timeframe       string `json:"timeframe"`
	PreviousStartMS int64  `json:"previous_start_ms"`
	NextStartMS     int64  `json:"next_start_ms"`
	FirstMS         int64  `json:"first_missing_ms"`
	LastMS          int64  `json:"last_missing_ms"`
	First           string `json:"first_missing"`
	Last            string `json:"last_missing"`
	Previous        string `json:"previous_start"`
	Next            string `json:"next_start"`
	Count           int64  `json:"missing_bars"`
}

type TimeExample struct {
	StartMS int64 `json:"start_ms"`
	EndMS   int64 `json:"end_ms"`
}

type Series struct {
	Metadata
	Timeframe             string        `json:"timeframe"`
	Status                string        `json:"status"`
	Bars                  int64         `json:"bars"`
	ActualBars            int64         `json:"actual_bars"`
	CanonicalBars         int64         `json:"canonical_bars"`
	ExpectedInteriorCount int64         `json:"expected_interior_count"`
	FirstMS               int64         `json:"first_ms"`
	LastMS                int64         `json:"last_ms"`
	First                 string        `json:"first"`
	Last                  string        `json:"last"`
	FirstCanonicalMS      int64         `json:"first_canonical_ms"`
	LastCanonicalMS       int64         `json:"last_canonical_ms"`
	LatestCompletedMS     int64         `json:"latest_completed_ms"`
	LatestCompleted       string        `json:"latest_completed"`
	InternalMissing       int64         `json:"internal_missing_bars"`
	GapRanges             []Gap         `json:"gap_ranges"`
	Misaligned            int64         `json:"misaligned_bars"`
	InvalidStart          int64         `json:"invalid_start_bars"`
	InvalidEnd            int64         `json:"invalid_end_bars"`
	Duplicates            int64         `json:"duplicate_bars"`
	MisalignedExamples    []TimeExample `json:"misaligned_examples"`
	InvalidEndExamples    []TimeExample `json:"invalid_end_examples"`
	TailLagCandidate      bool          `json:"tail_lag_candidate"`
	TailLagBuckets        int64         `json:"tail_lag_buckets"`
}

type Result struct {
	Exchange               string   `json:"exchange"`
	Timeframe              string   `json:"timeframe"`
	SeriesCount            int      `json:"series_count"`
	SeriesWithData         int      `json:"series_with_data"`
	NoDataSeries           int      `json:"no_data_series"`
	BarCount               int64    `json:"bar_count"`
	CanonicalBars          int64    `json:"canonical_bars"`
	SeriesWithGaps         int      `json:"series_with_internal_gaps"`
	GapRanges              int      `json:"gap_ranges"`
	Missing                int64    `json:"internal_missing_bars"`
	SeriesWithMisalignment int      `json:"series_with_misalignment"`
	Misaligned             int64    `json:"misaligned_bars"`
	InvalidStart           int64    `json:"invalid_start_bars"`
	InvalidEnd             int64    `json:"invalid_end_bars"`
	Duplicates             int64    `json:"duplicate_bars"`
	TailLagSeries          int      `json:"tail_lag_candidate_series"`
	LatestCompletedMS      int64    `json:"latest_completed_ms"`
	LatestCompleted        string   `json:"latest_completed"`
	FirstMS                int64    `json:"first_ms"`
	LastMS                 int64    `json:"last_ms"`
	First                  string   `json:"first"`
	Last                   string   `json:"last"`
	Series                 []Series `json:"series"`
	IssueSeries            []Series `json:"issue_series"`
	Seconds                float64  `json:"seconds"`
}

type Accumulator struct {
	exchange, tf  string
	latest        int64
	series        map[string]*Series
	lastSymbol    string
	lastStart     int64
	hasRow        bool
	closedSymbols map[string]bool
}

// LatestCompletedStart uses expected UTC bucket boundaries, not stored end_ms,
// so a corrupt stored end cannot hide a final row or include an open candle.
func LatestCompletedStart(tf string, asOf time.Time) int64 {
	watermark := asOf.UTC().Add(-FinalityGrace).UnixMilli()
	start := timeframe.FloorStartMS(watermark, tf)
	if timeframe.EndMS(start, tf) > watermark {
		start = timeframe.PreviousStartMS(start, tf)
	}
	return start
}

func New(exchange, tf string, asOf time.Time, registry []Metadata) (*Accumulator, error) {
	if _, err := timeframe.Normalize(tf); err != nil {
		return nil, err
	}
	a := &Accumulator{exchange: exchange, tf: tf, latest: LatestCompletedStart(tf, asOf), series: map[string]*Series{}, closedSymbols: map[string]bool{}}
	for _, m := range registry {
		if m.Exchange == exchange {
			m.InRegistry = true
			a.addSeries(m)
		}
	}
	return a, nil
}

func (a *Accumulator) addSeries(m Metadata) *Series {
	s := &Series{Metadata: m, Timeframe: a.tf, LatestCompletedMS: a.latest, LatestCompleted: FormatMS(a.latest), GapRanges: []Gap{}, MisalignedExamples: []TimeExample{}, InvalidEndExamples: []TimeExample{}}
	a.series[m.Symbol] = s
	return s
}

// Add expects SQL order (symbol, start_ms). Off-grid rows are counted but never
// become gap anchors: mixed phases must not manufacture missing timestamps.
func (a *Accumulator) Add(row Row) error {
	if row.Symbol == "" {
		return fmt.Errorf("empty symbol")
	}
	// SQL symbol collation need not match Go byte ordering (for example '-').
	// Require contiguous symbol groups, not lexical ordering across groups.
	if a.hasRow && row.Symbol != a.lastSymbol {
		a.closedSymbols[a.lastSymbol] = true
	}
	if a.closedSymbols[row.Symbol] || a.hasRow && row.Symbol == a.lastSymbol && row.StartMS < a.lastStart {
		return fmt.Errorf("timestamp stream is not ordered")
	}
	duplicate := a.hasRow && row.Symbol == a.lastSymbol && row.StartMS == a.lastStart
	a.lastSymbol, a.lastStart, a.hasRow = row.Symbol, row.StartMS, true
	if row.StartMS >= timeframe.NextStartMS(a.latest, a.tf) {
		return nil
	}
	s := a.series[row.Symbol]
	if s == nil {
		s = a.addSeries(Metadata{Exchange: a.exchange, Symbol: row.Symbol})
	}
	s.Bars++
	s.ActualBars++
	if s.Bars == 1 {
		s.FirstMS = row.StartMS
		s.First = FormatMS(row.StartMS)
	}
	s.LastMS = row.StartMS
	s.Last = FormatMS(row.StartMS)
	if duplicate {
		s.Duplicates++
		return nil
	}
	if row.StartMS <= 0 {
		s.InvalidStart++
		return nil
	}
	if row.EndMS != timeframe.EndMS(row.StartMS, a.tf) {
		s.InvalidEnd++
		if len(s.InvalidEndExamples) < 8 {
			s.InvalidEndExamples = append(s.InvalidEndExamples, TimeExample{row.StartMS, row.EndMS})
		}
	}
	if timeframe.FloorStartMS(row.StartMS, a.tf) != row.StartMS {
		s.Misaligned++
		if len(s.MisalignedExamples) < 8 {
			s.MisalignedExamples = append(s.MisalignedExamples, TimeExample{row.StartMS, row.EndMS})
		}
		return nil
	}
	if s.CanonicalBars == 0 {
		s.FirstCanonicalMS = row.StartMS
	} else {
		first := timeframe.NextStartMS(s.LastCanonicalMS, a.tf)
		if first < row.StartMS {
			last := timeframe.PreviousStartMS(row.StartMS, a.tf)
			count := BucketCount(first, last, a.tf)
			s.InternalMissing += count
			s.GapRanges = append(s.GapRanges, Gap{a.exchange, row.Symbol, a.tf, s.LastCanonicalMS, row.StartMS, first, last, FormatMS(first), FormatMS(last), FormatMS(s.LastCanonicalMS), FormatMS(row.StartMS), count})
		}
	}
	s.LastCanonicalMS = row.StartMS
	s.CanonicalBars++
	return nil
}

// BucketCount counts inclusive, aligned UTC bucket starts. Month periods remain
// calendar-aware; no average 30-day/90-day approximation is used.
func BucketCount(first, last int64, tf string) int64 {
	if last < first {
		return 0
	}
	if tf == "1M" || tf == "3M" {
		f, l := time.UnixMilli(first).UTC(), time.UnixMilli(last).UTC()
		step := 1
		if tf == "3M" {
			step = 3
		}
		return int64(((l.Year()-f.Year())*12+int(l.Month())-int(f.Month()))/step + 1)
	}
	return (last-first)/(timeframe.NextStartMS(first, tf)-first) + 1
}

func (a *Accumulator) Finish() Result {
	r := Result{Exchange: a.exchange, Timeframe: a.tf, LatestCompletedMS: a.latest, LatestCompleted: FormatMS(a.latest), Series: []Series{}, IssueSeries: []Series{}}
	keys := make([]string, 0, len(a.series))
	for key := range a.series {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		s := *a.series[key]
		s.ExpectedInteriorCount = s.CanonicalBars + s.InternalMissing
		s.Status = "continuous"
		if s.Bars == 0 {
			s.Status = "no_data"
			r.NoDataSeries++
		} else {
			r.SeriesWithData++
			if r.FirstMS == 0 || s.FirstMS < r.FirstMS {
				r.FirstMS = s.FirstMS
			}
			if s.LastMS > r.LastMS {
				r.LastMS = s.LastMS
			}
		}
		if s.InternalMissing > 0 {
			s.Status = "internal_gaps"
			r.SeriesWithGaps++
		}
		if s.Misaligned > 0 || s.InvalidStart > 0 || s.InvalidEnd > 0 || s.Duplicates > 0 {
			s.Status = "time_anomaly"
		}
		if s.Misaligned > 0 {
			r.SeriesWithMisalignment++
		}
		if s.Active && s.CanonicalBars > 0 && s.LastCanonicalMS < a.latest {
			s.TailLagCandidate = true
			s.TailLagBuckets = BucketCount(timeframe.NextStartMS(s.LastCanonicalMS, a.tf), a.latest, a.tf)
			r.TailLagSeries++
		}
		r.BarCount += s.Bars
		r.CanonicalBars += s.CanonicalBars
		r.GapRanges += len(s.GapRanges)
		r.Missing += s.InternalMissing
		r.Misaligned += s.Misaligned
		r.InvalidStart += s.InvalidStart
		r.InvalidEnd += s.InvalidEnd
		r.Duplicates += s.Duplicates
		r.Series = append(r.Series, s)
		if s.Status != "continuous" {
			r.IssueSeries = append(r.IssueSeries, s)
		}
	}
	r.SeriesCount = len(r.Series)
	r.First = FormatMS(r.FirstMS)
	r.Last = FormatMS(r.LastMS)
	return r
}

func FormatMS(ms int64) string {
	if ms == 0 {
		return ""
	}
	return time.UnixMilli(ms).UTC().Format(time.RFC3339)
}

// CompleteUniverse adds no-data rows for stored symbols discovered in another
// selected period. It does not turn absent whole series into missing bars.
func CompleteUniverse(result *Result, universe []Metadata) {
	present := map[string]bool{}
	for _, s := range result.Series {
		present[s.Symbol] = true
	}
	for _, m := range universe {
		if m.Exchange != result.Exchange || present[m.Symbol] {
			continue
		}
		s := Series{Metadata: m, Timeframe: result.Timeframe, Status: "no_data", LatestCompletedMS: result.LatestCompletedMS, LatestCompleted: result.LatestCompleted, GapRanges: []Gap{}, MisalignedExamples: []TimeExample{}, InvalidEndExamples: []TimeExample{}}
		result.Series = append(result.Series, s)
		result.IssueSeries = append(result.IssueSeries, s)
		result.SeriesCount++
		result.NoDataSeries++
		present[m.Symbol] = true
	}
	sort.Slice(result.Series, func(i, j int) bool { return result.Series[i].Symbol < result.Series[j].Symbol })
	sort.Slice(result.IssueSeries, func(i, j int) bool { return result.IssueSeries[i].Symbol < result.IssueSeries[j].Symbol })
}
