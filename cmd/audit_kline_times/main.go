// audit_kline_times creates a read-only, timestamp-only full-market report.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"crypto-ticket/internal/config"
	mysqlstore "crypto-ticket/internal/storage/mysql"
	"crypto-ticket/internal/timeaudit"

	mysqldriver "github.com/go-sql-driver/mysql"
)

type options struct {
	report, progress, frames, exchanges, asOf, symbols, healthURL string
	delay, maxRuntime                                             time.Duration
	minFreeGB                                                     uint64
}

type report struct {
	GeneratedAt          string             `json:"generated_at"`
	AsOf                 string             `json:"as_of"`
	UpdatedAt            string             `json:"updated_at"`
	Complete             bool               `json:"complete"`
	Status               string             `json:"status"`
	Error                string             `json:"error,omitempty"`
	Definition           string             `json:"definition"`
	FinalityGraceSeconds int64              `json:"finality_grace_seconds"`
	Exchanges            []string           `json:"exchanges"`
	Timeframes           []string           `json:"timeframes"`
	Symbols              []string           `json:"symbol_filter,omitempty"`
	TotalPairs           int                `json:"total_pairs"`
	CompletedPairs       int                `json:"completed_pairs"`
	CurrentPair          string             `json:"current_pair,omitempty"`
	CurrentPairRows      int64              `json:"current_pair_rows"`
	Results              []timeaudit.Result `json:"results"`
}

type progress struct {
	GeneratedAt     string         `json:"generated_at"`
	AsOf            string         `json:"as_of"`
	UpdatedAt       string         `json:"updated_at"`
	Complete        bool           `json:"complete"`
	Status          string         `json:"status"`
	Error           string         `json:"error,omitempty"`
	CompletedPairs  int            `json:"completed_pairs"`
	TotalPairs      int            `json:"total_pairs"`
	CurrentPair     string         `json:"current_pair,omitempty"`
	CurrentPairRows int64          `json:"current_pair_rows"`
	BarCount        int64          `json:"bar_count"`
	Missing         int64          `json:"internal_missing_bars"`
	Misaligned      int64          `json:"misaligned_bars"`
	InvalidEnd      int64          `json:"invalid_end_bars"`
	NoDataSeries    int            `json:"no_data_series"`
	Results         []pairProgress `json:"results"`
}
type pairProgress struct {
	Exchange   string  `json:"exchange"`
	Timeframe  string  `json:"timeframe"`
	Series     int     `json:"series_count"`
	Bars       int64   `json:"bar_count"`
	Missing    int64   `json:"internal_missing_bars"`
	GapRanges  int     `json:"gap_ranges"`
	Misaligned int64   `json:"misaligned_bars"`
	InvalidEnd int64   `json:"invalid_end_bars"`
	NoData     int     `json:"no_data_series"`
	Seconds    float64 `json:"seconds"`
}

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)
	var o options
	flag.StringVar(&o.report, "report", "", "new full report JSON path (required; must not exist)")
	flag.StringVar(&o.progress, "progress", "", "new compact progress JSON path (required; must not exist)")
	flag.StringVar(&o.frames, "timeframes", strings.Join(timeaudit.DefaultTimeframes, ","), "exact periods; 3D is excluded")
	flag.StringVar(&o.exchanges, "exchanges", "binance,okx", "selected exchanges")
	flag.StringVar(&o.symbols, "symbols", "", "optional exact symbol filter for smoke tests")
	flag.StringVar(&o.asOf, "as-of", "", "fixed RFC3339 observation time; default invocation UTC time")
	flag.DurationVar(&o.delay, "query-delay", 100*time.Millisecond, "pause between exchange/timeframe SELECTs")
	flag.DurationVar(&o.maxRuntime, "max-runtime", time.Hour, "maximum total runtime")
	flag.Uint64Var(&o.minFreeGB, "min-free-gb", 5, "stop below this free GiB on the report filesystem")
	flag.StringVar(&o.healthURL, "health-url", "", "optional live readiness URL checked during the scan")
	flag.Parse()
	if err := run(o); err != nil {
		log.Printf("audit stopped: %v", err)
		os.Exit(1)
	}
}

func run(o options) (runErr error) {
	if o.report == "" || o.progress == "" || o.delay < 0 || o.maxRuntime <= 0 {
		return errors.New("report, progress and positive max-runtime are required")
	}
	frames, exchanges, symbols := split(o.frames), split(o.exchanges), split(o.symbols)
	if len(frames) == 0 || len(exchanges) == 0 {
		return errors.New("timeframes and exchanges cannot be empty")
	}
	allowed := map[string]bool{}
	for _, tf := range timeaudit.DefaultTimeframes {
		allowed[tf] = true
	}
	for _, tf := range frames {
		if !allowed[tf] {
			return fmt.Errorf("unsupported audit timeframe %q", tf)
		}
	}
	for _, ex := range exchanges {
		if ex != "binance" && ex != "okx" {
			return fmt.Errorf("unsupported audit exchange %q", ex)
		}
	}
	asOf := time.Now().UTC()
	if o.asOf != "" {
		var err error
		asOf, err = time.Parse(time.RFC3339Nano, o.asOf)
		if err != nil {
			return errors.New("as-of must be RFC3339")
		}
		asOf = asOf.UTC()
	}
	if asOf.After(time.Now().Add(time.Minute)) {
		return errors.New("as-of cannot be in the future")
	}
	if err := reservePaths(o.report, o.progress); err != nil {
		return err
	}
	r := report{GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano), AsOf: asOf.Format(time.RFC3339Nano), Status: "running", Definition: "Timestamp/count-only SELECT audit of all stored final rows up to each latest closed UTC bucket, with 2-minute finality grace. Internal gaps are only between canonical existing starts; leading/trailing coverage is not called missing without authoritative listing/delisting dates. Off-grid rows never anchor gaps. Registry active is discovery status, NOT subscription state; tail lag is a candidate only. All registry and stored symbols are included. Non-crypto market sessions/suspensions can create intentional time gaps. No OHLCV checks, external REST calls or database writes.", FinalityGraceSeconds: int64(timeaudit.FinalityGrace / time.Second), Exchanges: exchanges, Timeframes: frames, Symbols: symbols, TotalPairs: len(exchanges) * len(frames), Results: []timeaudit.Result{}}
	defer func() {
		if runErr != nil {
			r.Status = "failed"
			r.Error = runErr.Error()
		}
		if err := saveReport(o, &r); runErr == nil {
			runErr = err
		}
	}()
	if err := saveReport(o, &r); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, o.maxRuntime)
	defer cancel()
	if err := checkSafety(ctx, o); err != nil {
		return err
	}
	dsn, err := mysqldriver.ParseDSN(config.Load().MySQLDSN)
	if err != nil {
		return errors.New("invalid MYSQL_DSN")
	}
	dsn.MultiStatements = false
	dsn.Timeout = 10 * time.Second
	dsn.ReadTimeout = 30 * time.Second
	dsn.WriteTimeout = 10 * time.Second
	reader, err := mysqlstore.NewTimeAuditReader(dsn.FormatDSN())
	if err != nil {
		return errors.New("mysql reader initialization failed")
	}
	defer reader.Close()
	if err = reader.Ping(ctx); err != nil {
		return errors.New("mysql reader ping failed")
	}
	symbolSet := map[string]bool{}
	for _, s := range symbols {
		symbolSet[s] = true
	}
	for _, ex := range exchanges {
		registry, err := reader.Registry(ctx, ex)
		if err != nil {
			return fmt.Errorf("read %s registry failed", ex)
		}
		if len(symbolSet) > 0 {
			filtered := registry[:0]
			for _, m := range registry {
				if symbolSet[m.Symbol] {
					filtered = append(filtered, m)
				}
			}
			registry = filtered
		}
		for _, tf := range frames {
			if err = checkSafety(ctx, o); err != nil {
				return err
			}
			r.CurrentPair = ex + ":" + tf
			r.CurrentPairRows = 0
			if err = saveProgress(o.progress, &r); err != nil {
				return err
			}
			acc, err := timeaudit.New(ex, tf, asOf, registry)
			if err != nil {
				return err
			}
			started := time.Now()
			lastProgress := started
			err = reader.StreamTimes(ctx, ex, tf, timeaudit.LatestCompletedStart(tf, asOf), symbols, func(row timeaudit.Row) error {
				if err := acc.Add(row); err != nil {
					return err
				}
				r.CurrentPairRows++
				if r.CurrentPairRows%25000 == 0 {
					if err := checkSafety(ctx, o); err != nil {
						return err
					}
					if time.Since(lastProgress) >= 5*time.Second {
						if err := saveProgress(o.progress, &r); err != nil {
							return err
						}
						lastProgress = time.Now()
					}
				}
				return nil
			})
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return fmt.Errorf("timestamp SELECT failed for %s/%s: %s", ex, tf, safeDBError(err))
			}
			result := acc.Finish()
			result.Seconds = time.Since(started).Seconds()
			r.Results = append(r.Results, result)
			r.CompletedPairs++
			log.Printf("completed %s/%s rows=%d series=%d gaps=%d missing=%d seconds=%.1f", ex, tf, result.BarCount, result.SeriesCount, result.GapRanges, result.Missing, result.Seconds)
			if err = saveReport(o, &r); err != nil {
				return err
			}
			if o.delay > 0 {
				timer := time.NewTimer(o.delay)
				select {
				case <-ctx.Done():
					timer.Stop()
					return ctx.Err()
				case <-timer.C:
				}
			}
		}
	}
	universe := map[string]timeaudit.Metadata{}
	for _, result := range r.Results {
		for _, series := range result.Series {
			universe[series.Exchange+":"+series.Symbol] = series.Metadata
		}
	}
	all := make([]timeaudit.Metadata, 0, len(universe))
	for _, metadata := range universe {
		all = append(all, metadata)
	}
	for i := range r.Results {
		timeaudit.CompleteUniverse(&r.Results[i], all)
	}
	r.Complete = true
	r.Status = "finished"
	r.CurrentPair = ""
	r.CurrentPairRows = 0
	return nil
}

func split(value string) []string {
	out := []string{}
	seen := map[string]bool{}
	for _, s := range strings.Split(value, ",") {
		s = strings.TrimSpace(s)
		if s != "" && !seen[s] {
			out = append(out, s)
			seen[s] = true
		}
	}
	return out
}

func saveReport(o options, r *report) error {
	r.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if err := writeJSON(o.report, r); err != nil {
		return err
	}
	return saveProgress(o.progress, r)
}
func saveProgress(path string, r *report) error {
	p := progress{GeneratedAt: r.GeneratedAt, AsOf: r.AsOf, UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano), Complete: r.Complete, Status: r.Status, Error: r.Error, CompletedPairs: r.CompletedPairs, TotalPairs: r.TotalPairs, CurrentPair: r.CurrentPair, CurrentPairRows: r.CurrentPairRows, Results: []pairProgress{}}
	for _, x := range r.Results {
		p.BarCount += x.BarCount
		p.Missing += x.Missing
		p.Misaligned += x.Misaligned
		p.InvalidEnd += x.InvalidEnd
		p.NoDataSeries += x.NoDataSeries
		p.Results = append(p.Results, pairProgress{x.Exchange, x.Timeframe, x.SeriesCount, x.BarCount, x.Missing, x.GapRanges, x.Misaligned, x.InvalidEnd, x.NoDataSeries, x.Seconds})
	}
	return writeJSON(path, p)
}

func writeJSON(path string, value any) error {
	f, err := os.CreateTemp(parentDir(path), ".timeaudit-*.tmp")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	encoder := json.NewEncoder(f)
	encoder.SetEscapeHTML(false)
	if err = encoder.Encode(value); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(name, path)
}

func safeDBError(err error) string {
	var my *mysqldriver.MySQLError
	if errors.As(err, &my) {
		return fmt.Sprintf("mysql error %d", my.Number)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "query deadline exceeded"
	}
	if errors.Is(err, context.Canceled) {
		return "query canceled"
	}
	return "stream interrupted; result is incomplete"
}
