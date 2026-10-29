// gapfill_klines repairs only timestamp keys from a frozen integrity report.
// It deliberately does not initialize schema, refresh subscriptions, delete,
// upsert existing candles, or cascade repairs into the live market service.
package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"crypto-ticket/internal/config"
	"crypto-ticket/internal/exchange"
	"crypto-ticket/internal/gapfill"
	"crypto-ticket/internal/retention"
	"crypto-ticket/internal/timeframe"
)

type options struct {
	report, journal, summary, exchanges, symbols, frames string
	apply, planOnly, retryUnresolved                     bool
	maxTasks                                             int
	binanceRPS, okxRPS                                   float64
	maxRuntime                                           time.Duration
	minFreeGB                                            uint64
	healthURL                                            string
	snapshot                                             string
	refreshSnapshot                                      bool
	snapshotMaxAge                                       time.Duration
	maxInFlight                                          int
}

type journalEntry struct {
	Kind         string          `json:"kind"`
	ReportSHA256 string          `json:"report_sha256,omitempty"`
	Apply        bool            `json:"apply,omitempty"`
	At           string          `json:"at"`
	Result       *gapfill.Result `json:"result,omitempty"`
}

type runSummary struct {
	ReportSHA256     string                     `json:"report_sha256"`
	AuditGeneratedAt string                     `json:"audit_generated_at"`
	StartedAt        string                     `json:"started_at"`
	UpdatedAt        string                     `json:"updated_at"`
	Apply            bool                       `json:"apply"`
	Status           string                     `json:"status"`
	Tasks            int                        `json:"tasks"`
	Requested        int                        `json:"requested"`
	CompletedTasks   int                        `json:"completed_tasks"`
	RemainingTasks   int                        `json:"remaining_tasks"`
	Inserted         int                        `json:"inserted"`
	AlreadyPresent   int                        `json:"already_present"`
	Unresolved       int                        `json:"unresolved"`
	Unverified       int                        `json:"unverified"`
	Expired          int                        `json:"expired"`
	SkippedBars      int                        `json:"skipped_bars"`
	ByStatus         map[string]int             `json:"by_status"`
	ByExchange       map[string]exchangeSummary `json:"by_exchange"`
	Skipped          []gapfill.Skipped          `json:"skipped"`
	Results          []gapfill.Result           `json:"results"`
	PlannedTasks     []gapfill.Task             `json:"planned_tasks,omitempty"`
	ByKind           map[string]int             `json:"requested_by_kind"`
	PlanByExchange   map[string]planCounts      `json:"planned_by_exchange"`
}

type planCounts struct {
	Tasks   int `json:"tasks"`
	Targets int `json:"targets"`
}

type exchangeSummary struct {
	Tasks, Requested, Inserted, AlreadyPresent, Unresolved, Unverified int
}

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)
	var o options
	flag.StringVar(&o.report, "report", "", "frozen gap report JSON (required)")
	flag.StringVar(&o.journal, "journal", "", "append-only resumable results JSONL (required except plan-only)")
	flag.StringVar(&o.summary, "summary", "", "atomic progress/result JSON (required)")
	flag.StringVar(&o.exchanges, "exchanges", "binance,okx", "selected report exchanges")
	flag.StringVar(&o.symbols, "symbols", "", "optional exact symbols, comma separated")
	flag.StringVar(&o.frames, "timeframes", "", "optional exact timeframes, comma separated")
	flag.BoolVar(&o.apply, "apply", false, "insert missing keys; otherwise fetch/check only")
	flag.BoolVar(&o.planOnly, "plan-only", false, "build plan without DB or HTTP access")
	flag.BoolVar(&o.retryUnresolved, "retry-unresolved", false, "re-run journaled unresolved tasks")
	flag.IntVar(&o.maxTasks, "max-tasks", 0, "max tasks per invocation; zero runs all")
	flag.Float64Var(&o.binanceRPS, "binance-rps", 1, "extra Binance requests/sec, shared by UM and COIN-M")
	flag.Float64Var(&o.okxRPS, "okx-rps", 1, "extra OKX requests/sec")
	flag.DurationVar(&o.maxRuntime, "max-runtime", 6*time.Hour, "bounded run duration")
	flag.Uint64Var(&o.minFreeGB, "min-free-gb", 5, "stop below this much free space on the journal filesystem")
	flag.StringVar(&o.healthURL, "health-url", "", "optional local readiness URL checked before each task")
	flag.StringVar(&o.snapshot, "snapshot", "", "frozen persisted official/subscription metadata JSON (required)")
	flag.BoolVar(&o.refreshSnapshot, "refresh-snapshot", false, "export current persisted metadata to -snapshot before planning (read-only DB access)")
	flag.DurationVar(&o.snapshotMaxAge, "snapshot-max-age", 15*time.Minute, "maximum official snapshot age; unavailable/stale scopes are excluded")
	flag.IntVar(&o.maxInFlight, "max-in-flight", 8, "maximum concurrent REST repair tasks per exchange (1-8)")
	flag.Parse()
	if err := run(o); err != nil {
		log.Printf("stopped: %v", safeError(err))
		os.Exit(1)
	}
}

func run(o options) error {
	if o.report == "" || o.summary == "" || o.snapshot == "" || (!o.planOnly && o.journal == "") {
		return errors.New("report, snapshot, summary and journal paths are required")
	}
	if o.binanceRPS <= 0 || o.okxRPS <= 0 || o.binanceRPS > 8 || o.okxRPS > 5 || o.maxRuntime <= 0 {
		return errors.New("invalid request rate or runtime")
	}
	if err := validatePaths(o); err != nil {
		return err
	}
	if o.snapshotMaxAge <= 0 || o.maxInFlight < 1 || o.maxInFlight > 8 {
		return errors.New("invalid snapshot age or in-flight limit")
	}
	snapshot, err := loadSnapshot(o)
	if err != nil {
		return err
	}
	policies, err := snapshot.policies(time.Now(), o.snapshotMaxAge)
	if err != nil {
		return err
	}
	f, err := os.Open(o.report)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	plan, err := gapfill.BuildPlanWithOptions(io.TeeReader(f, h), gapfill.PlanOptions{Policies: policies, IncludeTail: true, IncludeNoData: true, IncludeLeading: true, EnforceRetention: true})
	if err != nil {
		return err
	}
	digest := hex.EncodeToString(h.Sum(nil))
	exchanges, symbols, frames := csvSet(o.exchanges), csvSet(o.symbols), csvSet(o.frames)
	var tasks []gapfill.Task
	for _, task := range plan.Tasks {
		if !exchanges[task.Exchange] || len(symbols) > 0 && !symbols[task.Symbol] || len(frames) > 0 && !frames[task.Timeframe] {
			continue
		}
		tasks = append(tasks, task)
	}
	var skipped []gapfill.Skipped
	for _, s := range plan.Skipped {
		if exchanges[s.Exchange] && (len(symbols) == 0 || symbols[s.Symbol]) && (len(frames) == 0 || frames[s.Timeframe]) {
			skipped = append(skipped, s)
		}
	}
	results := make(map[string]gapfill.Result)
	started := time.Now().UTC().Format(time.RFC3339)
	var mu sync.Mutex
	var lastSummaryAt time.Time // protected by mu while workers run
	writeSummary := func(status string) error {
		now := time.Now()
		if !shouldSaveSummary(status, lastSummaryAt, now) {
			return nil
		}
		if err := saveSummary(o.summary, digest, plan.GeneratedAt, started, status, o.apply, tasks, skipped, results); err != nil {
			return err
		}
		lastSummaryAt = now
		return nil
	}
	if o.planOnly {
		return writeSummary("planned")
	}
	jf, previous, err := openJournal(o.journal, digest, o.apply)
	if err != nil {
		return err
	}
	defer jf.Close()
	for id, r := range previous {
		results[id] = r
	}
	if err := writeSummary("running"); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, o.maxRuntime)
	defer cancel()
	cfg := config.Load()
	store, err := connectStore()
	if err != nil {
		return errors.New("connect mysql failed")
	}
	defer store.Close()
	if err := store.Ping(ctx); err != nil {
		return errors.New("mysql ping failed")
	}
	gate := &eligibilityGate{store: store, maxAge: o.snapshotMaxAge}
	adapters := make(map[string]exchange.RESTKlineFetcher)
	for _, c := range cfg.Exchanges {
		if c.Name == "binance" {
			adapters[c.Name+":"+c.MarketType] = exchange.NewBinanceFuturesAdapter(c.MarketType, c.RestURL, c.WSURL)
		}
		if c.Name == "okx" {
			adapters[c.Name+":"+c.MarketType] = exchange.NewOKXAdapter(c.MarketType, c.RestURL, c.WSURL)
		}
	}
	var wg sync.WaitGroup
	var runErr error
	taskBudget := 0
	for _, name := range []string{"binance", "okx"} {
		if !exchanges[name] {
			continue
		}
		var queue []gapfill.Task
		for _, t := range tasks {
			if t.Exchange != name {
				continue
			}
			mu.Lock()
			previousResult, previouslyDone := results[t.ID]
			mu.Unlock()
			if previouslyDone && (!o.retryUnresolved || len(previousResult.MissingStarts) == 0) {
				continue
			}
			if o.maxTasks > 0 && taskBudget >= o.maxTasks {
				continue
			}
			taskBudget++
			queue = append(queue, t)
		}
		rps := o.binanceRPS
		if name == "okx" {
			rps = o.okxRPS
		}
		base := http.DefaultTransport.(*http.Transport).Clone()
		base.MaxIdleConnsPerHost = o.maxInFlight
		base.MaxConnsPerHost = o.maxInFlight
		transport := newPacedTransport(base, time.Duration(float64(time.Second)/rps))
		client := &http.Client{Timeout: 90 * time.Second, Transport: transport}
		jobs := make(chan gapfill.Task, len(queue))
		for _, task := range queue {
			jobs <- task
		}
		close(jobs)
		var exchangeWorkers sync.WaitGroup
		consecutiveFailures := 0 // protected by mu; shared across this exchange's workers
		for worker := 0; worker < o.maxInFlight; worker++ {
			wg.Add(1)
			exchangeWorkers.Add(1)
			go func(name string) {
				defer wg.Done()
				defer exchangeWorkers.Done()
				for original := range jobs {
					if ctx.Err() != nil {
						return
					}
					if err := checkRunSafety(ctx, o); err != nil {
						mu.Lock()
						if runErr == nil {
							runErr = err
						}
						mu.Unlock()
						cancel()
						return
					}
					task, expired := retainedTask(original, time.Now().UTC())
					r := gapfill.Result{TaskID: original.ID, Exchange: name, Symbol: original.Symbol, Timeframe: original.Timeframe, Requested: len(original.TargetStarts), MissingStarts: append([]int64(nil), task.TargetStarts...), Unverified: true}
					var taskErr error
					metadataUnavailable := false
					if len(task.TargetStarts) == 0 {
						r.Status = "retention_expired"
						r.MissingStarts = nil
						r.Unverified = false
					} else {
						policy, policyErr := gate.policy(ctx, name, task.Symbol)
						fetcher := adapters[name+":"+policy.MarketType]
						if policyErr != nil {
							metadataUnavailable = true
							taskErr = fmt.Errorf("refresh repair eligibility: %w", policyErr)
						} else if !policy.Allowed {
							if strings.HasPrefix(policy.Reason, "snapshot_") || policy.Reason == "missing_official_subscription_snapshot" {
								metadataUnavailable = true
								taskErr = fmt.Errorf("current official metadata unavailable: %s", policy.Reason)
							} else {
								r.Status, r.Error = "skipped_policy_changed", policy.Reason
								r.Skipped = len(task.TargetStarts)
								r.MissingStarts = nil
								r.Unverified = false
							}
						} else if fetcher == nil {
							taskErr = errors.New("no verified registry market route")
						} else {
							factors, factorErr := store.ListCorporateActionFactors(ctx, name, task.Symbol)
							if factorErr != nil {
								taskErr = fmt.Errorf("read adjustment history: %w", factorErr)
							}
							for _, factor := range factors {
								if factor.Factor != 1 && factor.EffectiveMS > task.StartMS {
									taskErr = errors.New("adjustment_history_requires_review")
									break
								}
							}
							if taskErr == nil {
								committedInserted := 0
								for attempt := 0; attempt < 3; attempt++ {
									taskCtx, taskCancel := context.WithTimeout(ctx, 3*time.Minute)
									r, taskErr = gapfill.Repair(taskCtx, store, fetcher, client, task, o.apply)
									committedInserted += r.Inserted
									r.Inserted = committedInserted
									taskCancel()
									if taskErr == nil || ctx.Err() != nil || !transientError(taskErr) {
										break
									}
									timer := time.NewTimer(time.Duration(1<<attempt) * 5 * time.Second)
									select {
									case <-ctx.Done():
										timer.Stop()
									case <-timer.C:
									}
								}
								if expired > 0 {
									r.Requested = len(original.TargetStarts)
								}
							}
						}
					}
					if taskErr != nil && metadataUnavailable {
						mu.Lock()
						if runErr == nil {
							runErr = taskErr
						}
						mu.Unlock()
						cancel()
					}
					r.Expired = expired
					r.Kind = original.Kind
					r.Requested = len(original.TargetStarts)
					if taskErr != nil {
						r.Status = "error"
						r.Error = safeError(taskErr)
						if len(r.MissingStarts) == 0 {
							r.MissingStarts = append([]int64(nil), task.TargetStarts...)
						}
					}
					mu.Lock()
					if taskErr != nil && transientError(taskErr) {
						consecutiveFailures++
					} else {
						consecutiveFailures = 0
					}
					stopExchange := consecutiveFailures >= 8
					if old, ok := results[original.ID]; ok {
						if r.Unverified && r.Inserted == 0 && !old.Unverified {
							r.MissingStarts = retainedRetryStarts(old.MissingStarts, task.TargetStarts)
						}
						r.Inserted += old.Inserted
					}
					if o.apply {
						r.AlreadyPresent = r.Requested - r.Expired - r.Skipped - len(r.MissingStarts) - r.Inserted
						if r.AlreadyPresent < 0 || r.Unverified {
							r.AlreadyPresent = 0
						}
					}
					results[original.ID] = r
					entry := journalEntry{Kind: "result", At: time.Now().UTC().Format(time.RFC3339), Result: &r}
					journalErr := json.NewEncoder(jf).Encode(entry)
					if journalErr == nil {
						journalErr = jf.Sync()
					}
					if journalErr != nil && runErr == nil {
						runErr = journalErr
						cancel()
					}
					if err := writeSummary("running"); err != nil && runErr == nil {
						runErr = err
						cancel()
					}
					log.Printf("task=%s exchange=%s symbol=%s tf=%s status=%s requested=%d inserted=%d present=%d unresolved=%d expired=%d", r.TaskID, name, r.Symbol, r.Timeframe, r.Status, r.Requested, r.Inserted, r.AlreadyPresent, len(r.MissingStarts), expired)
					if r.Error != "" {
						log.Printf("task=%s error=%q", r.TaskID, r.Error)
					}
					mu.Unlock()
					if stopExchange {
						mu.Lock()
						if runErr == nil {
							runErr = fmt.Errorf("%s circuit stopped after 8 failed tasks", name)
						}
						mu.Unlock()
						cancel()
						return
					}
				}
			}(name)
		}
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			exchangeWorkers.Wait()
			base.CloseIdleConnections()
			s := transport.Stats()
			log.Printf("exchange=%s REST requests=%d limited=%d failures=%d", name, s.Requests, s.Limited, s.Failures)
		}(name)
	}
	wg.Wait()
	status := "finished"
	if ctx.Err() != nil || runErr != nil {
		status = "stopped"
	}
	if err := writeSummary(status); err != nil {
		return err
	}
	if runErr != nil {
		return runErr
	}
	return ctx.Err()
}

func csvSet(raw string) map[string]bool {
	out := map[string]bool{}
	for _, v := range strings.Split(raw, ",") {
		if v = strings.TrimSpace(v); v != "" {
			out[v] = true
		}
	}
	return out
}

func retainedTask(t gapfill.Task, now time.Time) (gapfill.Task, int) {
	cutoff, ok := retention.CutoffMS(retention.RuleFor(t.Timeframe), now)
	if !ok {
		return t, 0
	}
	kept := make([]int64, 0, len(t.TargetStarts))
	for _, s := range t.TargetStarts {
		if s >= cutoff {
			kept = append(kept, s)
		}
	}
	expired := len(t.TargetStarts) - len(kept)
	t.TargetStarts = kept
	if len(kept) > 0 {
		t.StartMS = kept[0]
		t.EndMS = timeframe.EndMS(kept[len(kept)-1], t.Timeframe)
	}
	return t, expired
}

func safeError(err error) string {
	if err == nil {
		return ""
	}
	var u *url.Error
	if errors.As(err, &u) {
		return safeError(u.Err)
	}
	return err.Error()
}
func transientError(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var networkError net.Error
	if errors.As(err, &networkError) {
		return true
	}
	for depth := 0; err != nil && depth < 16; depth++ {
		s := strings.ToLower(safeError(err))
		for _, v := range []string{"timeout", "deadline", "429", "418", "50011", "50040", "50061", "502", "503", "504", "connection", "eof", "temporar"} {
			if strings.Contains(s, v) {
				return true
			}
		}
		err = errors.Unwrap(err)
	}
	return false
}

func openJournal(path, digest string, apply bool) (*os.File, map[string]gapfill.Result, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, nil, err
	}
	fail := func(err error) (*os.File, map[string]gapfill.Result, error) { f.Close(); return nil, nil, err }
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fail(errors.New("journal already locked by another run"))
	}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 65536), 16<<20)
	results := map[string]gapfill.Result{}
	lines := 0
	for scanner.Scan() {
		lines++
		var e journalEntry
		if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
			return fail(fmt.Errorf("invalid journal line %d", lines))
		}
		if lines == 1 && (e.Kind != "manifest" || e.ReportSHA256 != digest || e.Apply != apply) {
			return fail(errors.New("journal report hash/apply mode mismatch"))
		}
		if e.Result != nil {
			results[e.Result.TaskID] = *e.Result
		}
	}
	if err := scanner.Err(); err != nil {
		return fail(err)
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return fail(err)
	}
	if lines > 0 {
		stat, err := f.Stat()
		if err != nil {
			return fail(err)
		}
		last := make([]byte, 1)
		if _, err := f.ReadAt(last, stat.Size()-1); err != nil {
			return fail(err)
		}
		if last[0] != '\n' {
			if _, err := f.WriteString("\n"); err != nil {
				return fail(err)
			}
		}
	}
	if lines == 0 {
		if err := json.NewEncoder(f).Encode(journalEntry{Kind: "manifest", ReportSHA256: digest, Apply: apply, At: time.Now().UTC().Format(time.RFC3339)}); err != nil {
			return fail(err)
		}
	}
	return f, results, nil
}

func saveSummary(path, digest, generated, started, status string, apply bool, tasks []gapfill.Task, skipped []gapfill.Skipped, results map[string]gapfill.Result) error {
	s := runSummary{ReportSHA256: digest, AuditGeneratedAt: generated, StartedAt: started, UpdatedAt: time.Now().UTC().Format(time.RFC3339), Apply: apply, Status: status, Tasks: len(tasks), Skipped: skipped, ByStatus: map[string]int{}, ByExchange: map[string]exchangeSummary{}, ByKind: map[string]int{}, PlanByExchange: map[string]planCounts{}}
	if status == "planned" {
		s.PlannedTasks = tasks
	}
	for _, skip := range skipped {
		s.SkippedBars += skip.MissingBars
	}
	if status == "running" {
		s.Skipped = nil
	}
	for _, t := range tasks {
		s.Requested += len(t.TargetStarts)
		s.ByKind[t.Kind] += len(t.TargetStarts)
		p := s.PlanByExchange[t.Exchange]
		p.Tasks++
		p.Targets += len(t.TargetStarts)
		s.PlanByExchange[t.Exchange] = p
		if r, ok := results[t.ID]; ok {
			if status != "running" {
				s.Results = append(s.Results, r)
			}
			s.CompletedTasks++
			s.ByStatus[r.Status]++
			s.Inserted += r.Inserted
			s.AlreadyPresent += r.AlreadyPresent
			if r.Unverified {
				s.Unverified += len(r.MissingStarts)
			} else {
				s.Unresolved += len(r.MissingStarts)
			}
			s.Expired += r.Expired
			s.SkippedBars += r.Skipped
			e := s.ByExchange[t.Exchange]
			e.Tasks++
			e.Requested += r.Requested
			e.Inserted += r.Inserted
			e.AlreadyPresent += r.AlreadyPresent
			if !r.Unverified {
				e.Unresolved += len(r.MissingStarts)
			} else {
				e.Unverified += len(r.MissingStarts)
			}
			s.ByExchange[t.Exchange] = e
		}
	}
	s.RemainingTasks = s.Tasks - s.CompletedTasks
	sort.Slice(s.Results, func(i, j int) bool { return s.Results[i].TaskID < s.Results[j].TaskID })
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	if err := os.WriteFile(path+".tmp", b, 0600); err != nil {
		return err
	}
	return os.Rename(path+".tmp", path)
}

func shouldSaveSummary(status string, last, now time.Time) bool {
	return status != "running" || last.IsZero() || now.Sub(last) >= 5*time.Second
}

func retainedRetryStarts(previous, retained []int64) []int64 {
	wanted := make(map[int64]bool, len(retained))
	for _, start := range retained {
		wanted[start] = true
	}
	starts := make([]int64, 0, len(previous))
	for _, start := range previous {
		if wanted[start] {
			starts = append(starts, start)
		}
	}
	return starts
}
