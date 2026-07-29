package corporateaction

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"crypto-ticket/internal/aggregator"
	"crypto-ticket/internal/exchange"
	"crypto-ticket/internal/market"
	"crypto-ticket/internal/retention"
	"crypto-ticket/internal/timeframe"
)

const (
	statusPending   = "pending"
	statusRunning   = "running"
	statusRetry     = "retry"
	statusCompleted = "completed"
	statusFailed    = "failed"
)

type Config struct {
	Enabled             bool
	Timeframes          []string
	QueueSize           int
	PollInterval        time.Duration
	HTTPTimeout         time.Duration
	RequestDelay        time.Duration
	MaxAttempts         int
	RetryBaseDelay      time.Duration
	JobsPerRun          int
	BatchSize           int
	ConfirmationWindow  time.Duration
	AnchorInterval      time.Duration
	AnchorSymbolsPerRun int
	FloatTolerance      float64
}

type Store interface {
	BarsInRange(ctx context.Context, exchange string, symbol string, timeframe string, startMS int64, endMS int64) ([]market.Bar, error)
	ListSymbols(ctx context.Context, exchange string, activeOnly *bool) ([]market.SymbolInfo, error)
	UpsertBars(ctx context.Context, bars []market.Bar) error
	DeleteBarsInRange(ctx context.Context, exchange string, symbol string, timeframe string, startMS int64, endMS int64) (int64, error)
	LoadCorporateActionJob(ctx context.Context, exchange string, symbol string, effectiveMS int64) (*market.CorporateActionJob, error)
	InsertCorporateActionJob(ctx context.Context, job market.CorporateActionJob) error
	ListDueCorporateActionJobs(ctx context.Context, nowMS int64, limit int) ([]market.CorporateActionJob, error)
	UpdateCorporateActionJob(ctx context.Context, job market.CorporateActionJob) error
	ListCorporateActionFactors(ctx context.Context, exchange string, symbol string) ([]market.CorporateActionJob, error)
}

type Fetcher interface {
	Name() string
	MarketType() string
	FetchKlines(ctx context.Context, client *http.Client, request exchange.KlineRequest) ([]market.Bar, error)
}

type Notifier interface {
	Notify(ctx context.Context, notification Notification) error
}

type CacheClearer interface {
	ClearSymbolKlines(ctx context.Context, exchange string, symbol string, timeframes []string) (int64, error)
}

type Notification struct {
	Stage       string
	Title       string
	Job         market.CorporateActionJob
	Timeframes  []FrameReport
	Message     string
	RetryAtMS   int64
	RowsWritten int
}

type FrameReport struct {
	Timeframe       string `json:"timeframe"`
	Adjustment      string `json:"adjustment"`
	RowsFetched     int    `json:"rows_fetched"`
	RowsDeleted     int64  `json:"rows_deleted"`
	RowsWritten     int    `json:"rows_written"`
	RowsVerified    int    `json:"rows_verified"`
	MismatchCount   int    `json:"mismatch_count"`
	FirstStartMS    int64  `json:"first_start_ms,omitempty"`
	LastStartMS     int64  `json:"last_start_ms,omitempty"`
	VerificationErr string `json:"verification_error,omitempty"`
}

type Worker struct {
	cfg      Config
	store    Store
	client   *http.Client
	notifier Notifier
	cache    CacheClearer
	finals   chan market.Bar

	mu               sync.Mutex
	previous         map[string]market.Bar
	fetchersByMarket map[string]Fetcher
	fetchersByName   map[string][]Fetcher
	anchorCursor     int
	forwardCache     map[string]forwardSupportCache
}

type forwardSupportCache struct {
	Symbols   map[string]bool
	ExpiresAt time.Time
}

type forwardSymbolProvider interface {
	ForwardAdjustmentSymbols(context.Context, *http.Client) (map[string]bool, error)
}

func New(store Store, fetchers []Fetcher, notifier Notifier, cache CacheClearer, cfg Config) *Worker {
	cfg = normalizeConfig(cfg)
	w := &Worker{
		cfg:              cfg,
		store:            store,
		client:           &http.Client{Timeout: cfg.HTTPTimeout},
		notifier:         notifier,
		cache:            cache,
		finals:           make(chan market.Bar, cfg.QueueSize),
		previous:         make(map[string]market.Bar),
		fetchersByMarket: make(map[string]Fetcher),
		fetchersByName:   make(map[string][]Fetcher),
		forwardCache:     make(map[string]forwardSupportCache),
	}
	for _, fetcher := range fetchers {
		if fetcher == nil {
			continue
		}
		name := normalizeExchange(fetcher.Name())
		marketType := normalizeMarketType(fetcher.MarketType())
		w.fetchersByMarket[fetcherKey(name, marketType)] = fetcher
		w.fetchersByName[name] = append(w.fetchersByName[name], fetcher)
	}
	return w
}

func normalizeConfig(cfg Config) Config {
	if len(cfg.Timeframes) == 0 {
		cfg.Timeframes = append([]string(nil), timeframe.Order...)
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 1024
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 15 * time.Second
	}
	if cfg.HTTPTimeout <= 0 {
		cfg.HTTPTimeout = 20 * time.Second
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 5
	}
	if cfg.RetryBaseDelay <= 0 {
		cfg.RetryBaseDelay = time.Minute
	}
	if cfg.JobsPerRun <= 0 {
		cfg.JobsPerRun = 1
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 500
	}
	if cfg.ConfirmationWindow <= 0 {
		cfg.ConfirmationWindow = 5 * time.Minute
	}
	if cfg.AnchorInterval <= 0 {
		cfg.AnchorInterval = time.Minute
	}
	if cfg.AnchorSymbolsPerRun <= 0 {
		cfg.AnchorSymbolsPerRun = 1
	}
	if cfg.FloatTolerance <= 0 {
		cfg.FloatTolerance = 1e-7
	}
	return cfg
}

func (w *Worker) ObserveFinalBar(_ context.Context, bar market.Bar) error {
	if !w.cfg.Enabled || !bar.IsFinal || bar.Timeframe != aggregator.OneMinute {
		return nil
	}
	select {
	case w.finals <- market.DecorateBar(bar):
	default:
		log.Printf("corporate action final-bar queue full exchange=%s symbol=%s start_ms=%d", bar.Exchange, bar.Symbol, bar.StartMS)
	}
	return nil
}

func (w *Worker) Run(ctx context.Context) error {
	if !w.cfg.Enabled {
		return nil
	}
	errCh := make(chan error, 3)
	go func() { errCh <- w.runDetector(ctx) }()
	go func() { errCh <- w.runJobs(ctx) }()
	go func() { errCh <- w.runAnchorAudits(ctx) }()
	for i := 0; i < 3; i++ {
		if err := <-errCh; err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
	}
	return ctx.Err()
}

func (w *Worker) runDetector(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case bar := <-w.finals:
			if err := w.handleFinalBar(ctx, bar); err != nil {
				log.Printf("corporate action detection failed exchange=%s symbol=%s start_ms=%d err=%v", bar.Exchange, bar.Symbol, bar.StartMS, err)
			}
		}
	}
}

func (w *Worker) handleFinalBar(ctx context.Context, bar market.Bar) error {
	bar.Exchange = normalizeExchange(bar.Exchange)
	bar.Symbol = normalizeSymbol(bar.Symbol)
	key := bar.Exchange + ":" + bar.Symbol
	w.mu.Lock()
	previous, ok := w.previous[key]
	w.previous[key] = bar
	w.mu.Unlock()
	if !ok || previous.StartMS+timeframe.MinuteMS != bar.StartMS || previous.ClosePrice <= 0 || bar.OpenPrice <= 0 {
		return nil
	}
	observedRatio := bar.OpenPrice / previous.ClosePrice
	factor, ok := SnapFactor(observedRatio)
	if !ok {
		return nil
	}
	marketType, fetcher, err := w.symbolFetcher(ctx, bar.Exchange, bar.Symbol)
	if err != nil || fetcher == nil {
		return err
	}
	job := market.CorporateActionJob{
		Exchange: bar.Exchange, Symbol: bar.Symbol, MarketType: marketType,
		EffectiveMS: bar.StartMS, ObservedRatio: observedRatio, Factor: factor,
		Detector: "ws_1m_ratio", Status: "suspected", CreatedAtMS: market.NowMS(), UpdatedAtMS: market.NowMS(),
	}
	w.notify(ctx, Notification{Stage: "suspected", Title: "复权事件疑似触发", Job: job})
	confirmedRatio, confirmedFactor, supported, err := w.confirm(ctx, fetcher, previous, bar, factor)
	if err != nil {
		w.notify(ctx, Notification{Stage: "confirmation_failed", Title: "复权事件确认失败", Job: job, Message: err.Error()})
		return err
	}
	if !supported {
		w.notify(ctx, Notification{Stage: "unsupported", Title: "品种不支持自动前复权", Job: job, Message: "官方数据无法确认前复权能力，未修改历史数据"})
		return nil
	}
	job.ObservedRatio = confirmedRatio
	job.Factor = confirmedFactor
	job.Status = statusPending
	job.Detector = "ws_1m_ratio+rest_confirm"
	existing, err := w.store.LoadCorporateActionJob(ctx, job.Exchange, job.Symbol, job.EffectiveMS)
	if err != nil {
		return err
	}
	if existing != nil {
		return nil
	}
	if err := w.store.InsertCorporateActionJob(ctx, job); err != nil {
		return err
	}
	w.notify(ctx, Notification{Stage: "confirmed", Title: "复权事件已确认并入队", Job: job})
	return nil
}

func (w *Worker) confirm(ctx context.Context, fetcher Fetcher, previous market.Bar, current market.Bar, candidateFactor float64) (float64, float64, bool, error) {
	windowMS := int64(w.cfg.ConfirmationWindow / time.Millisecond)
	raw, err := fetcher.FetchKlines(ctx, w.client, exchange.KlineRequest{
		Symbol: current.Symbol, Timeframe: aggregator.OneMinute,
		StartMS: previous.StartMS - windowMS, EndMS: current.StartMS + windowMS,
		Limit: 20, Adjustment: exchange.KlineAdjustmentRaw,
	})
	if err != nil {
		return 0, 0, false, err
	}
	sort.Slice(raw, func(i, j int) bool { return raw[i].StartMS < raw[j].StartMS })
	confirmedRatio, confirmedFactor, ok := findConfirmedFactor(raw, current.StartMS, candidateFactor)
	if !ok {
		return 0, 0, false, fmt.Errorf("official raw 1m did not confirm ratio near %.8f", candidateFactor)
	}
	if normalizeExchange(fetcher.Name()) == "binance" {
		supported, err := w.binanceForwardSupported(ctx, fetcher, current.Symbol)
		return confirmedRatio, confirmedFactor, supported, err
	}
	_, err = fetcher.FetchKlines(ctx, w.client, exchange.KlineRequest{
		Symbol: current.Symbol, Timeframe: aggregator.OneMinute,
		StartMS: previous.StartMS - timeframe.MinuteMS, EndMS: current.StartMS,
		Limit: 4, Adjustment: exchange.KlineAdjustmentForward,
	})
	if errors.Is(err, exchange.ErrUnsupportedKlineAdjustment) {
		return confirmedRatio, confirmedFactor, false, nil
	}
	if err != nil {
		return 0, 0, false, err
	}
	return confirmedRatio, confirmedFactor, true, nil
}

func findConfirmedFactor(bars []market.Bar, effectiveMS int64, candidateFactor float64) (float64, float64, bool) {
	for i := 1; i < len(bars); i++ {
		if bars[i].StartMS-bars[i-1].StartMS != timeframe.MinuteMS || bars[i-1].ClosePrice <= 0 || bars[i].OpenPrice <= 0 {
			continue
		}
		if absDurationMS(bars[i].StartMS-effectiveMS) > 2*timeframe.MinuteMS {
			continue
		}
		ratio := bars[i].OpenPrice / bars[i-1].ClosePrice
		factor, ok := SnapFactor(ratio)
		if ok && relativeDifference(factor, candidateFactor) <= 0.08 {
			return ratio, factor, true
		}
	}
	return 0, 0, false
}

func SnapFactor(observedRatio float64) (float64, bool) {
	if observedRatio <= 0 {
		return 0, false
	}
	magnitude := observedRatio
	if magnitude < 1 {
		magnitude = 1 / magnitude
	}
	if magnitude < 1.75 {
		return 0, false
	}
	common := [...]float64{2, 2.5, 3, 4, 5, 10, 12.5, 20, 25, 50, 100}
	nearest := common[0]
	errorRate := relativeDifference(magnitude, nearest)
	for _, candidate := range common[1:] {
		if diff := relativeDifference(magnitude, candidate); diff < errorRate {
			nearest = candidate
			errorRate = diff
		}
	}
	if errorRate > 0.08 {
		return 0, false
	}
	if observedRatio < 1 {
		return 1 / nearest, true
	}
	return nearest, true
}

func (w *Worker) runJobs(ctx context.Context) error {
	ticker := time.NewTicker(w.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := w.ProcessDueJobs(ctx); err != nil {
				log.Printf("corporate action job poll failed: %v", err)
			}
		}
	}
}

func (w *Worker) ProcessDueJobs(ctx context.Context) error {
	jobs, err := w.store.ListDueCorporateActionJobs(ctx, market.NowMS(), w.cfg.JobsPerRun)
	if err != nil {
		return err
	}
	var firstErr error
	for _, job := range jobs {
		if err := w.processJob(ctx, job); err != nil {
			log.Printf("corporate action job failed exchange=%s symbol=%s effective_ms=%d err=%v", job.Exchange, job.Symbol, job.EffectiveMS, err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (w *Worker) processJob(ctx context.Context, job market.CorporateActionJob) error {
	job.Status = statusRunning
	job.Attempts++
	job.NextRetryMS = 0
	job.LastError = ""
	if err := w.store.UpdateCorporateActionJob(ctx, job); err != nil {
		return err
	}
	w.notify(ctx, Notification{Stage: "backfill_started", Title: "前复权历史回填开始", Job: job})
	reports, rowsWritten, err := w.backfill(ctx, job)
	if err != nil {
		job.LastError = err.Error()
		if job.Attempts >= w.cfg.MaxAttempts {
			job.Status = statusFailed
			job.NextRetryMS = 0
			w.notify(ctx, Notification{Stage: "failed", Title: "前复权历史回填最终失败", Job: job, Timeframes: reports, Message: err.Error()})
		} else {
			job.Status = statusRetry
			job.NextRetryMS = market.NowMS() + int64(w.retryDelay(job.Attempts)/time.Millisecond)
			w.notify(ctx, Notification{Stage: "retry", Title: "前复权历史回填等待重试", Job: job, Timeframes: reports, Message: err.Error(), RetryAtMS: job.NextRetryMS})
		}
		_ = w.store.UpdateCorporateActionJob(ctx, job)
		return err
	}
	verificationStatus := "passed"
	for _, report := range reports {
		if report.MismatchCount > 0 || report.VerificationErr != "" {
			verificationStatus = "failed"
			break
		}
	}
	reportJSON, _ := json.Marshal(reports)
	job.RowsWritten = rowsWritten
	job.VerificationStatus = verificationStatus
	job.VerificationJSON = string(reportJSON)
	job.CompletedAtMS = market.NowMS()
	job.Status = statusCompleted
	if err := w.store.UpdateCorporateActionJob(ctx, job); err != nil {
		return err
	}
	w.notify(ctx, Notification{Stage: "completed", Title: "前复权历史回填验证报告", Job: job, Timeframes: reports, RowsWritten: rowsWritten})
	return nil
}

func (w *Worker) retryDelay(attempt int) time.Duration {
	shift := attempt - 1
	if shift < 0 {
		shift = 0
	}
	if shift > 6 {
		shift = 6
	}
	return w.cfg.RetryBaseDelay * time.Duration(1<<shift)
}

func (w *Worker) backfill(ctx context.Context, job market.CorporateActionJob) ([]FrameReport, int, error) {
	fetcher := w.fetcherFor(job.Exchange, job.MarketType)
	if fetcher == nil {
		return nil, 0, fmt.Errorf("missing fetcher for %s %s", job.Exchange, job.MarketType)
	}
	factors, err := w.store.ListCorporateActionFactors(ctx, job.Exchange, job.Symbol)
	if err != nil {
		return nil, 0, err
	}
	now := time.Now().UTC()
	endMS := timeframe.FloorStartMS(now.Add(-2*time.Minute).UnixMilli(), aggregator.OneMinute) - 1
	reports := make([]FrameReport, 0, len(w.cfg.Timeframes))
	total := 0
	for i, tf := range w.cfg.Timeframes {
		startMS, limit := retainedWindow(tf, now)
		bars, adjustment, err := w.fetchAdjustedBars(ctx, fetcher, job, factors, tf, startMS, endMS, limit)
		report := FrameReport{Timeframe: tf, Adjustment: adjustment, RowsFetched: len(bars)}
		if err != nil {
			report.VerificationErr = err.Error()
			reports = append(reports, report)
			return reports, total, fmt.Errorf("backfill timeframe %s: %w", tf, err)
		}
		if len(bars) == 0 {
			reports = append(reports, report)
			continue
		}
		report.FirstStartMS = bars[0].StartMS
		report.LastStartMS = bars[len(bars)-1].StartMS
		deleted, err := w.store.DeleteBarsInRange(ctx, job.Exchange, job.Symbol, tf, bars[0].StartMS, bars[len(bars)-1].StartMS)
		if err != nil {
			return reports, total, fmt.Errorf("delete timeframe %s: %w", tf, err)
		}
		report.RowsDeleted = deleted
		for start := 0; start < len(bars); start += w.cfg.BatchSize {
			end := start + w.cfg.BatchSize
			if end > len(bars) {
				end = len(bars)
			}
			if err := w.store.UpsertBars(ctx, bars[start:end]); err != nil {
				return reports, total, fmt.Errorf("write timeframe %s: %w", tf, err)
			}
		}
		report.RowsWritten = len(bars)
		total += len(bars)
		verified, mismatches, verifyErr := w.verifyStored(ctx, job, tf, bars)
		report.RowsVerified = verified
		report.MismatchCount = mismatches
		if verifyErr != nil {
			report.VerificationErr = verifyErr.Error()
		}
		reports = append(reports, report)
		if verifyErr != nil || mismatches > 0 {
			return reports, total, fmt.Errorf("verification failed timeframe=%s checked=%d mismatches=%d err=%v", tf, verified, mismatches, verifyErr)
		}
		if w.cfg.RequestDelay > 0 && i < len(w.cfg.Timeframes)-1 {
			if err := wait(ctx, w.cfg.RequestDelay); err != nil {
				return reports, total, err
			}
		}
	}
	if w.cache != nil {
		if _, err := w.cache.ClearSymbolKlines(ctx, job.Exchange, job.Symbol, w.cfg.Timeframes); err != nil {
			return reports, total, fmt.Errorf("clear redis cache: %w", err)
		}
	}
	return reports, total, nil
}

func (w *Worker) fetchAdjustedBars(ctx context.Context, fetcher Fetcher, job market.CorporateActionJob, factors []market.CorporateActionJob, tf string, startMS int64, endMS int64, limit int) ([]market.Bar, string, error) {
	request := exchange.KlineRequest{Symbol: job.Symbol, Timeframe: tf, StartMS: startMS, EndMS: endMS, Limit: limit, PageDelay: w.cfg.RequestDelay}
	if normalizeExchange(job.Exchange) == "okx" {
		request.Adjustment = exchange.KlineAdjustmentForward
		bars, strategy, err := fetchWithRollup(ctx, w.client, fetcher, request, nil)
		if errors.Is(err, exchange.ErrUnsupportedKlineAdjustment) {
			request.Adjustment = exchange.KlineAdjustmentRaw
			bars, strategy, err = fetchWithRollup(ctx, w.client, fetcher, request, nil)
			return decorateBackfillBars(bars, "corporate_action_raw_fallback"), "raw-fallback/" + strategy, err
		}
		return decorateBackfillBars(bars, "corporate_action_forward"), "forward/" + strategy, err
	}
	request.Adjustment = exchange.KlineAdjustmentRaw
	bars, strategy, err := fetchWithRollup(ctx, w.client, fetcher, request, nil)
	if err != nil {
		return nil, "binance-forward/" + strategy, err
	}
	bars, err = w.adjustBinanceBars(ctx, fetcher, request, bars, factors)
	return bars, "binance-forward/" + strategy, err
}

func fetchWithRollup(ctx context.Context, client *http.Client, fetcher Fetcher, request exchange.KlineRequest, transform func([]market.Bar) []market.Bar) ([]market.Bar, string, error) {
	bars, err := fetcher.FetchKlines(ctx, client, request)
	if err == nil {
		return bars, "official", nil
	}
	if !errors.Is(err, exchange.ErrUnsupportedKlineInterval) {
		return nil, "official", err
	}
	sourceTF := aggregator.RollupSourceTimeframe(request.Timeframe)
	if sourceTF == "" {
		return nil, "official", err
	}
	sourceRequest := request
	sourceRequest.Timeframe = sourceTF
	if request.StartMS > 0 {
		sourceRequest.StartMS = timeframe.FloorStartMS(request.StartMS, request.Timeframe)
		if sourceRequest.StartMS <= 0 {
			sourceRequest.StartMS = 1
		}
		sourceRequest.Limit = 0
	} else if request.Limit > 0 {
		reference := request.EndMS
		if reference <= 0 {
			reference = market.NowMS()
		}
		windowStart := retention.BarWindowStartMS(request.Timeframe, reference, request.Limit+1)
		sourceRequest.StartMS = timeframe.FloorStartMS(windowStart, sourceTF)
		if sourceRequest.StartMS <= 0 {
			sourceRequest.StartMS = 1
		}
		sourceRequest.Limit = 0
	}
	source, sourceErr := fetcher.FetchKlines(ctx, client, sourceRequest)
	if sourceErr != nil {
		return nil, "rollup:" + sourceTF, sourceErr
	}
	if transform != nil {
		source = transform(source)
	}
	return deriveBars(request, source), "rollup:" + sourceTF, nil
}

func deriveBars(request exchange.KlineRequest, source []market.Bar) []market.Bar {
	groups := make(map[int64][]market.Bar)
	for _, bar := range source {
		if bar.IsFinal {
			start := timeframe.FloorStartMS(bar.StartMS, request.Timeframe)
			groups[start] = append(groups[start], bar)
		}
	}
	starts := make([]int64, 0, len(groups))
	for start := range groups {
		starts = append(starts, start)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
	var out []market.Bar
	for _, start := range starts {
		if timeframe.EndMS(start, request.Timeframe) >= market.NowMS() {
			continue
		}
		bar := aggregator.RollupBars(request.Timeframe, groups[start], true, "corporate_action_rollup", market.NowMS())
		if bar != nil {
			out = append(out, *bar)
		}
	}
	if request.Limit > 0 && len(out) > request.Limit {
		out = out[len(out)-request.Limit:]
	}
	return decorateBackfillBars(out, "corporate_action_rollup")
}

func (w *Worker) adjustBinanceBars(ctx context.Context, fetcher Fetcher, request exchange.KlineRequest, bars []market.Bar, factors []market.CorporateActionJob) ([]market.Bar, error) {
	out := append([]market.Bar(nil), bars...)
	sort.Slice(out, func(i, j int) bool { return out[i].StartMS < out[j].StartMS })
	for i := range out {
		crossesAction := false
		multiplier := float64(1)
		for _, action := range factors {
			if action.Factor <= 0 {
				continue
			}
			if out[i].StartMS < action.EffectiveMS && out[i].EndMS >= action.EffectiveMS {
				crossesAction = true
			}
			if out[i].EndMS < action.EffectiveMS {
				multiplier *= action.Factor
			}
		}
		if crossesAction && request.Timeframe != aggregator.OneMinute {
			raw, err := fetcher.FetchKlines(ctx, w.client, exchange.KlineRequest{
				Symbol: request.Symbol, Timeframe: aggregator.OneMinute,
				StartMS: out[i].StartMS, EndMS: out[i].EndMS, Limit: 0,
				Adjustment: exchange.KlineAdjustmentRaw, PageDelay: w.cfg.RequestDelay,
			})
			if err != nil {
				return nil, fmt.Errorf("fetch crossing %s bucket at %d: %w", request.Timeframe, out[i].StartMS, err)
			}
			adjustedSource := applyFactors(raw, factors)
			rollup := aggregator.RollupBars(request.Timeframe, adjustedSource, true, "binance_corporate_action_crossing_rollup", market.NowMS())
			if rollup == nil {
				return nil, fmt.Errorf("empty crossing %s bucket at %d", request.Timeframe, out[i].StartMS)
			}
			out[i] = *rollup
			continue
		}
		out[i].OpenPrice *= multiplier
		out[i].HighPrice *= multiplier
		out[i].LowPrice *= multiplier
		out[i].ClosePrice *= multiplier
		if multiplier != 0 {
			out[i].Volume /= multiplier
			out[i].ContractVolume /= multiplier
		}
		out[i].Source = "rest_forward_adjusted"
		out[i].Reason = "binance_corporate_action_forward_adjustment"
		out[i].UpdatedAtMS = market.NowMS()
	}
	return decorateBackfillBars(out, "binance_corporate_action_forward_adjustment"), nil
}

func applyFactors(bars []market.Bar, factors []market.CorporateActionJob) []market.Bar {
	out := append([]market.Bar(nil), bars...)
	sort.Slice(out, func(i, j int) bool { return out[i].StartMS < out[j].StartMS })
	previousClose := float64(0)
	for i := range out {
		multiplier := float64(1)
		for _, action := range factors {
			if out[i].StartMS < action.EffectiveMS && action.Factor > 0 {
				multiplier *= action.Factor
			}
		}
		out[i].OpenPrice *= multiplier
		out[i].HighPrice *= multiplier
		out[i].LowPrice *= multiplier
		out[i].ClosePrice *= multiplier
		if multiplier != 0 {
			out[i].Volume /= multiplier
			out[i].ContractVolume /= multiplier
		}
		out[i].Source = "rest_forward_adjusted"
		out[i].Reason = "binance_corporate_action_forward_adjustment"
		out[i].UpdatedAtMS = market.NowMS()
		out[i] = aggregator.ApplyDerived(out[i], previousClose)
		previousClose = out[i].ClosePrice
	}
	return out
}

func decorateBackfillBars(bars []market.Bar, reason string) []market.Bar {
	out := append([]market.Bar(nil), bars...)
	sort.Slice(out, func(i, j int) bool { return out[i].StartMS < out[j].StartMS })
	previousClose := float64(0)
	for i := range out {
		out[i].Source = "rest_forward_adjusted"
		out[i].Reason = reason
		out[i].UpdatedAtMS = market.NowMS()
		out[i] = aggregator.ApplyDerived(out[i], previousClose)
		previousClose = out[i].ClosePrice
	}
	return out
}

func (w *Worker) verifyStored(ctx context.Context, job market.CorporateActionJob, tf string, expected []market.Bar) (int, int, error) {
	local, err := w.store.BarsInRange(ctx, job.Exchange, job.Symbol, tf, expected[0].StartMS, expected[len(expected)-1].StartMS)
	if err != nil {
		return 0, 0, err
	}
	localByStart := make(map[int64]market.Bar, len(local))
	for _, bar := range local {
		localByStart[bar.StartMS] = bar
	}
	mismatches := 0
	for _, bar := range expected {
		stored, ok := localByStart[bar.StartMS]
		if !ok || barsDiffer(stored, bar, w.cfg.FloatTolerance) {
			mismatches++
		}
	}
	return len(expected), mismatches, nil
}

func (w *Worker) runAnchorAudits(ctx context.Context) error {
	ticker := time.NewTicker(w.cfg.AnchorInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := w.AnchorAuditOnce(ctx); err != nil {
				log.Printf("corporate action anchor audit failed: %v", err)
			}
		}
	}
}

func (w *Worker) AnchorAuditOnce(ctx context.Context) error {
	active := true
	var targets []market.SymbolInfo
	for name := range w.fetchersByName {
		if name != "okx" {
			continue
		}
		symbols, err := w.store.ListSymbols(ctx, name, &active)
		if err != nil {
			return err
		}
		targets = append(targets, symbols...)
	}
	if len(targets) == 0 {
		return nil
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].Symbol < targets[j].Symbol })
	count := w.cfg.AnchorSymbolsPerRun
	if count > len(targets) {
		count = len(targets)
	}
	for i := 0; i < count; i++ {
		info := targets[(w.anchorCursor+i)%len(targets)]
		fetcher := w.fetcherFor(info.Exchange, info.MarketType)
		if fetcher == nil {
			continue
		}
		official, err := fetcher.FetchKlines(ctx, w.client, exchange.KlineRequest{Symbol: info.Symbol, Timeframe: "1D", Limit: 3, Adjustment: exchange.KlineAdjustmentForward})
		if err != nil {
			continue
		}
		if candidate, ok, err := w.anchorCandidate(ctx, info, official); err != nil {
			return err
		} else if ok {
			existing, err := w.store.LoadCorporateActionJob(ctx, candidate.Exchange, candidate.Symbol, candidate.EffectiveMS)
			if err != nil {
				return err
			}
			if existing == nil {
				if err := w.store.InsertCorporateActionJob(ctx, candidate); err != nil {
					return err
				}
				w.notify(ctx, Notification{Stage: "confirmed", Title: "日线锚点发现前复权差异并入队", Job: candidate})
			}
		}
		if w.cfg.RequestDelay > 0 && i < count-1 {
			if err := wait(ctx, w.cfg.RequestDelay); err != nil {
				return err
			}
		}
	}
	w.anchorCursor = (w.anchorCursor + count) % len(targets)
	return nil
}

func (w *Worker) anchorCandidate(ctx context.Context, info market.SymbolInfo, official []market.Bar) (market.CorporateActionJob, bool, error) {
	if len(official) < 2 {
		return market.CorporateActionJob{}, false, nil
	}
	local, err := w.store.BarsInRange(ctx, info.Exchange, info.Symbol, "1D", official[0].StartMS, official[len(official)-1].StartMS)
	if err != nil {
		return market.CorporateActionJob{}, false, err
	}
	localByStart := make(map[int64]market.Bar, len(local))
	for _, bar := range local {
		localByStart[bar.StartMS] = bar
	}
	var mismatchRatios []float64
	matchingLater := false
	for _, bar := range official {
		stored, ok := localByStart[bar.StartMS]
		if !ok || stored.ClosePrice <= 0 {
			continue
		}
		ratio := bar.ClosePrice / stored.ClosePrice
		if relativeDifference(ratio, 1) > 0.005 {
			mismatchRatios = append(mismatchRatios, ratio)
		} else if len(mismatchRatios) > 0 {
			matchingLater = true
		}
	}
	if len(mismatchRatios) == 0 || !matchingLater {
		return market.CorporateActionJob{}, false, nil
	}
	effectiveMS := official[len(official)-1].StartMS
	for _, bar := range official {
		stored, ok := localByStart[bar.StartMS]
		if ok && stored.ClosePrice > 0 && relativeDifference(bar.ClosePrice/stored.ClosePrice, 1) <= 0.005 {
			effectiveMS = bar.StartMS
			break
		}
	}
	nowMS := market.NowMS()
	return market.CorporateActionJob{
		Exchange: normalizeExchange(info.Exchange), Symbol: normalizeSymbol(info.Symbol), MarketType: info.MarketType,
		EffectiveMS: effectiveMS, ObservedRatio: mismatchRatios[0], Factor: 1,
		Detector: "okx_forward_1d_anchor", Status: statusPending, CreatedAtMS: nowMS, UpdatedAtMS: nowMS,
	}, true, nil
}

func (w *Worker) binanceForwardSupported(ctx context.Context, fetcher Fetcher, symbol string) (bool, error) {
	provider, ok := fetcher.(forwardSymbolProvider)
	if !ok {
		return false, nil
	}
	key := fetcherKey(fetcher.Name(), fetcher.MarketType())
	w.mu.Lock()
	cached, found := w.forwardCache[key]
	w.mu.Unlock()
	if !found || time.Now().After(cached.ExpiresAt) {
		symbols, err := provider.ForwardAdjustmentSymbols(ctx, w.client)
		if err != nil {
			return false, err
		}
		cached = forwardSupportCache{Symbols: symbols, ExpiresAt: time.Now().Add(6 * time.Hour)}
		w.mu.Lock()
		w.forwardCache[key] = cached
		w.mu.Unlock()
	}
	return cached.Symbols[normalizeSymbol(symbol)], nil
}

func (w *Worker) symbolFetcher(ctx context.Context, exchangeName string, symbol string) (string, Fetcher, error) {
	active := true
	symbols, err := w.store.ListSymbols(ctx, exchangeName, &active)
	if err != nil {
		return "", nil, err
	}
	for _, info := range symbols {
		if normalizeSymbol(info.Symbol) == normalizeSymbol(symbol) {
			return info.MarketType, w.fetcherFor(exchangeName, info.MarketType), nil
		}
	}
	fetchers := w.fetchersByName[normalizeExchange(exchangeName)]
	if len(fetchers) == 1 {
		return fetchers[0].MarketType(), fetchers[0], nil
	}
	return "", nil, nil
}

func (w *Worker) fetcherFor(exchangeName string, marketType string) Fetcher {
	if fetcher := w.fetchersByMarket[fetcherKey(exchangeName, marketType)]; fetcher != nil {
		return fetcher
	}
	fetchers := w.fetchersByName[normalizeExchange(exchangeName)]
	if len(fetchers) == 1 {
		return fetchers[0]
	}
	return nil
}

func (w *Worker) notify(ctx context.Context, notification Notification) {
	if w.notifier == nil {
		return
	}
	if err := w.notifier.Notify(ctx, notification); err != nil {
		log.Printf("corporate action notification failed stage=%s exchange=%s symbol=%s err=%v", notification.Stage, notification.Job.Exchange, notification.Job.Symbol, err)
	}
}

func retainedStartMS(tf string, now time.Time) int64 {
	start, _ := retainedWindow(tf, now)
	return start
}

func retainedWindow(tf string, now time.Time) (int64, int) {
	rule := retention.RuleFor(tf)
	if rule.KeepBars > 0 {
		return 0, rule.KeepBars
	}
	cutoff, ok := retention.CutoffMS(rule, now)
	if !ok {
		return 1, 0
	}
	start := timeframe.FloorStartMS(cutoff, tf)
	if start <= 0 {
		return 1, 0
	}
	return start, 0
}

func barsDiffer(left market.Bar, right market.Bar, tolerance float64) bool {
	return relativeDifference(left.OpenPrice, right.OpenPrice) > tolerance ||
		relativeDifference(left.HighPrice, right.HighPrice) > tolerance ||
		relativeDifference(left.LowPrice, right.LowPrice) > tolerance ||
		relativeDifference(left.ClosePrice, right.ClosePrice) > tolerance ||
		relativeDifference(left.Volume, right.Volume) > tolerance ||
		relativeDifference(left.QuoteVolume, right.QuoteVolume) > tolerance
}

func relativeDifference(left float64, right float64) float64 {
	scale := math.Max(math.Abs(left), math.Abs(right))
	if scale == 0 {
		return 0
	}
	return math.Abs(left-right) / scale
}

func normalizeExchange(value string) string { return strings.ToLower(strings.TrimSpace(value)) }
func normalizeSymbol(value string) string   { return strings.ToUpper(strings.TrimSpace(value)) }
func normalizeMarketType(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}
func fetcherKey(exchangeName string, marketType string) string {
	return normalizeExchange(exchangeName) + ":" + normalizeMarketType(marketType)
}
func absDurationMS(value int64) int64 {
	if value < 0 {
		return -value
	}
	return value
}
func wait(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
