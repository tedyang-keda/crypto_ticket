package monitoring

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"crypto-ticket/internal/market"
	"crypto-ticket/internal/notify"
)

type Pinger interface {
	Ping(context.Context) error
}

type ActivityStore interface {
	ContinuousOneMinuteSeries(ctx context.Context, startMS int64, endMS int64, minimumHours int) ([]market.MarketSeries, error)
}

type DBStatsProvider interface {
	DBStats() sql.DBStats
}

type Config struct {
	Enabled            bool
	CollectorEnabled   bool
	P1AlertsEnabled    bool
	EvaluationInterval time.Duration
	DatabaseTimeout    time.Duration
	StartupGrace       time.Duration
	WSWarningAge       time.Duration
	WSCriticalAge      time.Duration
	FinalCriticalAge   time.Duration
	DailyReportHour    int
	DailyLocation      *time.Location
	DiskPath           string
}

type Check struct {
	Name       string `json:"name"`
	OK         bool   `json:"ok"`
	Status     string `json:"status"`
	Message    string `json:"message,omitempty"`
	LastOKMS   int64  `json:"last_ok_ms,omitempty"`
	AgeSeconds int64  `json:"age_seconds,omitempty"`
}

type ReadyReport struct {
	OK            bool              `json:"ok"`
	TSMS          int64             `json:"ts_ms"`
	StartedAtMS   int64             `json:"started_at_ms"`
	UptimeSeconds int64             `json:"uptime_seconds"`
	Checks        []Check           `json:"checks"`
	Runtimes      []RuntimeSnapshot `json:"runtimes,omitempty"`
}

type Service struct {
	registry      *Registry
	pinger        Pinger
	activityStore ActivityStore
	dbStats       DBStatsProvider
	alerts        *AlertEngine
	cfg           Config

	mu              sync.Mutex
	dbFailures      int
	dbLastOK        time.Time
	dbLastError     string
	lastActivityAt  time.Time
	resourceSampler ResourceSampler
	aboveSince      map[string]time.Time
}

func NewService(registry *Registry, pinger Pinger, activityStore ActivityStore, webhookURL string, cfg Config) *Service {
	if registry == nil {
		registry = NewRegistry()
	}
	if cfg.EvaluationInterval <= 0 {
		cfg.EvaluationInterval = 15 * time.Second
	}
	if cfg.DatabaseTimeout <= 0 {
		cfg.DatabaseTimeout = 2 * time.Second
	}
	if cfg.StartupGrace <= 0 {
		cfg.StartupGrace = 3 * time.Minute
	}
	if cfg.WSWarningAge <= 0 {
		cfg.WSWarningAge = 90 * time.Second
	}
	if cfg.WSCriticalAge <= 0 {
		cfg.WSCriticalAge = 3 * time.Minute
	}
	if cfg.FinalCriticalAge <= 0 {
		cfg.FinalCriticalAge = 2 * time.Minute
	}
	if cfg.DailyReportHour < 0 || cfg.DailyReportHour > 23 {
		cfg.DailyReportHour = 9
	}
	if cfg.DailyLocation == nil {
		cfg.DailyLocation = time.FixedZone("Asia/Shanghai", 8*60*60)
	}
	if strings.TrimSpace(cfg.DiskPath) == "" {
		cfg.DiskPath = "."
	}
	notifier := notify.NewFeishuClient(webhookURL, nil)
	service := &Service{
		registry: registry, pinger: pinger, activityStore: activityStore, cfg: cfg,
		alerts: NewAlertEngine(notifier, registry, cfg.P1AlertsEnabled), aboveSince: make(map[string]time.Time),
	}
	if provider, ok := pinger.(DBStatsProvider); ok {
		service.dbStats = provider
	}
	return service
}

func (s *Service) Registry() *Registry {
	return s.registry
}

func (s *Service) StartedAt() time.Time {
	return s.registry.StartedAt()
}

func (s *Service) Run(ctx context.Context) error {
	if !s.cfg.Enabled {
		<-ctx.Done()
		return ctx.Err()
	}
	s.refreshContinuousSeries(ctx)
	evaluation := time.NewTicker(s.cfg.EvaluationInterval)
	activity := time.NewTicker(time.Hour)
	daily := time.NewTimer(s.untilNextDaily(s.registry.now()))
	defer evaluation.Stop()
	defer activity.Stop()
	defer daily.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-evaluation.C:
			s.EvaluateOnce(ctx)
		case <-activity.C:
			s.refreshContinuousSeries(ctx)
		case <-daily.C:
			if err := s.sendDailyReport(ctx); err != nil {
				log.Printf("monitoring daily report failed: %v", err)
			}
			daily.Reset(s.untilNextDaily(s.registry.now()))
		}
	}
}

func (s *Service) EvaluateOnce(ctx context.Context) {
	s.checkDatabase(ctx)
	if s.dbStats != nil {
		s.registry.SetMySQLPoolStats(s.dbStats.DBStats())
	}
	now := s.registry.now()
	s.registry.SetResources(s.resourceSampler.Sample(now, s.cfg.DiskPath))
	snapshot := s.registry.Snapshot(now)
	conditions := s.conditions(now, snapshot)
	s.alerts.Evaluate(ctx, conditions)
}

func (s *Service) Readiness(ctx context.Context) ReadyReport {
	now := s.registry.now()
	report := ReadyReport{
		OK: true, TSMS: now.UnixMilli(), StartedAtMS: s.registry.StartedAt().UnixMilli(),
		UptimeSeconds: int64(now.Sub(s.registry.StartedAt()).Seconds()),
	}
	dbCheck := Check{Name: "mysql", OK: true, Status: "ok"}
	if s.pinger != nil {
		checkCtx, cancel := context.WithTimeout(ctx, s.cfg.DatabaseTimeout)
		started := time.Now()
		err := s.pinger.Ping(checkCtx)
		cancel()
		s.registry.StorageOperation("ping", time.Since(started), err)
		if err != nil {
			dbCheck.OK = false
			dbCheck.Status = "unavailable"
			dbCheck.Message = err.Error()
			report.OK = false
		}
	}
	report.Checks = append(report.Checks, dbCheck)
	if !s.cfg.CollectorEnabled {
		report.Checks = append(report.Checks, Check{Name: "collector", OK: true, Status: "disabled"})
		return report
	}
	snapshot := s.registry.Snapshot(now)
	report.Runtimes = snapshot.Runtimes
	for _, runtime := range snapshot.Runtimes {
		name := "collector:" + runtime.Exchange + ":" + runtime.MarketType
		check := Check{Name: name, OK: true, Status: "ok", LastOKMS: runtime.LastBarMS}
		age := ageFromMS(now, runtime.LastBarMS)
		check.AgeSeconds = int64(age.Seconds())
		if now.Sub(snapshot.StartedAt) < s.cfg.StartupGrace && runtime.LastBarMS == 0 {
			check.Status = "initializing"
		} else if runtime.LastBarMS == 0 || age >= s.cfg.WSCriticalAge {
			check.OK = false
			check.Status = "stale"
			check.Message = fmt.Sprintf("no valid WS kline for %s", age.Round(time.Second))
			report.OK = false
		}
		if runtime.LastBarMS > 0 && age < s.cfg.WSWarningAge && now.Sub(snapshot.StartedAt) >= s.cfg.FinalCriticalAge {
			persistAge := ageFromMS(now, runtime.LastPersistedMS)
			if runtime.LastPersistedMS == 0 || persistAge >= s.cfg.FinalCriticalAge {
				check.OK = false
				check.Status = "persist_stale"
				check.Message = fmt.Sprintf("final 1m persist watermark stale for %s", persistAge.Round(time.Second))
				report.OK = false
			} else {
				publishAge := ageFromMS(now, runtime.LastPublishedMS)
				if runtime.LastPublishedMS == 0 || publishAge >= s.cfg.FinalCriticalAge {
					check.OK = false
					check.Status = "publish_stale"
					check.Message = fmt.Sprintf("final 1m publish watermark stale for %s", publishAge.Round(time.Second))
					report.OK = false
				}
			}
		}
		report.Checks = append(report.Checks, check)
	}
	if len(snapshot.Runtimes) == 0 && now.Sub(snapshot.StartedAt) >= s.cfg.StartupGrace {
		report.OK = false
		report.Checks = append(report.Checks, Check{Name: "collector", OK: false, Status: "missing", Message: "no collector runtimes registered"})
	}
	return report
}

func (s *Service) conditions(now time.Time, snapshot Snapshot) []Condition {
	var out []Condition
	s.mu.Lock()
	dbFailures := s.dbFailures
	dbLastError := s.dbLastError
	s.mu.Unlock()
	out = append(out, Condition{
		Key: "mysql_unavailable", Title: "MySQL 不可用", Severity: SeverityCritical,
		Message: fmt.Sprintf("**连续失败**: `%d`\n**错误**: %s", dbFailures, dbLastError), Active: dbFailures >= 3,
	})
	for _, runtime := range snapshot.Runtimes {
		prefix := runtime.Exchange + ":" + runtime.MarketType
		barAge := ageFromMS(now, runtime.LastBarMS)
		staleSeverity := SeverityWarning
		if runtime.LastBarMS == 0 || barAge >= s.cfg.WSCriticalAge {
			staleSeverity = SeverityCritical
		}
		staleActive := now.Sub(snapshot.StartedAt) >= s.cfg.StartupGrace && (runtime.LastBarMS == 0 || barAge >= s.cfg.WSWarningAge)
		out = append(out, Condition{
			Key: "collector_stale:" + prefix, Title: "交易所行情停滞", Severity: staleSeverity, Active: staleActive,
			Message: fmt.Sprintf("**市场**: `%s`\n**最后有效 K 线**: `%s`\n**延迟**: `%s`", prefix, formatMS(runtime.LastBarMS), barAge.Round(time.Second)),
		})
		persistAge := ageFromMS(now, runtime.LastPersistedMS)
		persistActive := now.Sub(snapshot.StartedAt) >= s.cfg.FinalCriticalAge && runtime.LastBarMS > 0 && barAge < s.cfg.WSWarningAge && (runtime.LastPersistedMS == 0 || persistAge >= s.cfg.FinalCriticalAge)
		out = append(out, Condition{
			Key: "persist_stale:" + prefix, Title: "final 1m 入库停滞", Severity: SeverityCritical, Active: persistActive,
			Message: fmt.Sprintf("**市场**: `%s`\n**最后入库**: `%s`\n**延迟**: `%s`", prefix, formatMS(runtime.LastPersistedMS), persistAge.Round(time.Second)),
		})
		publishAge := ageFromMS(now, runtime.LastPublishedMS)
		publishActive := runtime.LastPersistedMS > 0 && persistAge < s.cfg.FinalCriticalAge && (runtime.LastPublishedMS == 0 || publishAge >= s.cfg.FinalCriticalAge)
		out = append(out, Condition{
			Key: "publish_stale:" + prefix, Title: "实时行情发布停滞", Severity: SeverityCritical, Active: publishActive,
			Message: fmt.Sprintf("**市场**: `%s`\n**最后发布**: `%s`\n**延迟**: `%s`", prefix, formatMS(runtime.LastPublishedMS), publishAge.Round(time.Second)),
		})
		reconnectSeverity := SeverityWarning
		reconnectActive := runtime.Reconnects5m >= 3
		if runtime.Reconnects10m >= 10 {
			reconnectSeverity = SeverityCritical
			reconnectActive = true
		}
		out = append(out, Condition{
			Key: "reconnect_storm:" + prefix, Title: "Collector 频繁重连", Severity: reconnectSeverity, Active: reconnectActive,
			Message: fmt.Sprintf("**市场**: `%s`\n**5 分钟**: `%d`\n**10 分钟**: `%d`", prefix, runtime.Reconnects5m, runtime.Reconnects10m),
		})
	}

	repairErrors := snapshot.Window.GuardianEvents5m["repair_error"]
	out = append(out, Condition{Key: "guardian_repair_error", Title: "Guardian 修复失败", Severity: SeverityCritical,
		Active: repairErrors >= 3, Message: fmt.Sprintf("**5 分钟 repair_error**: `%d`", repairErrors)})
	repairs := snapshot.Window.GuardianEvents5m["missing_repair"] + snapshot.Window.GuardianEvents5m["mismatch_repair"]
	out = append(out, Condition{Key: "guardian_repair_spike", Title: "Guardian 修复数量异常", Severity: SeverityWarning, P1: true,
		Active:  repairs >= 10 || len(snapshot.Window.GuardianSymbols5m) >= 5,
		Message: fmt.Sprintf("**5 分钟修复**: `%d`\n**涉及品种**: `%d`", repairs, len(snapshot.Window.GuardianSymbols5m))})
	queueDrops := snapshot.Window.GuardianEvents5m["queue_dropped"]
	queueSeverity := SeverityWarning
	if queueDrops >= 10 {
		queueSeverity = SeverityCritical
	}
	out = append(out, Condition{Key: "guardian_queue_drop", Title: "Guardian 队列丢弃", Severity: queueSeverity,
		Active: queueDrops > 0, Message: fmt.Sprintf("**5 分钟丢弃**: `%d`", queueDrops)})

	if snapshot.Window.HTTPRequests5m >= 20 {
		ratio := float64(snapshot.Window.HTTP5xx5m) / float64(snapshot.Window.HTTPRequests5m)
		severity := SeverityWarning
		if ratio >= 0.20 {
			severity = SeverityCritical
		}
		out = append(out, Condition{Key: "http_5xx", Title: "HTTP 5xx 比例异常", Severity: severity, P1: true,
			Active: ratio >= 0.05, Message: fmt.Sprintf("**5 分钟请求**: `%d`\n**5xx**: `%d` (`%.2f%%`)", snapshot.Window.HTTPRequests5m, snapshot.Window.HTTP5xx5m, ratio*100)})
		latencySeverity := SeverityWarning
		if snapshot.Window.HTTPP95 >= 3*time.Second {
			latencySeverity = SeverityCritical
		}
		out = append(out, Condition{Key: "http_latency", Title: "HTTP 延迟异常", Severity: latencySeverity, P1: true,
			Active: s.sustained("http_latency", snapshot.Window.HTTPP95 >= time.Second, 5*time.Minute, now), Message: fmt.Sprintf("**5 分钟 P95**: `%s`", snapshot.Window.HTTPP95.Round(time.Millisecond))})
	}
	wsSeverity := SeverityWarning
	if snapshot.Window.WSDrops5m >= 100 {
		wsSeverity = SeverityCritical
	}
	out = append(out, Condition{Key: "ws_slow_consumers", Title: "WebSocket 慢消费者丢数据", Severity: wsSeverity, P1: true,
		Active: snapshot.Window.WSDrops5m >= 10, Message: fmt.Sprintf("**5 分钟丢弃事件**: `%d`", snapshot.Window.WSDrops5m)})
	out = append(out, Condition{Key: "mysql_latency", Title: "MySQL 延迟异常", Severity: SeverityWarning, P1: true,
		Active: s.sustained("mysql_latency", snapshot.Window.StorageP95 >= 500*time.Millisecond, 5*time.Minute, now), Message: fmt.Sprintf("**5 分钟 P95**: `%s`", snapshot.Window.StorageP95.Round(time.Millisecond))})

	integrityTotal := snapshot.Window.Integrity.Missing + snapshot.Window.Integrity.Mismatch + snapshot.Window.Integrity.InvalidOHLC
	integritySeverity := SeverityWarning
	if integrityTotal >= 20 || len(snapshot.Window.Integrity.Affected) >= 5 {
		integritySeverity = SeverityCritical
	}
	out = append(out, Condition{Key: "high_timeframe_integrity", Title: "高级周期 K 线完整性异常", Severity: integritySeverity, P1: true,
		Active: integrityTotal > 0, Message: formatIntegrity(snapshot.Window.Integrity)})
	out = append(out, s.symbolStaleConditions(now, snapshot)...)
	out = append(out, s.resourceConditions(now, snapshot.Resources)...)
	return out
}

func (s *Service) resourceConditions(now time.Time, resources ResourceSnapshot) []Condition {
	cpuActive := s.sustained("cpu", resources.CPURatio >= 0.85, 10*time.Minute, now)
	memoryActive := s.sustained("memory", resources.MemoryRatio >= 0.80, 5*time.Minute, now)
	fdActive := s.sustained("fd", resources.FDRatio >= 0.80, 5*time.Minute, now)
	goroutineActive := s.sustained("goroutines", resources.Goroutines >= 5000, 10*time.Minute, now)
	memorySeverity := SeverityWarning
	if resources.MemoryRatio >= 0.95 {
		memorySeverity = SeverityCritical
	}
	fdSeverity := SeverityWarning
	if resources.FDRatio >= 0.95 {
		fdSeverity = SeverityCritical
	}
	return []Condition{
		{Key: "process_cpu", Title: "进程 CPU 使用率异常", Severity: SeverityWarning, P1: true, Active: cpuActive, Message: fmt.Sprintf("**CPU 占整机比例**: `%.2f%%`", resources.CPURatio*100)},
		{Key: "process_memory", Title: "进程内存使用率异常", Severity: memorySeverity, P1: true, Active: memoryActive, Message: fmt.Sprintf("**RSS**: `%d`\n**占整机内存**: `%.2f%%`", resources.RSSBytes, resources.MemoryRatio*100)},
		{Key: "process_fd", Title: "进程文件描述符使用率异常", Severity: fdSeverity, P1: true, Active: fdActive, Message: fmt.Sprintf("**FD**: `%d/%d` (`%.2f%%`)", resources.OpenFDs, resources.FDLimit, resources.FDRatio*100)},
		{Key: "process_goroutines", Title: "goroutine 数量异常", Severity: SeverityWarning, P1: true, Active: goroutineActive, Message: fmt.Sprintf("**goroutines**: `%d`", resources.Goroutines)},
	}
}

func (s *Service) sustained(key string, above bool, required time.Duration, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !above {
		delete(s.aboveSince, key)
		return false
	}
	started := s.aboveSince[key]
	if started.IsZero() {
		s.aboveSince[key] = now
		return false
	}
	return now.Sub(started) >= required
}

func (s *Service) symbolStaleConditions(now time.Time, snapshot Snapshot) []Condition {
	grouped := make(map[string][]SymbolSnapshot)
	for _, symbol := range snapshot.Symbols {
		if !symbol.Continuous || symbol.MarketType == "" {
			continue
		}
		key := symbol.Exchange + ":" + symbol.MarketType
		grouped[key] = append(grouped[key], symbol)
	}
	var out []Condition
	for key, symbols := range grouped {
		if len(symbols) < 5 {
			continue
		}
		fresh := 0
		var stale []string
		critical := false
		for _, symbol := range symbols {
			age := ageFromMS(now, symbol.LastFinalMS)
			if symbol.LastFinalMS > 0 && age < 2*time.Minute {
				fresh++
			}
			if symbol.LastFinalMS == 0 || age >= 3*time.Minute {
				stale = append(stale, symbol.Symbol)
				if age >= 10*time.Minute {
					critical = true
				}
			}
		}
		if float64(fresh)/float64(len(symbols)) < 0.80 || len(stale) == 0 {
			out = append(out, Condition{Key: "symbol_stale:" + key, Title: "局部品种行情停滞", Severity: SeverityWarning, P1: true, Active: false})
			continue
		}
		sort.Strings(stale)
		display := stale
		if len(display) > 20 {
			display = display[:20]
		}
		severity := SeverityWarning
		if critical {
			severity = SeverityCritical
		}
		out = append(out, Condition{Key: "symbol_stale:" + key, Title: "局部品种行情停滞", Severity: severity, P1: true, Active: true,
			Message: fmt.Sprintf("**市场**: `%s`\n**异常品种**: `%d`\n%s", key, len(stale), strings.Join(display, ", "))})
	}
	return out
}

func (s *Service) checkDatabase(ctx context.Context) {
	if s.pinger == nil {
		return
	}
	checkCtx, cancel := context.WithTimeout(ctx, s.cfg.DatabaseTimeout)
	started := time.Now()
	err := s.pinger.Ping(checkCtx)
	cancel()
	s.registry.StorageOperation("ping", time.Since(started), err)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		s.dbFailures++
		s.dbLastError = err.Error()
		return
	}
	s.dbFailures = 0
	s.dbLastError = ""
	s.dbLastOK = s.registry.now()
}

func (s *Service) refreshContinuousSeries(ctx context.Context) {
	if s.activityStore == nil {
		return
	}
	now := s.registry.now()
	series, err := s.activityStore.ContinuousOneMinuteSeries(ctx, now.Add(-72*time.Hour).UnixMilli(), now.UnixMilli(), 70)
	if err != nil {
		log.Printf("monitoring continuous symbol refresh failed: %v", err)
		return
	}
	s.registry.SetContinuousSeries(series)
	s.mu.Lock()
	s.lastActivityAt = now
	s.mu.Unlock()
}

func (s *Service) sendDailyReport(ctx context.Context) error {
	now := s.registry.now()
	snapshot := s.registry.Snapshot(now)
	warning, critical, active := s.alerts.Active()
	lines := []string{
		fmt.Sprintf("**时间**: `%s`", now.In(s.cfg.DailyLocation).Format(time.RFC3339)),
		fmt.Sprintf("**运行时间**: `%s`", now.Sub(snapshot.StartedAt).Round(time.Second)),
		fmt.Sprintf("**活跃告警**: warning=%d, critical=%d", warning, critical),
		fmt.Sprintf("**HTTP**: requests=%d, 5xx=%d, P95=%s", snapshot.Window.HTTPRequestsTotal, snapshot.Window.HTTP5xxTotal, snapshot.Window.HTTPP95.Round(time.Millisecond)),
		fmt.Sprintf("**WebSocket 丢弃**: `%d`", snapshot.Window.WSDropsTotal),
		fmt.Sprintf("**资源**: CPU=%.2f%%, RSS=%d, FD=%d/%d, goroutines=%d, disk=%.2f%%", snapshot.Resources.CPURatio*100, snapshot.Resources.RSSBytes, snapshot.Resources.OpenFDs, snapshot.Resources.FDLimit, snapshot.Resources.Goroutines, snapshot.Resources.DiskRatio*100),
		fmt.Sprintf("**Guardian 累计**: `%s`", formatUintMap(snapshot.Window.GuardianTotals)),
		"**高级周期审计**: " + formatIntegrity(snapshot.Window.Integrity),
	}
	for _, runtime := range snapshot.Runtimes {
		lines = append(lines, fmt.Sprintf("- `%s:%s`: last=%s, persist=%s, publish=%s, reconnect10m=%d",
			runtime.Exchange, runtime.MarketType, formatMS(runtime.LastBarMS), formatMS(runtime.LastPersistedMS), formatMS(runtime.LastPublishedMS), runtime.Reconnects10m))
	}
	if len(active) > 0 {
		lines = append(lines, "**当前告警**:", strings.Join(active, "\n"))
	}
	wouldFire := s.alerts.WouldFire()
	if len(wouldFire) > 0 {
		lines = append(lines, "**P1 影子告警**: `"+formatUintMap(wouldFire)+"`")
	}
	return s.alerts.SendDaily(ctx, strings.Join(lines, "\n"))
}

func (s *Service) untilNextDaily(now time.Time) time.Duration {
	local := now.In(s.cfg.DailyLocation)
	next := time.Date(local.Year(), local.Month(), local.Day(), s.cfg.DailyReportHour, 0, 0, 0, s.cfg.DailyLocation)
	if !next.After(local) {
		next = next.AddDate(0, 0, 1)
	}
	return next.Sub(local)
}

func ageFromMS(now time.Time, value int64) time.Duration {
	if value <= 0 {
		return time.Duration(math.MaxInt64)
	}
	age := now.Sub(time.UnixMilli(value))
	if age < 0 {
		return 0
	}
	return age
}

func formatMS(value int64) string {
	if value <= 0 {
		return "never"
	}
	return time.UnixMilli(value).Format(time.RFC3339)
}

func formatIntegrity(summary IntegritySummary) string {
	return fmt.Sprintf("checked=%d, missing=%d, mismatch=%d, invalid_ohlc=%d, affected=%d",
		summary.CheckedSymbols, summary.Missing, summary.Mismatch, summary.InvalidOHLC, len(summary.Affected))
}

func formatUintMap(values map[string]uint64) string {
	if len(values) == 0 {
		return "none"
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", key, values[key]))
	}
	return strings.Join(parts, ", ")
}
