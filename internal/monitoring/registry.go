package monitoring

import (
	"database/sql"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"crypto-ticket/internal/market"
)

const observationRetention = 24 * time.Hour

type runtimeKey struct {
	Exchange   string
	MarketType string
}

type runtimeState struct {
	Connected          int
	LastMessageAt      time.Time
	LastBarAt          time.Time
	LastFinalAt        time.Time
	LastFinalStartMS   int64
	LastPersistedAt    time.Time
	LastPublishedAt    time.Time
	Reconnects         []time.Time
	ParseErrors        []time.Time
	IngestErrors       []time.Time
	ReceivedTotal      uint64
	FinalIngestedTotal uint64
}

type symbolState struct {
	Runtime          runtimeKey
	LastFinalAt      time.Time
	LastFinalStartMS int64
	Continuous       bool
}

type timedGuardianEvent struct {
	At     time.Time
	Event  string
	Symbol string
}

type httpObservation struct {
	At         time.Time
	StatusCode int
	Duration   time.Duration
}

type storageObservation struct {
	At        time.Time
	Operation string
	Duration  time.Duration
	Failed    bool
}

type IntegritySummary struct {
	CheckedAt      time.Time         `json:"checked_at"`
	CheckedSymbols int               `json:"checked_symbols"`
	Missing        int               `json:"missing"`
	Mismatch       int               `json:"mismatch"`
	InvalidOHLC    int               `json:"invalid_ohlc"`
	Affected       []string          `json:"affected,omitempty"`
	ByTimeframe    map[string][3]int `json:"by_timeframe,omitempty"`
}

type RuntimeSnapshot struct {
	Exchange           string `json:"exchange"`
	MarketType         string `json:"market_type"`
	Connected          int    `json:"connected"`
	LastMessageMS      int64  `json:"last_message_ms"`
	LastBarMS          int64  `json:"last_bar_ms"`
	LastFinalMS        int64  `json:"last_final_ms"`
	LastFinalStartMS   int64  `json:"last_final_start_ms"`
	LastPersistedMS    int64  `json:"last_persisted_ms"`
	LastPublishedMS    int64  `json:"last_published_ms"`
	Reconnects5m       int    `json:"reconnects_5m"`
	Reconnects10m      int    `json:"reconnects_10m"`
	ParseErrors5m      int    `json:"parse_errors_5m"`
	IngestErrors5m     int    `json:"ingest_errors_5m"`
	ReceivedTotal      uint64 `json:"received_total"`
	FinalIngestedTotal uint64 `json:"final_ingested_total"`
}

type SymbolSnapshot struct {
	Exchange         string
	MarketType       string
	Symbol           string
	LastFinalMS      int64
	LastFinalStartMS int64
	Continuous       bool
}

type WindowSnapshot struct {
	HTTPRequests5m    int
	HTTP5xx5m         int
	HTTPP95           time.Duration
	WSDrops5m         int
	GuardianEvents5m  map[string]int
	GuardianSymbols5m map[string]int
	StorageP95        time.Duration
	StorageFailures5m int
	GuardianTotals    map[string]uint64
	HTTPRequestsTotal uint64
	HTTP5xxTotal      uint64
	WSDropsTotal      uint64
	Integrity         IntegritySummary
}

type Snapshot struct {
	StartedAt time.Time
	Runtimes  []RuntimeSnapshot
	Symbols   []SymbolSnapshot
	Window    WindowSnapshot
	Resources ResourceSnapshot
}

type Registry struct {
	mu             sync.Mutex
	now            func() time.Time
	startedAt      time.Time
	runtimes       map[runtimeKey]*runtimeState
	symbols        map[string]*symbolState
	guardianEvents []timedGuardianEvent
	httpRequests   []httpObservation
	storageOps     []storageObservation
	wsDrops        []time.Time
	guardianTotals map[string]uint64
	httpTotal      uint64
	http5xxTotal   uint64
	wsDropsTotal   uint64
	integrity      IntegritySummary
	resources      ResourceSnapshot
	prometheus     *prometheus.Registry

	collectorConnected       *prometheus.GaugeVec
	collectorLastMessage     *prometheus.GaugeVec
	collectorLastBar         *prometheus.GaugeVec
	collectorReconnects      *prometheus.CounterVec
	collectorParseErrors     *prometheus.CounterVec
	collectorIngestErrors    *prometheus.CounterVec
	klineReceived            *prometheus.CounterVec
	klineLastPersisted       *prometheus.GaugeVec
	klineLastPublished       *prometheus.GaugeVec
	guardianEventTotal       *prometheus.CounterVec
	guardianQueueDrops       *prometheus.CounterVec
	guardianAuditTotal       *prometheus.CounterVec
	httpRequestTotal         *prometheus.CounterVec
	httpRequestDuration      *prometheus.HistogramVec
	wsConnections            prometheus.Gauge
	wsEventsSent             prometheus.Counter
	wsEventsDropped          prometheus.Counter
	storageOperationTotal    *prometheus.CounterVec
	storageOperationDuration *prometheus.HistogramVec
	mysqlPoolConnections     *prometheus.GaugeVec
	integrityIssues          *prometheus.GaugeVec
	activeAlerts             *prometheus.GaugeVec
	resourceUsage            *prometheus.GaugeVec
}

func NewRegistry() *Registry {
	return newRegistry(time.Now)
}

func newRegistry(now func() time.Time) *Registry {
	promRegistry := prometheus.NewRegistry()
	r := &Registry{
		now:                      now,
		startedAt:                now(),
		runtimes:                 make(map[runtimeKey]*runtimeState),
		symbols:                  make(map[string]*symbolState),
		guardianTotals:           make(map[string]uint64),
		prometheus:               promRegistry,
		collectorConnected:       prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "crypto_ticket_collector_connected", Help: "Current connected collector streams."}, []string{"exchange", "market_type"}),
		collectorLastMessage:     prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "crypto_ticket_collector_last_message_timestamp_seconds", Help: "Unix timestamp of the latest collector message."}, []string{"exchange", "market_type"}),
		collectorLastBar:         prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "crypto_ticket_collector_last_bar_timestamp_seconds", Help: "Unix timestamp of the latest parsed kline."}, []string{"exchange", "market_type"}),
		collectorReconnects:      prometheus.NewCounterVec(prometheus.CounterOpts{Name: "crypto_ticket_collector_reconnects_total", Help: "Collector reconnect attempts."}, []string{"exchange", "market_type"}),
		collectorParseErrors:     prometheus.NewCounterVec(prometheus.CounterOpts{Name: "crypto_ticket_collector_parse_errors_total", Help: "Collector kline parse errors."}, []string{"exchange", "market_type"}),
		collectorIngestErrors:    prometheus.NewCounterVec(prometheus.CounterOpts{Name: "crypto_ticket_collector_ingest_errors_total", Help: "Collector kline ingest errors."}, []string{"exchange", "market_type"}),
		klineReceived:            prometheus.NewCounterVec(prometheus.CounterOpts{Name: "crypto_ticket_kline_received_total", Help: "Parsed klines received from exchange streams."}, []string{"exchange", "market_type", "final"}),
		klineLastPersisted:       prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "crypto_ticket_kline_last_persisted_timestamp_seconds", Help: "Unix timestamp of the latest persisted final kline."}, []string{"exchange", "market_type", "timeframe"}),
		klineLastPublished:       prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "crypto_ticket_kline_last_published_timestamp_seconds", Help: "Unix timestamp of the latest published kline."}, []string{"exchange", "market_type", "timeframe"}),
		guardianEventTotal:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: "crypto_ticket_guardian_events_total", Help: "Kline guardian events."}, []string{"exchange", "event_type"}),
		guardianQueueDrops:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: "crypto_ticket_guardian_queue_dropped_total", Help: "Final bars dropped by a full guardian queue."}, []string{"exchange"}),
		guardianAuditTotal:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: "crypto_ticket_guardian_audits_total", Help: "Kline guardian audit runs."}, []string{"result"}),
		httpRequestTotal:         prometheus.NewCounterVec(prometheus.CounterOpts{Name: "crypto_ticket_http_requests_total", Help: "HTTP requests served."}, []string{"route", "method", "status_class"}),
		httpRequestDuration:      prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "crypto_ticket_http_request_duration_seconds", Help: "HTTP request latency.", Buckets: prometheus.DefBuckets}, []string{"route", "method"}),
		wsConnections:            prometheus.NewGauge(prometheus.GaugeOpts{Name: "crypto_ticket_ws_connections", Help: "Current public WebSocket connections."}),
		wsEventsSent:             prometheus.NewCounter(prometheus.CounterOpts{Name: "crypto_ticket_ws_events_sent_total", Help: "Events written to public WebSocket clients."}),
		wsEventsDropped:          prometheus.NewCounter(prometheus.CounterOpts{Name: "crypto_ticket_ws_events_dropped_total", Help: "Events dropped for slow public WebSocket clients."}),
		storageOperationTotal:    prometheus.NewCounterVec(prometheus.CounterOpts{Name: "crypto_ticket_storage_operations_total", Help: "Storage operations by result."}, []string{"operation", "result"}),
		storageOperationDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "crypto_ticket_storage_operation_duration_seconds", Help: "Storage operation latency.", Buckets: prometheus.DefBuckets}, []string{"operation"}),
		mysqlPoolConnections:     prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "crypto_ticket_mysql_pool_connections", Help: "Current database/sql MySQL connection pool state."}, []string{"state"}),
		integrityIssues:          prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "crypto_ticket_integrity_issues", Help: "Issues found by the latest high-timeframe audit."}, []string{"timeframe", "type"}),
		activeAlerts:             prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "crypto_ticket_active_alerts", Help: "Current active health alerts."}, []string{"severity"}),
		resourceUsage:            prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "crypto_ticket_resource_usage", Help: "Current process resource usage."}, []string{"resource"}),
	}
	promRegistry.MustRegister(
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		r.collectorConnected, r.collectorLastMessage, r.collectorLastBar, r.collectorReconnects,
		r.collectorParseErrors, r.collectorIngestErrors, r.klineReceived, r.klineLastPersisted,
		r.klineLastPublished, r.guardianEventTotal, r.guardianQueueDrops, r.guardianAuditTotal,
		r.httpRequestTotal, r.httpRequestDuration, r.wsConnections, r.wsEventsSent, r.wsEventsDropped,
		r.storageOperationTotal, r.storageOperationDuration, r.mysqlPoolConnections, r.integrityIssues, r.activeAlerts, r.resourceUsage,
	)
	return r
}

func (r *Registry) MetricsHandler() http.Handler {
	return promhttp.HandlerFor(r.prometheus, promhttp.HandlerOpts{})
}

func (r *Registry) StartedAt() time.Time {
	return r.startedAt
}

func (r *Registry) RegisterCollectorRuntime(exchangeName string, marketType string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.runtimeLocked(exchangeName, marketType)
}

func (r *Registry) CollectorConnection(exchangeName string, marketType string, delta int) {
	key := normalizeRuntimeKey(exchangeName, marketType)
	r.mu.Lock()
	state := r.runtimeLocked(key.Exchange, key.MarketType)
	state.Connected += delta
	if state.Connected < 0 {
		state.Connected = 0
	}
	connected := state.Connected
	r.mu.Unlock()
	r.collectorConnected.WithLabelValues(key.Exchange, key.MarketType).Set(float64(connected))
}

func (r *Registry) CollectorMessage(exchangeName string, marketType string) {
	key := normalizeRuntimeKey(exchangeName, marketType)
	now := r.now()
	r.mu.Lock()
	r.runtimeLocked(key.Exchange, key.MarketType).LastMessageAt = now
	r.mu.Unlock()
	r.collectorLastMessage.WithLabelValues(key.Exchange, key.MarketType).Set(float64(now.Unix()))
}

func (r *Registry) CollectorBarReceived(exchangeName string, marketType string, bar market.Bar) {
	key := normalizeRuntimeKey(exchangeName, marketType)
	now := r.now()
	r.mu.Lock()
	state := r.runtimeLocked(key.Exchange, key.MarketType)
	state.LastBarAt = now
	state.ReceivedTotal++
	if bar.IsFinal && bar.Timeframe == "1m" {
		state.LastFinalAt = now
		state.LastFinalStartMS = bar.StartMS
		r.symbols[symbolKey(bar.Exchange, bar.Symbol)] = &symbolState{Runtime: key, LastFinalAt: now, LastFinalStartMS: bar.StartMS, Continuous: r.symbolContinuousLocked(bar.Exchange, bar.Symbol)}
	} else {
		entry := r.symbols[symbolKey(bar.Exchange, bar.Symbol)]
		if entry == nil {
			r.symbols[symbolKey(bar.Exchange, bar.Symbol)] = &symbolState{Runtime: key}
		} else {
			entry.Runtime = key
		}
	}
	r.mu.Unlock()
	r.collectorLastBar.WithLabelValues(key.Exchange, key.MarketType).Set(float64(now.Unix()))
	r.klineReceived.WithLabelValues(key.Exchange, key.MarketType, boolLabel(bar.IsFinal)).Inc()
}

func (r *Registry) CollectorIngested(exchangeName string, marketType string, bar market.Bar) {
	if !bar.IsFinal || bar.Timeframe != "1m" {
		return
	}
	key := normalizeRuntimeKey(exchangeName, marketType)
	now := r.now()
	r.mu.Lock()
	state := r.runtimeLocked(key.Exchange, key.MarketType)
	state.LastPersistedAt = now
	state.FinalIngestedTotal++
	r.mu.Unlock()
	r.klineLastPersisted.WithLabelValues(key.Exchange, key.MarketType, "1m").Set(float64(now.Unix()))
}

func (r *Registry) CollectorReconnect(exchangeName string, marketType string) {
	key := normalizeRuntimeKey(exchangeName, marketType)
	now := r.now()
	r.mu.Lock()
	state := r.runtimeLocked(key.Exchange, key.MarketType)
	state.Reconnects = append(state.Reconnects, now)
	r.mu.Unlock()
	r.collectorReconnects.WithLabelValues(key.Exchange, key.MarketType).Inc()
}

func (r *Registry) CollectorParseError(exchangeName string, marketType string) {
	r.collectorError(exchangeName, marketType, true)
}

func (r *Registry) CollectorIngestError(exchangeName string, marketType string) {
	r.collectorError(exchangeName, marketType, false)
}

func (r *Registry) collectorError(exchangeName string, marketType string, parse bool) {
	key := normalizeRuntimeKey(exchangeName, marketType)
	now := r.now()
	r.mu.Lock()
	state := r.runtimeLocked(key.Exchange, key.MarketType)
	if parse {
		state.ParseErrors = append(state.ParseErrors, now)
	} else {
		state.IngestErrors = append(state.IngestErrors, now)
	}
	r.mu.Unlock()
	if parse {
		r.collectorParseErrors.WithLabelValues(key.Exchange, key.MarketType).Inc()
	} else {
		r.collectorIngestErrors.WithLabelValues(key.Exchange, key.MarketType).Inc()
	}
}

func (r *Registry) BarPersisted(bar market.Bar) {
	key := r.runtimeForBar(bar)
	now := r.now()
	if bar.IsFinal && bar.Timeframe == "1m" {
		r.mu.Lock()
		r.runtimeLocked(key.Exchange, key.MarketType).LastPersistedAt = now
		r.mu.Unlock()
	}
	r.klineLastPersisted.WithLabelValues(key.Exchange, key.MarketType, bar.Timeframe).Set(float64(now.Unix()))
}

func (r *Registry) BarPublished(bar market.Bar) {
	key := r.runtimeForBar(bar)
	now := r.now()
	if bar.IsFinal && bar.Timeframe == "1m" {
		r.mu.Lock()
		r.runtimeLocked(key.Exchange, key.MarketType).LastPublishedAt = now
		r.mu.Unlock()
	}
	r.klineLastPublished.WithLabelValues(key.Exchange, key.MarketType, bar.Timeframe).Set(float64(now.Unix()))
}

func (r *Registry) GuardianQueueDropped(exchangeName string) {
	now := r.now()
	r.mu.Lock()
	r.guardianEvents = append(r.guardianEvents, timedGuardianEvent{At: now, Event: "queue_dropped"})
	r.guardianTotals["queue_dropped"]++
	r.mu.Unlock()
	r.guardianQueueDrops.WithLabelValues(strings.ToLower(exchangeName)).Inc()
}

func (r *Registry) GuardianEvent(event market.KlineGuardianEvent) {
	now := r.now()
	r.mu.Lock()
	r.guardianEvents = append(r.guardianEvents, timedGuardianEvent{At: now, Event: event.EventType, Symbol: event.Symbol})
	r.guardianTotals[event.EventType]++
	r.mu.Unlock()
	r.guardianEventTotal.WithLabelValues(strings.ToLower(event.Exchange), event.EventType).Inc()
}

func (r *Registry) GuardianAudit(success bool) {
	result := "success"
	if !success {
		result = "failed"
	}
	r.guardianAuditTotal.WithLabelValues(result).Inc()
}

func (r *Registry) HTTPRequest(route string, method string, statusCode int, duration time.Duration) {
	now := r.now()
	r.mu.Lock()
	r.httpRequests = append(r.httpRequests, httpObservation{At: now, StatusCode: statusCode, Duration: duration})
	r.httpTotal++
	if statusCode >= 500 {
		r.http5xxTotal++
	}
	r.mu.Unlock()
	r.httpRequestTotal.WithLabelValues(route, method, statusClass(statusCode)).Inc()
	r.httpRequestDuration.WithLabelValues(route, method).Observe(duration.Seconds())
}

func (r *Registry) WSConnection(delta int) {
	if delta > 0 {
		r.wsConnections.Add(float64(delta))
	} else if delta < 0 {
		r.wsConnections.Sub(float64(-delta))
	}
}

func (r *Registry) WSEventSent() {
	r.wsEventsSent.Inc()
}

func (r *Registry) WSEventDropped() {
	now := r.now()
	r.mu.Lock()
	r.wsDrops = append(r.wsDrops, now)
	r.wsDropsTotal++
	r.mu.Unlock()
	r.wsEventsDropped.Inc()
}

func (r *Registry) StorageOperation(operation string, duration time.Duration, err error) {
	now := r.now()
	failed := err != nil
	r.mu.Lock()
	r.storageOps = append(r.storageOps, storageObservation{At: now, Operation: operation, Duration: duration, Failed: failed})
	r.mu.Unlock()
	result := "success"
	if failed {
		result = "failed"
	}
	r.storageOperationTotal.WithLabelValues(operation, result).Inc()
	r.storageOperationDuration.WithLabelValues(operation).Observe(duration.Seconds())
}

func (r *Registry) SetMySQLPoolStats(stats sql.DBStats) {
	r.mysqlPoolConnections.WithLabelValues("max_open").Set(float64(stats.MaxOpenConnections))
	r.mysqlPoolConnections.WithLabelValues("open").Set(float64(stats.OpenConnections))
	r.mysqlPoolConnections.WithLabelValues("in_use").Set(float64(stats.InUse))
	r.mysqlPoolConnections.WithLabelValues("idle").Set(float64(stats.Idle))
}

func (r *Registry) SetIntegrity(summary IntegritySummary) {
	r.mu.Lock()
	r.integrity = summary
	r.mu.Unlock()
	for tf, values := range summary.ByTimeframe {
		r.integrityIssues.WithLabelValues(tf, "missing").Set(float64(values[0]))
		r.integrityIssues.WithLabelValues(tf, "mismatch").Set(float64(values[1]))
		r.integrityIssues.WithLabelValues(tf, "invalid_ohlc").Set(float64(values[2]))
	}
}

func (r *Registry) SetContinuousSeries(series []market.MarketSeries) {
	continuous := make(map[string]bool, len(series))
	for _, item := range series {
		continuous[symbolKey(item.Exchange, item.Symbol)] = true
	}
	r.mu.Lock()
	for key, state := range r.symbols {
		state.Continuous = continuous[key]
	}
	for key := range continuous {
		if r.symbols[key] == nil {
			r.symbols[key] = &symbolState{Continuous: true}
		}
	}
	r.mu.Unlock()
}

func (r *Registry) SetActiveAlerts(warning int, critical int) {
	r.activeAlerts.WithLabelValues("warning").Set(float64(warning))
	r.activeAlerts.WithLabelValues("critical").Set(float64(critical))
}

func (r *Registry) SetResources(resources ResourceSnapshot) {
	r.mu.Lock()
	r.resources = resources
	r.mu.Unlock()
	r.resourceUsage.WithLabelValues("cpu_ratio").Set(resources.CPURatio)
	r.resourceUsage.WithLabelValues("rss_bytes").Set(float64(resources.RSSBytes))
	r.resourceUsage.WithLabelValues("memory_ratio").Set(resources.MemoryRatio)
	r.resourceUsage.WithLabelValues("open_fds").Set(float64(resources.OpenFDs))
	r.resourceUsage.WithLabelValues("fd_ratio").Set(resources.FDRatio)
	r.resourceUsage.WithLabelValues("goroutines").Set(float64(resources.Goroutines))
	r.resourceUsage.WithLabelValues("disk_ratio").Set(resources.DiskRatio)
}

func (r *Registry) Snapshot(now time.Time) Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	cutoff := now.Add(-observationRetention)
	fiveMinutes := now.Add(-5 * time.Minute)
	tenMinutes := now.Add(-10 * time.Minute)
	for _, state := range r.runtimes {
		state.Reconnects = pruneTimes(state.Reconnects, cutoff)
		state.ParseErrors = pruneTimes(state.ParseErrors, cutoff)
		state.IngestErrors = pruneTimes(state.IngestErrors, cutoff)
	}
	r.guardianEvents = pruneGuardianEvents(r.guardianEvents, cutoff)
	r.httpRequests = pruneHTTP(r.httpRequests, cutoff)
	r.storageOps = pruneStorage(r.storageOps, cutoff)
	r.wsDrops = pruneTimes(r.wsDrops, cutoff)

	snapshot := Snapshot{StartedAt: r.startedAt}
	for key, state := range r.runtimes {
		snapshot.Runtimes = append(snapshot.Runtimes, RuntimeSnapshot{
			Exchange: key.Exchange, MarketType: key.MarketType, Connected: state.Connected,
			LastMessageMS: timeMS(state.LastMessageAt), LastBarMS: timeMS(state.LastBarAt), LastFinalMS: timeMS(state.LastFinalAt),
			LastFinalStartMS: state.LastFinalStartMS, LastPersistedMS: timeMS(state.LastPersistedAt), LastPublishedMS: timeMS(state.LastPublishedAt),
			Reconnects5m: countTimesSince(state.Reconnects, fiveMinutes), Reconnects10m: countTimesSince(state.Reconnects, tenMinutes),
			ParseErrors5m: countTimesSince(state.ParseErrors, fiveMinutes), IngestErrors5m: countTimesSince(state.IngestErrors, fiveMinutes),
			ReceivedTotal: state.ReceivedTotal, FinalIngestedTotal: state.FinalIngestedTotal,
		})
	}
	for key, state := range r.symbols {
		exchangeName, symbol := splitSymbolKey(key)
		snapshot.Symbols = append(snapshot.Symbols, SymbolSnapshot{
			Exchange: exchangeName, MarketType: state.Runtime.MarketType, Symbol: symbol,
			LastFinalMS: timeMS(state.LastFinalAt), LastFinalStartMS: state.LastFinalStartMS, Continuous: state.Continuous,
		})
	}
	sort.Slice(snapshot.Runtimes, func(i, j int) bool {
		if snapshot.Runtimes[i].Exchange != snapshot.Runtimes[j].Exchange {
			return snapshot.Runtimes[i].Exchange < snapshot.Runtimes[j].Exchange
		}
		return snapshot.Runtimes[i].MarketType < snapshot.Runtimes[j].MarketType
	})
	sort.Slice(snapshot.Symbols, func(i, j int) bool {
		if snapshot.Symbols[i].Exchange != snapshot.Symbols[j].Exchange {
			return snapshot.Symbols[i].Exchange < snapshot.Symbols[j].Exchange
		}
		return snapshot.Symbols[i].Symbol < snapshot.Symbols[j].Symbol
	})

	window := WindowSnapshot{
		GuardianEvents5m:  make(map[string]int),
		GuardianSymbols5m: make(map[string]int),
		GuardianTotals:    cloneUintMap(r.guardianTotals),
		HTTPRequestsTotal: r.httpTotal, HTTP5xxTotal: r.http5xxTotal, WSDropsTotal: r.wsDropsTotal,
		Integrity: r.integrity,
	}
	var httpDurations []time.Duration
	for _, item := range r.httpRequests {
		if item.At.Before(fiveMinutes) {
			continue
		}
		window.HTTPRequests5m++
		if item.StatusCode >= 500 {
			window.HTTP5xx5m++
		}
		httpDurations = append(httpDurations, item.Duration)
	}
	window.HTTPP95 = percentile95(httpDurations)
	window.WSDrops5m = countTimesSince(r.wsDrops, fiveMinutes)
	for _, event := range r.guardianEvents {
		if event.At.Before(fiveMinutes) {
			continue
		}
		window.GuardianEvents5m[event.Event]++
		if event.Symbol != "" {
			window.GuardianSymbols5m[event.Symbol]++
		}
	}
	var storageDurations []time.Duration
	for _, item := range r.storageOps {
		if item.At.Before(fiveMinutes) {
			continue
		}
		storageDurations = append(storageDurations, item.Duration)
		if item.Failed {
			window.StorageFailures5m++
		}
	}
	window.StorageP95 = percentile95(storageDurations)
	snapshot.Window = window
	snapshot.Resources = r.resources
	return snapshot
}

func (r *Registry) runtimeLocked(exchangeName string, marketType string) *runtimeState {
	key := normalizeRuntimeKey(exchangeName, marketType)
	state := r.runtimes[key]
	if state == nil {
		state = &runtimeState{}
		r.runtimes[key] = state
	}
	return state
}

func (r *Registry) runtimeForBar(bar market.Bar) runtimeKey {
	r.mu.Lock()
	defer r.mu.Unlock()
	if state := r.symbols[symbolKey(bar.Exchange, bar.Symbol)]; state != nil && state.Runtime.Exchange != "" {
		return state.Runtime
	}
	return normalizeRuntimeKey(bar.Exchange, "unknown")
}

func (r *Registry) symbolContinuousLocked(exchangeName string, symbol string) bool {
	if state := r.symbols[symbolKey(exchangeName, symbol)]; state != nil {
		return state.Continuous
	}
	return false
}

func normalizeRuntimeKey(exchangeName string, marketType string) runtimeKey {
	return runtimeKey{Exchange: strings.ToLower(strings.TrimSpace(exchangeName)), MarketType: strings.ToLower(strings.TrimSpace(marketType))}
}

func symbolKey(exchangeName string, symbol string) string {
	return strings.ToLower(strings.TrimSpace(exchangeName)) + "\x00" + strings.ToUpper(strings.TrimSpace(symbol))
}

func splitSymbolKey(key string) (string, string) {
	parts := strings.SplitN(key, "\x00", 2)
	if len(parts) != 2 {
		return "", key
	}
	return parts[0], parts[1]
}

func boolLabel(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func statusClass(status int) string {
	if status <= 0 {
		return "0xx"
	}
	return string(rune('0'+status/100)) + "xx"
}

func timeMS(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}
	return value.UnixMilli()
}

func pruneTimes(values []time.Time, cutoff time.Time) []time.Time {
	index := 0
	for index < len(values) && values[index].Before(cutoff) {
		index++
	}
	return append([]time.Time(nil), values[index:]...)
}

func pruneGuardianEvents(values []timedGuardianEvent, cutoff time.Time) []timedGuardianEvent {
	index := 0
	for index < len(values) && values[index].At.Before(cutoff) {
		index++
	}
	return append([]timedGuardianEvent(nil), values[index:]...)
}

func pruneHTTP(values []httpObservation, cutoff time.Time) []httpObservation {
	index := 0
	for index < len(values) && values[index].At.Before(cutoff) {
		index++
	}
	return append([]httpObservation(nil), values[index:]...)
}

func pruneStorage(values []storageObservation, cutoff time.Time) []storageObservation {
	index := 0
	for index < len(values) && values[index].At.Before(cutoff) {
		index++
	}
	return append([]storageObservation(nil), values[index:]...)
}

func countTimesSince(values []time.Time, cutoff time.Time) int {
	count := 0
	for _, value := range values {
		if !value.Before(cutoff) {
			count++
		}
	}
	return count
}

func percentile95(values []time.Duration) time.Duration {
	if len(values) == 0 {
		return 0
	}
	ordered := append([]time.Duration(nil), values...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	index := (len(ordered)*95 + 99) / 100
	if index <= 0 {
		index = 1
	}
	return ordered[index-1]
}

func cloneUintMap(values map[string]uint64) map[string]uint64 {
	out := make(map[string]uint64, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}
