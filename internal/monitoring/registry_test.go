package monitoring

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"crypto-ticket/internal/market"
)

type fakePinger struct {
	err error
}

func (p *fakePinger) Ping(context.Context) error {
	return p.err
}

func TestRegistryMetricsAvoidSymbolLabels(t *testing.T) {
	now := time.Date(2026, 7, 28, 1, 0, 0, 0, time.UTC)
	registry := newRegistry(func() time.Time { return now })
	registry.RegisterCollectorRuntime("binance", "um_futures")
	registry.CollectorBarReceived("binance", "um_futures", market.Bar{
		Exchange: "binance", Symbol: "BTCUSDT", Timeframe: "1m", StartMS: now.Add(-time.Minute).UnixMilli(), IsFinal: true,
	})
	registry.SetMySQLPoolStats(sql.DBStats{MaxOpenConnections: 20, OpenConnections: 3, InUse: 2, Idle: 1})
	request := httptest.NewRequest("GET", "/metrics", nil)
	response := httptest.NewRecorder()
	registry.MetricsHandler().ServeHTTP(response, request)
	body, err := io.ReadAll(response.Result().Body)
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	if !strings.Contains(text, "crypto_ticket_kline_received_total") {
		t.Fatalf("expected kline metric, got %s", text)
	}
	if !strings.Contains(text, `crypto_ticket_mysql_pool_connections{state="in_use"} 2`) {
		t.Fatalf("expected MySQL pool metric, got %s", text)
	}
	if strings.Contains(text, "symbol=") || strings.Contains(text, "BTCUSDT") {
		t.Fatalf("metrics must not expose symbol labels: %s", text)
	}
}

func TestReadinessSeparatesCollectorAndDatabaseFailures(t *testing.T) {
	now := time.Date(2026, 7, 28, 1, 0, 0, 0, time.UTC)
	registry := newRegistry(func() time.Time { return now })
	pinger := &fakePinger{}
	service := NewService(registry, pinger, nil, "", Config{
		Enabled: true, CollectorEnabled: true, StartupGrace: time.Minute,
		WSWarningAge: 90 * time.Second, WSCriticalAge: 3 * time.Minute, FinalCriticalAge: 2 * time.Minute,
	})
	registry.RegisterCollectorRuntime("okx", "swap")
	registry.CollectorMessage("okx", "swap")
	bar := market.Bar{Exchange: "okx", Symbol: "BTC-USDT-SWAP", Timeframe: "1m", StartMS: now.Add(-time.Minute).UnixMilli(), IsFinal: true}
	registry.CollectorBarReceived("okx", "swap", bar)
	registry.CollectorIngested("okx", "swap", bar)
	registry.BarPublished(bar)
	report := service.Readiness(context.Background())
	if !report.OK {
		t.Fatalf("expected ready report: %+v", report)
	}

	pinger.err = errors.New("database down")
	report = service.Readiness(context.Background())
	if report.OK || report.Checks[0].Status != "unavailable" {
		t.Fatalf("expected database failure: %+v", report)
	}

	pinger.err = nil
	now = now.Add(4 * time.Minute)
	report = service.Readiness(context.Background())
	if report.OK {
		t.Fatalf("expected stale collector: %+v", report)
	}
	found := false
	for _, check := range report.Checks {
		if check.Status == "stale" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected stale collector check: %+v", report.Checks)
	}
}

func TestRegistryGuardianAndHTTPWindows(t *testing.T) {
	now := time.Date(2026, 7, 28, 1, 0, 0, 0, time.UTC)
	registry := newRegistry(func() time.Time { return now })
	registry.GuardianEvent(market.KlineGuardianEvent{Exchange: "okx", Symbol: "KORU-USDT-SWAP", EventType: "mismatch_repair"})
	registry.HTTPRequest("/api/v1/klines", "GET", 500, 2*time.Second)
	registry.WSEventDropped()
	snapshot := registry.Snapshot(now)
	if snapshot.Window.GuardianEvents5m["mismatch_repair"] != 1 || snapshot.Window.HTTP5xx5m != 1 || snapshot.Window.WSDrops5m != 1 {
		t.Fatalf("unexpected window: %+v", snapshot.Window)
	}
}

func TestCollectorSubscriptionsReplaceRuntimeSymbols(t *testing.T) {
	now := time.Date(2026, 7, 30, 6, 0, 0, 0, time.UTC)
	registry := newRegistry(func() time.Time { return now })
	registry.RegisterCollectorRuntime("binance", "um_futures")
	registry.CollectorSubscriptions("binance", "um_futures", []string{"BTCUSDT", "ETHUSDT"})
	snapshot := registry.Snapshot(now)
	if snapshot.Runtimes[0].SubscribedSymbols != 2 {
		t.Fatalf("unexpected subscription count: %+v", snapshot.Runtimes[0])
	}
	registry.CollectorSubscriptions("binance", "um_futures", []string{"BTCUSDT"})
	snapshot = registry.Snapshot(now)
	subscribed := make(map[string]bool)
	for _, symbol := range snapshot.Symbols {
		subscribed[symbol.Symbol] = symbol.Subscribed
	}
	if snapshot.Runtimes[0].SubscribedSymbols != 1 || !subscribed["BTCUSDT"] || subscribed["ETHUSDT"] {
		t.Fatalf("subscriptions were not replaced: runtime=%+v symbols=%+v", snapshot.Runtimes[0], snapshot.Symbols)
	}
}

func TestMarketQualityReportIncludesConnectionsDelayAndGuardianRepairs(t *testing.T) {
	now := time.Date(2026, 7, 30, 6, 0, 0, 0, time.UTC)
	registry := newRegistry(func() time.Time { return now })
	registry.RegisterCollectorRuntime("binance", "um_futures")
	registry.CollectorSubscriptions("binance", "um_futures", []string{"BTCUSDT", "ETHUSDT"})
	registry.SetContinuousSeries([]market.MarketSeries{
		{Exchange: "binance", Symbol: "BTCUSDT", MarketType: "um_futures"},
		{Exchange: "binance", Symbol: "ETHUSDT", MarketType: "um_futures"},
	})
	registry.CollectorConnection("binance", "um_futures", 1)
	registry.CollectorMessage("binance", "um_futures")
	registry.CollectorBarReceived("binance", "um_futures", market.Bar{
		Exchange: "binance", Symbol: "BTCUSDT", Timeframe: "1m", StartMS: now.Add(-time.Minute).UnixMilli(), IsFinal: true,
	})
	oldBar := market.Bar{OpenPrice: 100, HighPrice: 101, LowPrice: 99, ClosePrice: 100, Volume: 10}
	newBar := market.Bar{OpenPrice: 100, HighPrice: 102, LowPrice: 99, ClosePrice: 101, Volume: 12}
	oldJSON, _ := json.Marshal(oldBar)
	newJSON, _ := json.Marshal(newBar)
	registry.GuardianEvent(market.KlineGuardianEvent{
		Exchange: "binance", Symbol: "BTCUSDT", Timeframe: "1m", StartMS: now.Add(-10 * time.Minute).UnixMilli(),
		EventType: "mismatch_repair", OldValueJSON: string(oldJSON), NewValueJSON: string(newJSON), CreatedAtMS: now.UnixMilli(),
	})
	registry.GuardianEvent(market.KlineGuardianEvent{
		Exchange: "binance", Symbol: "ETHUSDT", Timeframe: "1m", StartMS: now.Add(-11 * time.Minute).UnixMilli(),
		EventType: "missing_repair", CreatedAtMS: now.UnixMilli(),
	})
	registry.GuardianAudit(true, 1188, 35000, 0, 95*time.Second)
	now = now.Add(3 * time.Minute)
	service := NewService(registry, &fakePinger{}, nil, "", Config{Enabled: true, CollectorEnabled: true})
	report := service.formatMarketQualityReport(now, registry.Snapshot(now))
	for _, expected := range []string{
		"subscribed=2, connections=1, 在线但无消息",
		"tracked=2, 2m~5m=1, >=5m=1",
		"checked_symbols=1188, checked_bars=35000, failed_symbols=0",
		"missing=1, mismatch=1, repair_failed=0, rest_error=0, affected=2",
		"价格/数量不一致，已修复",
		"缺失，已修复",
	} {
		if !strings.Contains(report, expected) {
			t.Fatalf("quality report missing %q:\n%s", expected, report)
		}
	}
}

func TestFormatIntegrityIncludesIssueDetailsAndLimit(t *testing.T) {
	issues := make([]IntegrityIssue, 0, 21)
	for i := 0; i < 21; i++ {
		issues = append(issues, IntegrityIssue{
			Exchange: "binance", Symbol: "BTCUSDT", Timeframe: "15m",
			StartMS: time.Date(2026, 7, 29, 12, 15+i, 0, 0, time.UTC).UnixMilli(), Type: "missing",
		})
	}
	message := formatIntegrity(IntegritySummary{
		CheckedSymbols: 50, Missing: 21, Affected: []string{"binance:BTCUSDT"}, Issues: issues,
	})
	for _, expected := range []string{
		"checked=50, missing=21, mismatch=0, invalid_ohlc=0, affected=1",
		"**异常明细**:",
		"- `binance:BTCUSDT`, `15m`, `2026-07-29T12:15:00Z`, `missing`",
		"- 其余 `1` 条异常已省略",
	} {
		if !strings.Contains(message, expected) {
			t.Fatalf("formatted integrity message missing %q: %s", expected, message)
		}
	}
	if strings.Contains(message, "2026-07-29T12:35:00Z") {
		t.Fatalf("formatted integrity message exceeded detail limit: %s", message)
	}
}

func TestRuntimeWatermarksOnlyUseFinalOneMinuteBars(t *testing.T) {
	now := time.Date(2026, 7, 28, 1, 0, 0, 0, time.UTC)
	registry := newRegistry(func() time.Time { return now })
	registry.RegisterCollectorRuntime("okx", "swap")
	registry.CollectorBarReceived("okx", "swap", market.Bar{Exchange: "okx", Symbol: "BTC-USDT-SWAP", Timeframe: "1m"})
	registry.BarPersisted(market.Bar{Exchange: "okx", Symbol: "BTC-USDT-SWAP", Timeframe: "5m", IsFinal: true})
	registry.BarPublished(market.Bar{Exchange: "okx", Symbol: "BTC-USDT-SWAP", Timeframe: "1m", IsFinal: false})
	snapshot := registry.Snapshot(now)
	if snapshot.Runtimes[0].LastPersistedMS != 0 || snapshot.Runtimes[0].LastPublishedMS != 0 {
		t.Fatalf("non-final or higher timeframe bars changed final 1m watermarks: %+v", snapshot.Runtimes[0])
	}
	bar := market.Bar{Exchange: "okx", Symbol: "BTC-USDT-SWAP", Timeframe: "1m", IsFinal: true}
	registry.BarPersisted(bar)
	registry.BarPublished(bar)
	snapshot = registry.Snapshot(now)
	if snapshot.Runtimes[0].LastPersistedMS != now.UnixMilli() || snapshot.Runtimes[0].LastPublishedMS != now.UnixMilli() {
		t.Fatalf("final 1m watermarks were not updated: %+v", snapshot.Runtimes[0])
	}
}

func TestReadinessDetectsPublishStall(t *testing.T) {
	now := time.Date(2026, 7, 28, 1, 0, 0, 0, time.UTC)
	registry := newRegistry(func() time.Time { return now })
	service := NewService(registry, &fakePinger{}, nil, "", Config{
		Enabled: true, CollectorEnabled: true, StartupGrace: time.Minute,
		WSWarningAge: 90 * time.Second, WSCriticalAge: 3 * time.Minute, FinalCriticalAge: 2 * time.Minute,
	})
	registry.RegisterCollectorRuntime("okx", "swap")
	bar := market.Bar{Exchange: "okx", Symbol: "BTC-USDT-SWAP", Timeframe: "1m", IsFinal: true}
	registry.CollectorBarReceived("okx", "swap", bar)
	registry.CollectorIngested("okx", "swap", bar)
	now = now.Add(3 * time.Minute)
	registry.CollectorBarReceived("okx", "swap", bar)
	registry.CollectorIngested("okx", "swap", bar)
	report := service.Readiness(context.Background())
	if report.OK || len(report.Checks) < 2 || report.Checks[1].Status != "publish_stale" {
		t.Fatalf("expected publish stall readiness failure: %+v", report)
	}
}
