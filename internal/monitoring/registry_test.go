package monitoring

import (
	"context"
	"database/sql"
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
