package monitoring

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"crypto-ticket/internal/notify"
)

func TestAlertEngineDeduplicatesEscalatesAndRecovers(t *testing.T) {
	var mu sync.Mutex
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		requests++
		mu.Unlock()
		_, _ = w.Write([]byte(`{"code":0}`))
	}))
	defer server.Close()
	now := time.Date(2026, 7, 28, 1, 0, 0, 0, time.UTC)
	registry := newRegistry(func() time.Time { return now })
	engine := newAlertEngine(notify.NewFeishuClient(server.URL, server.Client()), registry, true, func() time.Time { return now })
	ctx := context.Background()

	engine.Evaluate(ctx, []Condition{{Key: "collector", Title: "Collector", Severity: SeverityWarning, Active: true, Message: "stale"}})
	engine.Evaluate(ctx, []Condition{{Key: "collector", Title: "Collector", Severity: SeverityWarning, Active: true, Message: "stale"}})
	engine.Evaluate(ctx, []Condition{{Key: "collector", Title: "Collector", Severity: SeverityCritical, Active: true, Message: "very stale"}})
	engine.Evaluate(ctx, []Condition{{Key: "collector", Active: false}})
	engine.Evaluate(ctx, []Condition{{Key: "collector", Active: false}})
	mu.Lock()
	got := requests
	mu.Unlock()
	if got != 3 {
		t.Fatalf("expected open, escalation and recovery notifications, got %d", got)
	}
}

func TestAlertEngineP1ShadowDoesNotNotify(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		_, _ = w.Write([]byte(`{"code":0}`))
	}))
	defer server.Close()
	now := time.Date(2026, 7, 28, 1, 0, 0, 0, time.UTC)
	registry := newRegistry(func() time.Time { return now })
	engine := newAlertEngine(notify.NewFeishuClient(server.URL, server.Client()), registry, false, func() time.Time { return now })
	engine.Evaluate(context.Background(), []Condition{{Key: "quality", Title: "Quality", Severity: SeverityWarning, Active: true, P1: true}})
	engine.Evaluate(context.Background(), []Condition{{Key: "quality", Title: "Quality", Severity: SeverityWarning, Active: true, P1: true}})
	if requests != 0 || engine.WouldFire()["quality"] != 1 {
		t.Fatalf("unexpected shadow behavior requests=%d would_fire=%v", requests, engine.WouldFire())
	}
}
