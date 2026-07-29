package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"crypto-ticket/internal/app"
	"crypto-ticket/internal/monitoring"
	"crypto-ticket/internal/realtime"
	"crypto-ticket/internal/storage"
)

type apiTestPinger struct {
	err error
}

func (p *apiTestPinger) Ping(context.Context) error { return p.err }

func TestHealthReadinessAndMetricsEndpoints(t *testing.T) {
	store := storage.NewMemoryHistoricalStore()
	registry := monitoring.NewRegistry()
	pinger := &apiTestPinger{}
	monitor := monitoring.NewService(registry, pinger, nil, "", monitoring.Config{
		Enabled: true,
	})
	hub := realtime.NewHub(registry)
	service := app.NewMarketService(store, hub, []string{"1m"}, 300)
	handler := NewServer(service, hub, "", monitor).Handler()

	health := httptest.NewRecorder()
	handler.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if health.Code != http.StatusOK {
		t.Fatalf("healthz status = %d, want 200", health.Code)
	}
	var healthBody map[string]any
	if err := json.Unmarshal(health.Body.Bytes(), &healthBody); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"started_at_ms", "uptime_seconds"} {
		if _, ok := healthBody[field]; !ok {
			t.Fatalf("healthz missing %q: %s", field, health.Body.String())
		}
	}

	ready := httptest.NewRecorder()
	handler.ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if ready.Code != http.StatusOK || !strings.Contains(ready.Body.String(), `"status":"disabled"`) {
		t.Fatalf("unexpected readyz response status=%d body=%s", ready.Code, ready.Body.String())
	}

	pinger.err = errors.New("mysql unavailable")
	failedReady := httptest.NewRecorder()
	handler.ServeHTTP(failedReady, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if failedReady.Code != http.StatusServiceUnavailable || !strings.Contains(failedReady.Body.String(), "mysql unavailable") {
		t.Fatalf("expected 503 with db error, status=%d body=%s", failedReady.Code, failedReady.Body.String())
	}

	metrics := httptest.NewRecorder()
	handler.ServeHTTP(metrics, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if metrics.Code != http.StatusOK || !strings.Contains(metrics.Body.String(), "crypto_ticket_http_requests_total") {
		t.Fatalf("unexpected metrics response status=%d body=%s", metrics.Code, metrics.Body.String())
	}
}
