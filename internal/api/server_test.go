package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"crypto-ticket/internal/app"
	"crypto-ticket/internal/realtime"
	"crypto-ticket/internal/storage"
)

func TestKlinesRejectsUnsupportedPriceMode(t *testing.T) {
	hub := realtime.NewHub()
	server := NewServer(app.NewMarketService(storage.NewMemoryHistoricalStore(), hub, []string{"1m"}, 300), hub, ".")
	req := httptest.NewRequest(http.MethodGet, "/api/v1/klines?exchange=binance&symbol=BTCUSDT&timeframe=1m&price_mode=split_magic", nil)
	rec := httptest.NewRecorder()

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "unsupported price mode") {
		t.Fatalf("unexpected body: %s", rec.Body.String())
	}
}
