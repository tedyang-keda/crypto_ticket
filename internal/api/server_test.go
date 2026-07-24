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

func TestKlinesPriceMode(t *testing.T) {
	hub := realtime.NewHub()
	server := NewServer(app.NewMarketService(storage.NewMemoryHistoricalStore(), hub, []string{"1m"}, 300), hub, ".")
	tests := []struct {
		name       string
		priceMode  string
		wantStatus int
	}{
		{name: "omitted defaults to raw", wantStatus: http.StatusOK},
		{name: "raw", priceMode: "raw", wantStatus: http.StatusOK},
		{name: "legacy backward adjusted", priceMode: "backward_adjusted", wantStatus: http.StatusBadRequest},
		{name: "legacy forward adjusted", priceMode: "forward_adjusted", wantStatus: http.StatusBadRequest},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			url := "/api/v1/klines?exchange=binance&symbol=BTCUSDT&timeframe=1m"
			if tc.priceMode != "" {
				url += "&price_mode=" + tc.priceMode
			}
			req := httptest.NewRequest(http.MethodGet, url, nil)
			rec := httptest.NewRecorder()

			server.Handler().ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("expected %d, got %d body=%s", tc.wantStatus, rec.Code, rec.Body.String())
			}
			if tc.wantStatus == http.StatusBadRequest && !strings.Contains(rec.Body.String(), "unsupported price mode") {
				t.Fatalf("unexpected body: %s", rec.Body.String())
			}
		})
	}
}
