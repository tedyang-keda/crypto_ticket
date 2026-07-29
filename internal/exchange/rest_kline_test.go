package exchange

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestBinanceFetchKlinesUsesOfficialVolumeFields(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/fapi/v1/klines" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		if r.URL.Query().Get("symbol") != "BTCUSDT" || r.URL.Query().Get("interval") != "1h" {
			t.Fatalf("unexpected query %s", r.URL.RawQuery)
		}
		return jsonResponse(`[[3600000,"100.0","110.0","90.0","105.0","1.234",7199999,"129.570",7,"0","0","0"]]`), nil
	})}

	adapter := NewBinanceFuturesAdapter("um_futures", "https://binance.test", "wss://example")
	bars, err := adapter.FetchKlines(context.Background(), client, KlineRequest{
		Symbol:    "BTCUSDT",
		Timeframe: "1H",
		Limit:     1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(bars) != 1 {
		t.Fatalf("expected one bar, got %d", len(bars))
	}
	bar := bars[0]
	if bar.Exchange != "binance" || bar.Symbol != "BTCUSDT" || bar.Timeframe != "1H" {
		t.Fatalf("unexpected identity: %+v", bar)
	}
	if bar.StartMS != 3600000 || bar.EndMS != 7199999 || !bar.IsFinal || bar.Source != "rest" {
		t.Fatalf("unexpected metadata: %+v", bar)
	}
	assertFloatEqual(t, bar.Volume, 1.234)
	assertFloatEqual(t, bar.QuoteVolume, 129.570)
	if bar.TradeCount != 7 {
		t.Fatalf("expected trade count 7, got %d", bar.TradeCount)
	}
}

func TestBinanceFetchContinuousKlinesUsesTradFiEndpoint(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/fapi/v1/continuousKlines" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		query := r.URL.Query()
		if query.Get("pair") != "KORUUSDT" || query.Get("contractType") != "TRADIFI_PERPETUAL" || query.Get("interval") != "1d" {
			t.Fatalf("unexpected query %s", r.URL.RawQuery)
		}
		return jsonResponse(`[[3600000,"100.0","110.0","90.0","105.0","1.234",7199999,"129.570",7,"0","0","0"]]`), nil
	})}

	adapter := NewBinanceFuturesAdapter("um_futures", "https://binance.test", "wss://example")
	bars, err := adapter.FetchContinuousKlines(context.Background(), client, KlineRequest{
		Symbol: "KORUUSDT", Timeframe: "1D", Limit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(bars) != 1 || bars[0].Source != "rest" {
		t.Fatalf("unexpected continuous bars: %+v", bars)
	}
}

func TestOKXFetchKlinesUsesBaseAndQuoteVolumeFields(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/api/v5/market/candles" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		if r.URL.Query().Get("instId") != "BTC-USDT-SWAP" || r.URL.Query().Get("bar") != "1H" {
			t.Fatalf("unexpected query %s", r.URL.RawQuery)
		}
		return jsonResponse(`{"code":"0","data":[["3600000","100.0","110.0","90.0","105.0","123.45","1.234","129.570","1"]]}`), nil
	})}

	adapter := NewOKXAdapter("SWAP", "https://okx.test", "wss://example")
	bars, err := adapter.FetchKlines(context.Background(), client, KlineRequest{
		Symbol:    "BTC-USDT-SWAP",
		Timeframe: "1H",
		Limit:     1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(bars) != 1 {
		t.Fatalf("expected one bar, got %d", len(bars))
	}
	bar := bars[0]
	if bar.Exchange != "okx" || bar.Symbol != "BTC-USDT-SWAP" || bar.Timeframe != "1H" {
		t.Fatalf("unexpected identity: %+v", bar)
	}
	assertFloatEqual(t, bar.Volume, 1.234)
	assertFloatEqual(t, bar.QuoteVolume, 129.570)
	if bar.TradeCount != 0 {
		t.Fatalf("okx REST candles do not provide trade count, got %d", bar.TradeCount)
	}
}

func TestOKXFetchKlinesAutoRequestsForwardAdjustment(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if got := r.URL.Query().Get("adjust"); got != "forward" {
			t.Fatalf("expected adjust=forward, got %q", got)
		}
		return jsonResponse(`{"code":"0","data":[["3600000","5","6","4","5.5","10","20","100","1"]]}`), nil
	})}

	adapter := NewOKXAdapter("SWAP", "https://okx.test", "wss://example")
	bars, err := adapter.FetchKlines(context.Background(), client, KlineRequest{
		Symbol:     "TEST-USDT-SWAP",
		Timeframe:  "1H",
		Limit:      1,
		Adjustment: KlineAdjustmentAuto,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(bars) != 1 || bars[0].OpenPrice != 5 {
		t.Fatalf("unexpected bars: %+v", bars)
	}
}

func TestOKXFetchKlinesAutoFallsBackToRaw(t *testing.T) {
	requests := 0
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requests++
		if requests == 1 {
			if got := r.URL.Query().Get("adjust"); got != "forward" {
				t.Fatalf("expected first request to use forward adjustment, got %q", got)
			}
			return jsonResponse(`{"code":"51000","msg":"Parameter adjust error","data":[]}`), nil
		}
		if got := r.URL.Query().Get("adjust"); got != "" {
			t.Fatalf("expected raw fallback without adjust, got %q", got)
		}
		return jsonResponse(`{"code":"0","data":[["3600000","100","110","90","105","10","20","2000","1"]]}`), nil
	})}

	adapter := NewOKXAdapter("SWAP", "https://okx.test", "wss://example")
	bars, err := adapter.FetchKlines(context.Background(), client, KlineRequest{
		Symbol:     "TEST-USDT-SWAP",
		Timeframe:  "1H",
		Limit:      1,
		Adjustment: KlineAdjustmentAuto,
	})
	if err != nil {
		t.Fatal(err)
	}
	if requests != 2 || len(bars) != 1 || bars[0].OpenPrice != 100 {
		t.Fatalf("unexpected fallback result requests=%d bars=%+v", requests, bars)
	}
}

func TestOKXFetchKlinesForwardDoesNotSilentlyFallback(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return jsonResponse(`{"code":"51000","msg":"Parameter adjust error","data":[]}`), nil
	})}

	adapter := NewOKXAdapter("SWAP", "https://okx.test", "wss://example")
	_, err := adapter.FetchKlines(context.Background(), client, KlineRequest{
		Symbol:     "TEST-USDT-SWAP",
		Timeframe:  "1H",
		Limit:      1,
		Adjustment: KlineAdjustmentForward,
	})
	if !errors.Is(err, ErrUnsupportedKlineAdjustment) {
		t.Fatalf("expected unsupported adjustment error, got %v", err)
	}
}

func TestKlineRequestRetriesRateLimit(t *testing.T) {
	requests := 0
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requests++
		if requests == 1 {
			return &http.Response{
				StatusCode: http.StatusTooManyRequests,
				Status:     "429 Too Many Requests",
				Body:       io.NopCloser(strings.NewReader(`{"code":"50011"}`)),
			}, nil
		}
		return jsonResponse(`{"code":"0","data":[["3600000","100","110","90","105","10","20","2000","1"]]}`), nil
	})}

	adapter := NewOKXAdapter("SWAP", "https://okx.test", "wss://example")
	_, err := adapter.FetchKlines(context.Background(), client, KlineRequest{
		Symbol: "TEST-USDT-SWAP", Timeframe: "1H", Limit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if requests != 2 {
		t.Fatalf("expected one retry, got %d requests", requests)
	}
}

func TestOKXFetchKlinesUsesUTCSessionIntervals(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if got := r.URL.Query().Get("bar"); got != "1Dutc" {
			t.Fatalf("expected 1Dutc, got %s", got)
		}
		return jsonResponse(`{"code":"0","data":[]}`), nil
	})}

	adapter := NewOKXAdapter("SWAP", "https://okx.test", "wss://example")
	_, err := adapter.FetchKlines(context.Background(), client, KlineRequest{
		Symbol:    "BTC-USDT-SWAP",
		Timeframe: "1D",
		Limit:     1,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestOKXFetchKlinesUsesHistoryEndpointForExplicitStart(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/api/v5/market/history-candles" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		return jsonResponse(`{"code":"0","data":[]}`), nil
	})}

	adapter := NewOKXAdapter("SWAP", "https://okx.test", "wss://example")
	_, err := adapter.FetchKlines(context.Background(), client, KlineRequest{
		Symbol:    "BTC-USDT-SWAP",
		Timeframe: "1H",
		StartMS:   3600000,
		Limit:     1,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestOKXFetchKlinesContinuesAfterShortHistoryPage(t *testing.T) {
	requests := 0
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requests++
		switch requests {
		case 1:
			return jsonResponse(`{"code":"0","data":[["240000","10","10","10","10","1","1","10","1"],["180000","10","10","10","10","1","1","10","1"]]}`), nil
		case 2:
			if got := r.URL.Query().Get("after"); got != "180000" {
				t.Fatalf("unexpected second-page cursor %s", got)
			}
			return jsonResponse(`{"code":"0","data":[["120000","10","10","10","10","1","1","10","1"],["60000","10","10","10","10","1","1","10","1"]]}`), nil
		default:
			t.Fatalf("unexpected request %d", requests)
			return nil, nil
		}
	})}

	adapter := NewOKXAdapter("SWAP", "https://okx.test", "wss://example")
	bars, err := adapter.FetchKlines(context.Background(), client, KlineRequest{
		Symbol: "TEST-USDT-SWAP", Timeframe: "1m", StartMS: 60_000, EndMS: 300_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if requests != 2 || len(bars) != 4 {
		t.Fatalf("short history page stopped pagination requests=%d bars=%d", requests, len(bars))
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func jsonResponse(body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}
