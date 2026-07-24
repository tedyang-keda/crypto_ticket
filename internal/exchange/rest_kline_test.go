package exchange

import (
	"context"
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

func TestOKXFetchKlinesUsesBaseAndQuoteVolumeFields(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/api/v5/market/candles" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		if r.URL.Query().Get("instId") != "BTC-USDT-SWAP" || r.URL.Query().Get("bar") != "1H" {
			t.Fatalf("unexpected query %s", r.URL.RawQuery)
		}
		if got := r.URL.Query().Get("adjust"); got != "forward" {
			t.Fatalf("expected adjust=forward, got %q", got)
		}
		return jsonResponse(`{"code":"0","data":[["3600000","100.0","110.0","90.0","105.0","123.45","1.234","129.570","1"]]}`), nil
	})}

	adapter := NewOKXAdapter("SWAP", "https://okx.test", "wss://example")
	bars, err := adapter.FetchKlines(context.Background(), client, KlineRequest{
		Symbol:          "BTC-USDT-SWAP",
		Timeframe:       "1H",
		Limit:           1,
		ForwardAdjusted: true,
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

func TestOKXFetchKlinesLooksPastEndForStableConfirmation(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if got := r.URL.Query().Get("after"); got != "420000" {
			t.Fatalf("expected one complete lookahead bucket, got after=%s", got)
		}
		return jsonResponse(`{"code":"0","data":[["360000","11","11","11","11","0","0","0","0"],["300000","10","10","10","10","0","0","0","1"],["240000","9","9","9","9","1","1","9","1"]]}`), nil
	})}

	adapter := NewOKXAdapter("SWAP", "https://okx.test", "wss://example")
	bars, err := adapter.FetchKlines(context.Background(), client, KlineRequest{
		Symbol: "TEST-USDT-SWAP", Timeframe: "1m", StartMS: 240_000, EndMS: 300_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(bars) != 2 || bars[0].StartMS != 240_000 || bars[1].StartMS != 300_000 {
		t.Fatalf("unexpected stable historical window: %+v", bars)
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
