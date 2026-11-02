package exchange

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"crypto-ticket/internal/timeframe"
)

func TestBinanceThreeDayKlinesDisabledBeforeHTTP(t *testing.T) {
	for _, marketType := range []string{"um_futures", "coin_futures", "spot"} {
		for _, continuous := range []bool{false, true} {
			t.Run(marketType+"/continuous="+fmt.Sprint(continuous), func(t *testing.T) {
				requests := 0
				client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
					requests++
					return jsonResponse(`[]`), nil
				})}
				adapter := NewBinanceFuturesAdapter(marketType, "https://binance.test", "wss://example")
				request := KlineRequest{Symbol: "BTCUSDT", Timeframe: "3D", Limit: 1}
				var err error
				if continuous {
					_, err = adapter.FetchContinuousKlines(context.Background(), client, request)
				} else {
					_, err = adapter.FetchKlines(context.Background(), client, request)
				}
				if !errors.Is(err, timeframe.ErrDisabledTimeframe) || !errors.Is(err, ErrUnsupportedKlineInterval) {
					t.Fatalf("expected disabled interval error, got %v", err)
				}
				if requests != 0 {
					t.Fatalf("disabled interval issued %d HTTP requests", requests)
				}
			})
		}
	}
}

func TestOKXThreeDayKlinesDisabledBeforeHTTP(t *testing.T) {
	requests := 0
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		requests++
		return jsonResponse(`{"code":"0","data":[]}`), nil
	})}
	adapter := NewOKXAdapter("SWAP", "https://okx.test", "wss://example")
	_, err := adapter.FetchKlines(context.Background(), client, KlineRequest{Symbol: "BTC-USDT-SWAP", Timeframe: "3D", Limit: 1})
	if !errors.Is(err, timeframe.ErrDisabledTimeframe) || !errors.Is(err, ErrUnsupportedKlineInterval) {
		t.Fatalf("expected disabled interval error, got %v", err)
	}
	if requests != 0 {
		t.Fatalf("disabled interval issued %d HTTP requests", requests)
	}
}

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

func TestBinanceFetchKlinesRejectsInvalidActivity(t *testing.T) {
	invalid := []any{"", "not-a-number", "NaN", "+Inf", "-Inf", "-1", "1e9999", "1e-9999", "-1e-9999", "0x0p0", "0x1p-9999", "1_000", nil, true, map[string]any{}, []any{}}
	for _, field := range []struct {
		index int
		name  string
	}{{5, "volume"}, {7, "quote_volume"}, {8, "trade_count"}} {
		values := append([]any(nil), invalid...)
		if field.index == 8 {
			values = append(values, "1.5", json.Number("1.5"), "9223372036854775808", json.Number("9223372036854775808"))
		}
		for _, value := range values {
			t.Run(fmt.Sprintf("%s/%T/%v", field.name, value, value), func(t *testing.T) {
				row := []any{3600000, "100", "110", "90", "105", "0.000", 7199999, "0.000", 0}
				row[field.index] = value
				payload, err := json.Marshal([][]any{row})
				if err != nil {
					t.Fatal(err)
				}
				client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
					return jsonResponse(string(payload)), nil
				})}
				adapter := NewBinanceFuturesAdapter("um_futures", "https://binance.test", "wss://example")
				bars, err := adapter.FetchKlines(context.Background(), client, KlineRequest{Symbol: "TESTUSDT", Timeframe: "1H", Limit: 1})
				if err == nil || len(bars) != 0 {
					t.Fatalf("invalid activity was treated as a zero-activity candle: bars=%+v err=%v", bars, err)
				}
				for _, detail := range []string{"binance", "TESTUSDT", "1H", field.name} {
					if !strings.Contains(err.Error(), detail) {
						t.Fatalf("error lacks %q context: %v", detail, err)
					}
				}
			})
		}
	}
}

func TestBinanceFetchKlinesAcceptsExactZeroActivityAndInt64TradeCount(t *testing.T) {
	for _, count := range []string{"0", "9223372036854775807"} {
		t.Run(count, func(t *testing.T) {
			client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return jsonResponse(`[[3600000,"100","110","90","105","0.00000000",7199999,"0.0",` + count + `]]`), nil
			})}
			adapter := NewBinanceFuturesAdapter("um_futures", "https://binance.test", "wss://example")
			bars, err := adapter.FetchKlines(context.Background(), client, KlineRequest{Symbol: "TESTUSDT", Timeframe: "1H", Limit: 1})
			if err != nil || len(bars) != 1 || bars[0].Volume != 0 || bars[0].QuoteVolume != 0 {
				t.Fatalf("valid decimal zero rejected: bars=%+v err=%v", bars, err)
			}
			if fmt.Sprint(bars[0].TradeCount) != count {
				t.Fatalf("trade count lost integer precision: %+v", bars[0])
			}
		})
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

func TestOKXFetchKlinesRejectsInvalidActivity(t *testing.T) {
	for _, field := range []struct {
		index int
		name  string
	}{{5, "contract_volume"}, {6, "volume"}, {7, "quote_volume"}} {
		for _, value := range []any{"", "not-a-number", "NaN", "+Inf", "-Inf", "-1", "1e9999", "1e-9999", "-1e-9999", "0x0p0", "0x1p-9999", "1_000", nil, true, map[string]any{}, []any{}, 0} {
			t.Run(fmt.Sprintf("%s/%T/%v", field.name, value, value), func(t *testing.T) {
				row := []any{"3600000", "100", "110", "90", "105", "0.000", "0.000", "0.000", "1"}
				row[field.index] = value
				payload, err := json.Marshal(map[string]any{"code": "0", "data": [][]any{row}})
				if err != nil {
					t.Fatal(err)
				}
				client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
					return jsonResponse(string(payload)), nil
				})}
				adapter := NewOKXAdapter("SWAP", "https://okx.test", "wss://example")
				bars, err := adapter.FetchKlines(context.Background(), client, KlineRequest{Symbol: "TEST-USDT-SWAP", Timeframe: "1H", Limit: 1})
				if err == nil || len(bars) != 0 {
					t.Fatalf("invalid activity was treated as a zero-activity candle: bars=%+v err=%v", bars, err)
				}
				for _, detail := range []string{"okx", "TEST-USDT-SWAP", "1H", field.name} {
					if !strings.Contains(err.Error(), detail) {
						t.Fatalf("error lacks %q context: %v", detail, err)
					}
				}
			})
		}
	}
}

func TestOKXFetchKlinesAcceptsDecimalZeroActivity(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return jsonResponse(`{"code":"0","data":[["3600000","100","110","90","105","0.00000000","0.0","0","1"]]}`), nil
	})}
	adapter := NewOKXAdapter("SWAP", "https://okx.test", "wss://example")
	bars, err := adapter.FetchKlines(context.Background(), client, KlineRequest{Symbol: "TEST-USDT-SWAP", Timeframe: "1H", Limit: 1})
	if err != nil || len(bars) != 1 || bars[0].Volume != 0 || bars[0].QuoteVolume != 0 || bars[0].ContractVolume != 0 {
		t.Fatalf("valid decimal zero rejected: bars=%+v err=%v", bars, err)
	}
}

func TestParseKlineActivityFloatPreservesExactZeroAndRejectsUnderflow(t *testing.T) {
	for _, value := range []string{"0", "0.000", "0e-9999", "0e+9999", "-0.000E-9999", " 0.000e-12345 "} {
		t.Run("zero/"+value, func(t *testing.T) {
			got, err := parseKlineActivityFloat("okx", "TEST-USDT-SWAP", "1H", "volume", value)
			if err != nil || got != 0 {
				t.Fatalf("exact decimal zero rejected: value=%q got=%v err=%v", value, got, err)
			}
		})
	}
	for _, value := range []any{"1e-9999", "-1e-9999", "1.25E-9999", "-0.00001e-9999", "1e-324", "0x0p-9999", "0x1p-9999", json.Number("1e-9999")} {
		t.Run(fmt.Sprintf("invalid/%T/%v", value, value), func(t *testing.T) {
			if got, err := parseKlineActivityFloat("okx", "TEST-USDT-SWAP", "1H", "volume", value); err == nil {
				t.Fatalf("non-decimal or nonzero underflow was accepted: value=%v got=%v", value, got)
			}
		})
	}
	if got, err := parseKlineActivityFloat("okx", "TEST-USDT-SWAP", "1H", "volume", "5e-324"); err != nil || got <= 0 {
		t.Fatalf("representable nonzero subnormal rejected: got=%v err=%v", got, err)
	}
}

func TestRESTKlinesStillIgnorePendingCandlesBeforeActivityValidation(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if strings.HasPrefix(r.URL.Path, "/fapi/") {
			return jsonResponse(fmt.Sprintf(`[[3600000,"100","110","90","105","bad",%d,"bad","bad"]]`, time.Now().Add(time.Hour).UnixMilli())), nil
		}
		return jsonResponse(`{"code":"0","data":[["3600000","100","110","90","105","bad","bad","bad","0"]]}`), nil
	})}
	binance := NewBinanceFuturesAdapter("um_futures", "https://binance.test", "wss://example")
	bars, err := binance.FetchKlines(context.Background(), client, KlineRequest{Symbol: "TESTUSDT", Timeframe: "1H", Limit: 1})
	if err != nil || len(bars) != 0 {
		t.Fatalf("pending Binance candle validation changed: bars=%+v err=%v", bars, err)
	}
	okx := NewOKXAdapter("SWAP", "https://okx.test", "wss://example")
	bars, err = okx.FetchKlines(context.Background(), client, KlineRequest{Symbol: "TEST-USDT-SWAP", Timeframe: "1H", Limit: 1})
	if err != nil || len(bars) != 0 {
		t.Fatalf("pending OKX candle validation changed: bars=%+v err=%v", bars, err)
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

func TestOKXFetchKlinesLimitCountsOnlyRequestedWindow(t *testing.T) {
	requests := 0
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requests++
		switch requests {
		case 1:
			return jsonResponse(`{"code":"0","data":[["240000","10","10","10","10","1","1","10","1"],["180000","10","10","10","10","1","1","10","1"]]}`), nil
		case 2:
			if got := r.URL.Query().Get("limit"); got != "1" {
				t.Fatalf("expected one remaining requested bar, got limit=%s", got)
			}
			return jsonResponse(`{"code":"0","data":[["120000","10","10","10","10","1","1","10","1"]]}`), nil
		default:
			t.Fatalf("unexpected request %d", requests)
			return nil, nil
		}
	})}
	adapter := NewOKXAdapter("SWAP", "https://okx.test", "wss://example")
	bars, err := adapter.FetchKlines(context.Background(), client, KlineRequest{
		Symbol: "TEST-USDT-SWAP", Timeframe: "1m", StartMS: 120_000, EndMS: 180_000, Limit: 2,
	})
	if err != nil || len(bars) != 2 || requests != 2 {
		t.Fatalf("end-filtered row consumed requested limit: bars=%v requests=%d err=%v", bars, requests, err)
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
