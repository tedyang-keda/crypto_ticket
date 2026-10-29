package exchange

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

func snapshotHTTPClient(payload string) *http.Client {
	return &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Status: "200 OK", Body: io.NopCloser(strings.NewReader(payload))}, nil
	})}
}

func TestOfficialSymbolMetadataPreservesTimesAndLocalFilters(t *testing.T) {
	binance := NewBinanceFuturesAdapter("um_futures", "https://binance.test", "")
	items, err := binance.FetchSymbols(context.Background(), snapshotHTTPClient(`{"symbols":[{"symbol":"BTCUSDT","status":"TRADING","contractType":"PERPETUAL","baseAsset":"BTC","quoteAsset":"USDT","marginAsset":"USDT","onboardDate":1569398400000,"deliveryDate":4133404800000,"extraOfficialField":"retained"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || !items[0].OfficialActive || !items[0].IsActive || items[0].ListedAtMS != 1569398400000 || items[0].ExpiryAtMS != 0 || items[0].BaseCurrency != "BTC" || !strings.Contains(string(items[0].OfficialRaw), "extraOfficialField") {
		t.Fatalf("incorrect Binance metadata: %+v", items)
	}
	okx := NewOKXAdapter("SWAP", "https://okx.test", "")
	items, err = okx.FetchSymbols(context.Background(), snapshotHTTPClient(`{"code":"0","data":[{"instId":"BTC-USD-SWAP","state":"live","ctType":"inverse","settleCcy":"BTC","listTime":"1569398400000","expTime":"","custom":"kept"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || !items[0].OfficialActive || items[0].IsActive || items[0].ListedAtMS != 1569398400000 || items[0].SettleCurrency != "BTC" || !strings.Contains(string(items[0].OfficialRaw), "custom") {
		t.Fatalf("incorrect OKX metadata: %+v", items)
	}
}

func TestOfficialSymbolMetadataRejectsErrorEmptyAndIncompleteResponses(t *testing.T) {
	okx := NewOKXAdapter("SWAP", "https://okx.test", "")
	for _, payload := range []string{`{"code":"51001","data":[{"instId":"BTC-USDT-SWAP","state":"live"}]}`, `{"code":"0","data":[]}`, `{"data":[{"instId":"BTC-USDT-SWAP","state":"live"}]}`, `{"code":"0","data":[{"instId":"BTC-USDT-SWAP"}]}`} {
		if _, err := okx.FetchSymbols(context.Background(), snapshotHTTPClient(payload)); err == nil {
			t.Fatalf("accepted unsafe OKX payload %s", payload)
		}
	}
	binance := NewBinanceFuturesAdapter("um_futures", "https://binance.test", "")
	for _, payload := range []string{`{"code":-1003,"msg":"rate limited"}`, `{"symbols":[]}`, `{"symbols":[{"symbol":"BTCUSDT"}]}`} {
		if _, err := binance.FetchSymbols(context.Background(), snapshotHTTPClient(payload)); err == nil {
			t.Fatalf("accepted unsafe Binance payload %s", payload)
		}
	}
}
