package exchange

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestParseAnnouncedRatio(t *testing.T) {
	cases := []struct {
		text string
		want float64
		ok   bool
	}{
		{"Position quantity × 12.52", 12.52, true},
		{"position quantity ×20 and mark price ÷20", 20, true},
		{"20-for-1 stock split effective July 15", 20, true},
		{"1-for-5 reverse split (consolidation)", 0.2, true},
		{"adjusted at a ratio of 12.52", 12.52, true},
		{"Adjustment Scale Factor: 20", 20, true},
		{"20:1 split takes effect", 20, true},
		{"commence at approximately 00:30 (UTC) with no ratio", 0, false},
		{"OKX to Adjust MUU Equity Perpetual Futures Due to Corporate Action", 0, false},
	}
	for _, tc := range cases {
		got, ok := ParseAnnouncedRatio(tc.text)
		if ok != tc.ok {
			t.Fatalf("%q: ok=%v want %v (got=%v)", tc.text, ok, tc.ok, got)
		}
		if ok && math.Abs(got-tc.want) > 1e-9 {
			t.Fatalf("%q: ratio=%f want %f", tc.text, got, tc.want)
		}
	}
}

func TestBinanceAnnouncementVerifierUsesScaleFactor(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/bapi/composite/v1/public/cms/article/catalog/list/query":
			if r.URL.Query().Get("catalogId") != binanceDerivativesCatalogID {
				t.Fatalf("unexpected catalog: %s", r.URL.RawQuery)
			}
			_, _ = w.Write([]byte(`{"data":{"articles":[{"code":"koru-complete","title":"Binance Futures Has Completed KORUUSDT Contract Size Adjustment"},{"code":"koru-split","title":"Binance Futures Will Adjust KORUUSDT Contract Size"}]}}`))
		case "/bapi/composite/v1/public/cms/article/detail/query":
			if r.URL.Query().Get("articleCode") == "koru-complete" {
				_, _ = w.Write([]byte(`{"data":{"article":{"code":"koru-complete","title":"KORUUSDT adjustment completed","body":"{\"node\":\"root\",\"child\":[{\"node\":\"text\",\"text\":\"Trading resumed\"}]}"}}}`))
				return
			}
			_, _ = w.Write([]byte(`{"data":{"article":{"code":"koru-split","title":"KORUUSDT Contract Size Adjustment","body":"{\"node\":\"root\",\"child\":[{\"node\":\"element\",\"child\":[{\"node\":\"text\",\"text\":\"Adjustment Scale Factor\"}]},{\"node\":\"element\",\"child\":[{\"node\":\"text\",\"text\":\"20\"}]}]}"}}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	verifier := NewBinanceAnnouncementVerifier(server.URL, server.Client())
	found, ratio, hasRatio := verifier.VerifyCorporateAction(context.Background(), "binance", "binance:um_futures", "KORUUSDT", 0)
	if !found || !hasRatio || math.Abs(ratio-20) > 1e-9 {
		t.Fatalf("unexpected verification found=%v ratio=%f hasRatio=%v", found, ratio, hasRatio)
	}
}

func TestBinanceCMSBodyTextPreservesTableOrder(t *testing.T) {
	body := `{"node":"root","child":[{"node":"element","child":[{"node":"text","text":"Adjustment Scale Factor"}]},{"node":"element","child":[{"node":"text","text":"20"}]}]}`
	text := binanceCMSBodyText(body)
	if ratio, ok := ParseAnnouncedRatio(text); !ok || ratio != 20 {
		t.Fatalf("structured CMS text should expose scale factor, text=%q ratio=%f ok=%v", text, ratio, ok)
	}
}

func TestCollectBinanceCMSArticleReadsPublishDate(t *testing.T) {
	articles := collectBinanceCMSArticles(map[string]any{"data": map[string]any{
		"title": "KORUUSDT adjustment", "code": "code", "body": "body", "publishDate": float64(12345),
	}})
	if len(articles) != 1 || articles[0].ReleaseDate != 12345 {
		t.Fatalf("publishDate was not parsed: %+v", articles)
	}
}

func TestListBinanceCorporateActionsParsesHistoricalEvidence(t *testing.T) {
	published := time.Date(2026, 7, 13, 8, 0, 0, 0, time.UTC).UnixMilli()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/bapi/composite/v1/public/cms/article/catalog/list/query":
			if r.URL.Query().Get("pageNo") == "1" {
				_, _ = w.Write([]byte(`{"data":{"articles":[{"code":"koru-adjust","title":"Binance Futures Will Adjust the Contract Size of KORUUSDT"}]}}`))
				return
			}
			_, _ = w.Write([]byte(`{"data":{"articles":[]}}`))
		case "/bapi/composite/v1/public/cms/article/detail/query":
			body := `{"node":"root","child":[{"node":"text","text":"20-for-1 forward share split"},{"node":"text","text":"Adjustment starts 2026-07-15 00:15 (UTC) and concludes 2026-07-15 13:30 (UTC)"}]}`
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{
				"code": "koru-adjust", "title": "KORUUSDT Contract Size Adjustment", "body": body, "publishDate": published,
			}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	verifier := NewBinanceAnnouncementVerifier(server.URL, server.Client())
	actions, err := verifier.ListCorporateActions(context.Background(), BinanceAnnouncementQuery{
		StartMS: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC).UnixMilli(),
		EndMS:   time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC).UnixMilli(),
		Symbols: []string{"KORUUSDT"}, MaxPages: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(actions) != 1 {
		t.Fatalf("expected one action, got %+v", actions)
	}
	action := actions[0]
	if action.Symbol != "KORUUSDT" || action.Ratio != 20 || action.AnnouncementCode != "koru-adjust" {
		t.Fatalf("unexpected action: %+v", action)
	}
	if action.WindowStartMS == 0 || action.WindowEndMS <= action.WindowStartMS || len(action.Raw) == 0 {
		t.Fatalf("historical evidence is incomplete: %+v", action)
	}
}

func TestBinanceAnnouncementVerifierFailsClosedOnMalformedCMS(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"unexpected":true}}`))
	}))
	defer server.Close()
	verifier := NewBinanceAnnouncementVerifier(server.URL, server.Client())
	found, ratio, hasRatio := verifier.VerifyCorporateAction(context.Background(), "binance", "", "KORUUSDT", 0)
	if found || ratio != 0 || hasRatio {
		t.Fatalf("malformed CMS must not produce evidence: %v %f %v", found, ratio, hasRatio)
	}
	if !strings.Contains(verifier.baseURL, server.Listener.Addr().String()) {
		t.Fatal("test verifier should use the configured CMS host")
	}
}

func TestAnnouncementMatchesSymbol(t *testing.T) {
	if !AnnouncementMatchesSymbol("MUU-USDT-SWAP", "OKX to Adjust MUU Equity Perpetual Futures") {
		t.Fatal("should match OKX base token MUU")
	}
	if !AnnouncementMatchesSymbol("MUUUSDT", "Binance adjusts MUU perpetual") {
		t.Fatal("should match Binance base token MUU")
	}
	if AnnouncementMatchesSymbol("MUU-USDT-SWAP", "OKX to Adjust KORU Equity Perpetual Futures") {
		t.Fatal("should not match a different token")
	}
	// Base token must be a whole word, not a substring of a larger token.
	if AnnouncementMatchesSymbol("MUU-USDT-SWAP", "MUUX rebase announcement") {
		t.Fatal("should not match MUU inside MUUX")
	}
}
