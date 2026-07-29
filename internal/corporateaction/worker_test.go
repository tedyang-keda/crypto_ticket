package corporateaction

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"crypto-ticket/internal/exchange"
	"crypto-ticket/internal/market"
	"crypto-ticket/internal/storage"
)

func TestSnapFactor(t *testing.T) {
	tests := []struct {
		name  string
		ratio float64
		want  float64
	}{
		{name: "koru 20 to 1", ratio: 0.05, want: 0.05},
		{name: "crwd 4 to 1", ratio: 0.25, want: 0.25},
		{name: "reverse split", ratio: 4, want: 4},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := SnapFactor(tc.ratio)
			if !ok || got != tc.want {
				t.Fatalf("SnapFactor(%v) = %v, %t; want %v, true", tc.ratio, got, ok, tc.want)
			}
		})
	}
	if _, ok := SnapFactor(1.35); ok {
		t.Fatal("ordinary price jump should not be classified as corporate action")
	}
}

func TestWorkerConfirmsAndProcessesJob(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryHistoricalStore()
	now := time.Now().UnixMilli()
	base := now - 60*time.Minute.Milliseconds()
	if err := store.UpsertSymbols(ctx, []market.SymbolInfo{{
		Exchange: "okx", Symbol: "KORU-USDT-SWAP", MarketType: "SWAP", IsActive: true,
	}}); err != nil {
		t.Fatal(err)
	}
	old := testBar("okx", "KORU-USDT-SWAP", base, 100, 105, 95, 100)
	current := testBar("okx", "KORU-USDT-SWAP", base+time.Minute.Milliseconds(), 5, 5.2, 4.8, 5)
	if err := store.UpsertBars(ctx, []market.Bar{old, current}); err != nil {
		t.Fatal(err)
	}
	fetcher := &fakeFetcher{name: "okx", marketType: "SWAP", bars: []market.Bar{old, current}, forwardFactor: 0.05, effectiveMS: current.StartMS}
	worker := New(store, []Fetcher{fetcher}, nil, nil, Config{Enabled: true, Timeframes: []string{"1m"}, PollInterval: time.Hour})
	if err := worker.handleFinalBar(ctx, old); err != nil {
		t.Fatal(err)
	}
	if err := worker.handleFinalBar(ctx, current); err != nil {
		t.Fatal(err)
	}
	job, err := store.LoadCorporateActionJob(ctx, "okx", "KORU-USDT-SWAP", current.StartMS)
	if err != nil || job == nil {
		t.Fatalf("expected confirmed job, job=%+v err=%v", job, err)
	}
	if job.Factor != 0.05 || job.Status != statusPending {
		t.Fatalf("unexpected job: %+v", job)
	}
	if err := worker.ProcessDueJobs(ctx); err != nil {
		t.Fatal(err)
	}
	job, err = store.LoadCorporateActionJob(ctx, "okx", "KORU-USDT-SWAP", current.StartMS)
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != statusCompleted || job.RowsWritten != 2 || job.VerificationStatus != "passed" {
		t.Fatalf("unexpected completed job: %+v", job)
	}
	rows, err := store.BarsInRange(ctx, "okx", "KORU-USDT-SWAP", "1m", old.StartMS, current.StartMS)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].ClosePrice != 5 || rows[1].ClosePrice != 5 {
		t.Fatalf("expected forward-adjusted rows, got %+v", rows)
	}
}

func TestWorkerRetriesFailedJob(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryHistoricalStore()
	now := time.Now().UnixMilli()
	job := market.CorporateActionJob{
		Exchange: "okx", Symbol: "KORU-USDT-SWAP", MarketType: "SWAP", EffectiveMS: now,
		Factor: 0.05, Status: statusPending,
	}
	if err := store.InsertCorporateActionJob(ctx, job); err != nil {
		t.Fatal(err)
	}
	worker := New(store, []Fetcher{&fakeFetcher{name: "okx", marketType: "SWAP", err: context.DeadlineExceeded}}, nil, nil, Config{
		Enabled: true, Timeframes: []string{"1m"}, MaxAttempts: 3, RetryBaseDelay: time.Second,
	})
	if err := worker.ProcessDueJobs(ctx); err == nil {
		t.Fatal("expected backfill error")
	}
	updated, err := store.LoadCorporateActionJob(ctx, "okx", job.Symbol, job.EffectiveMS)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Status != statusRetry || updated.Attempts != 1 || updated.NextRetryMS <= now {
		t.Fatalf("expected retry state, got %+v", updated)
	}
}

func TestFeishuNotifierSendsInteractiveCard(t *testing.T) {
	var got map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Errorf("decode request: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	notifier := NewFeishuNotifier(server.URL, server.Client())
	err := notifier.Notify(context.Background(), Notification{
		Stage: "completed", Title: "验证报告",
		Job: market.CorporateActionJob{Exchange: "okx", Symbol: "KORU-USDT-SWAP", EffectiveMS: time.Now().UnixMilli(), Factor: 0.05, Status: statusCompleted},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got["msg_type"] != "interactive" {
		t.Fatalf("unexpected feishu payload: %+v", got)
	}
}

type fakeFetcher struct {
	name          string
	marketType    string
	bars          []market.Bar
	err           error
	forwardFactor float64
	effectiveMS   int64
}

func (f *fakeFetcher) Name() string       { return f.name }
func (f *fakeFetcher) MarketType() string { return f.marketType }
func (f *fakeFetcher) FetchKlines(_ context.Context, _ *http.Client, request exchange.KlineRequest) ([]market.Bar, error) {
	if f.err != nil {
		return nil, f.err
	}
	var out []market.Bar
	for _, bar := range f.bars {
		if bar.Symbol != request.Symbol || bar.Timeframe != request.Timeframe {
			continue
		}
		if request.StartMS > 0 && bar.StartMS < request.StartMS {
			continue
		}
		if request.EndMS > 0 && bar.StartMS > request.EndMS {
			continue
		}
		if request.Adjustment == exchange.KlineAdjustmentForward && f.forwardFactor > 0 && bar.StartMS < f.effectiveMS {
			bar.OpenPrice *= f.forwardFactor
			bar.HighPrice *= f.forwardFactor
			bar.LowPrice *= f.forwardFactor
			bar.ClosePrice *= f.forwardFactor
			bar.Volume /= f.forwardFactor
		}
		out = append(out, bar)
	}
	return out, nil
}

func testBar(exchangeName string, symbol string, startMS int64, open float64, high float64, low float64, closePrice float64) market.Bar {
	return market.DecorateBar(market.Bar{
		Exchange: exchangeName, Symbol: symbol, Timeframe: "1m", StartMS: startMS, EndMS: startMS + 59_999,
		OpenPrice: open, HighPrice: high, LowPrice: low, ClosePrice: closePrice,
		Volume: 10, QuoteVolume: 100, IsFinal: true, Source: "test", Reason: "test", UpdatedAtMS: time.Now().UnixMilli(),
	})
}

func TestEscapeMarkdown(t *testing.T) {
	if strings.Contains(escapeMarkdown("a`b\nc"), "`") {
		t.Fatal("markdown escape should remove backticks")
	}
}
