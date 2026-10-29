package corporateaction

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"crypto-ticket/internal/exchange"
	"crypto-ticket/internal/market"
	"crypto-ticket/internal/storage"
	"crypto-ticket/internal/timeframe"
)

type policyFetcher struct {
	name              string
	requests          []exchange.KlineRequest
	unsupportedTarget bool
	sources           []market.Bar
}

func (f *policyFetcher) Name() string     { return f.name }
func (*policyFetcher) MarketType() string { return "test" }
func (f *policyFetcher) FetchKlines(_ context.Context, _ *http.Client, request exchange.KlineRequest) ([]market.Bar, error) {
	f.requests = append(f.requests, request)
	if f.unsupportedTarget && request.Timeframe != "1D" {
		return nil, exchange.ErrUnsupportedKlineInterval
	}
	return f.sources, nil
}

type policyStore struct {
	*storage.MemoryHistoricalStore
	writes  int
	deletes int
}

func (s *policyStore) UpsertBars(ctx context.Context, bars []market.Bar) error {
	s.writes++
	return s.MemoryHistoricalStore.UpsertBars(ctx, bars)
}

func (s *policyStore) DeleteBarsInRange(ctx context.Context, ex, symbol, tf string, start, end int64) (int64, error) {
	s.deletes++
	return s.MemoryHistoricalStore.DeleteBarsInRange(ctx, ex, symbol, tf, start, end)
}

func TestCorporateActionConfigurationsNeverBackfillThreeDay(t *testing.T) {
	for _, frames := range [][]string{nil, {"3D"}, {"1D", "3D", "3M"}} {
		for _, ex := range []string{"binance", "okx"} {
			store := &policyStore{MemoryHistoricalStore: storage.NewMemoryHistoricalStore()}
			fetcher := &policyFetcher{name: ex}
			worker := New(store, []Fetcher{fetcher}, nil, nil, Config{Timeframes: frames})
			for _, tf := range worker.cfg.Timeframes {
				if tf == "3D" {
					t.Fatalf("retired interval survived configuration: %v", worker.cfg.Timeframes)
				}
			}
			if _, _, err := worker.backfill(context.Background(), market.CorporateActionJob{Exchange: ex, Symbol: "TEST", MarketType: "test"}); err != nil {
				t.Fatal(err)
			}
			if store.writes != 0 || store.deletes != 0 {
				t.Fatalf("empty REST result unexpectedly changed storage: writes=%d deletes=%d", store.writes, store.deletes)
			}
			for _, request := range fetcher.requests {
				if request.Timeframe == "3D" {
					t.Fatalf("retired interval fetched by configured worker: %+v", request)
				}
			}
			if len(frames) == 1 && len(fetcher.requests) != 0 {
				t.Fatalf("retired-only configuration issued requests: %v", fetcher.requests)
			}
		}
	}
}

func TestCorporateActionExplicitThreeDayCannotFetchFallbackDeleteOrWrite(t *testing.T) {
	for _, ex := range []string{"binance", "okx"} {
		fetcher := &policyFetcher{name: ex, unsupportedTarget: true}
		store := &policyStore{MemoryHistoricalStore: storage.NewMemoryHistoricalStore()}
		worker := New(store, []Fetcher{fetcher}, nil, nil, Config{Timeframes: []string{"1D"}})
		// Defend the write workflow too, even if a caller bypassed normalization.
		worker.cfg.Timeframes = []string{"3D"}
		_, _, err := worker.backfill(context.Background(), market.CorporateActionJob{Exchange: ex, Symbol: "TEST", MarketType: "test"})
		if !errors.Is(err, timeframe.ErrDisabledTimeframe) || len(fetcher.requests) != 0 || store.writes != 0 || store.deletes != 0 {
			t.Fatalf("explicit retired backfill touched REST/storage: err=%v requests=%v writes=%d deletes=%d", err, fetcher.requests, store.writes, store.deletes)
		}
		_, _, err = fetchWithRollup(context.Background(), nil, fetcher, exchange.KlineRequest{Symbol: "TEST", Timeframe: "3D"}, nil)
		if !errors.Is(err, timeframe.ErrDisabledTimeframe) || len(fetcher.requests) != 0 {
			t.Fatalf("retired fetch called official REST or fallback: err=%v requests=%v", err, fetcher.requests)
		}
		if bars := deriveBars(exchange.KlineRequest{Timeframe: "3D"}, nil); len(bars) != 0 {
			t.Fatalf("retired interval was derived: %+v", bars)
		}
	}
}

func TestCorporateActionOtherDerivedIntervalsStillWork(t *testing.T) {
	start := timeframe.FloorStartMS(time.Now().AddDate(0, 0, -10).UnixMilli(), "2D")
	var sources []market.Bar
	for index := int64(0); index < 2; index++ {
		day := start + index*timeframe.DayMS
		sources = append(sources, market.Bar{Exchange: "binance", Symbol: "TEST", Timeframe: "1D", StartMS: day, EndMS: timeframe.EndMS(day, "1D"), OpenPrice: 100, HighPrice: 110, LowPrice: 90, ClosePrice: 105, IsFinal: true})
	}
	fetcher := &policyFetcher{name: "binance", unsupportedTarget: true, sources: sources}
	bars, strategy, err := fetchWithRollup(context.Background(), nil, fetcher, exchange.KlineRequest{Symbol: "TEST", Timeframe: "2D", StartMS: start, EndMS: timeframe.EndMS(start, "2D")}, nil)
	if err != nil || strategy != "rollup:1D" || len(bars) != 1 || bars[0].Timeframe != "2D" || len(fetcher.requests) != 2 {
		t.Fatalf("supported derived interval stopped working: bars=%v strategy=%s err=%v requests=%v", bars, strategy, err, fetcher.requests)
	}
}
