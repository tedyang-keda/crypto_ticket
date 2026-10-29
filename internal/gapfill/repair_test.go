package gapfill

import (
	"context"
	"errors"
	"math"
	"net/http"
	"testing"
	"time"

	"crypto-ticket/internal/exchange"
	"crypto-ticket/internal/market"
	"crypto-ticket/internal/timeframe"
)

type repairStore struct {
	bars          map[int64]market.Bar
	finalOnly     bool
	reads         int
	writes        int
	written       []market.Bar
	readErr       error
	insertErr     error
	dropInsert    bool
	beforeInsert  func(*repairStore)
	failReadAfter int
}

func (s *repairStore) BarsInRange(_ context.Context, ex, symbol, tf string, start, end int64) ([]market.Bar, error) {
	s.reads++
	if s.readErr != nil && (s.failReadAfter == 0 || s.reads > s.failReadAfter) {
		return nil, s.readErr
	}
	var out []market.Bar
	for _, bar := range s.bars {
		if bar.Exchange == ex && bar.Symbol == symbol && bar.Timeframe == tf && bar.StartMS >= start && bar.StartMS <= end && (!s.finalOnly || bar.IsFinal) {
			out = append(out, bar)
		}
	}
	return out, nil
}

func (s *repairStore) InsertMissingBars(_ context.Context, bars []market.Bar) (int, error) {
	s.writes++
	s.written = append(s.written, bars...)
	if s.insertErr != nil {
		return 0, s.insertErr
	}
	if s.beforeInsert != nil {
		s.beforeInsert(s)
	}
	if s.dropInsert {
		return 0, nil
	}
	if s.bars == nil {
		s.bars = make(map[int64]market.Bar)
	}
	inserted := 0
	for _, bar := range bars {
		if _, found := s.bars[bar.StartMS]; found {
			continue
		}
		s.bars[bar.StartMS] = bar
		inserted++
	}
	return inserted, nil
}

type repairFetcher struct {
	bars     []market.Bar
	err      error
	calls    int
	requests []exchange.KlineRequest
}

func (*repairFetcher) Name() string { return "test" }

func (f *repairFetcher) FetchKlines(_ context.Context, _ *http.Client, request exchange.KlineRequest) ([]market.Bar, error) {
	f.calls++
	f.requests = append(f.requests, request)
	return f.bars, f.err
}

func testTask(ex, tf string, starts ...int64) Task {
	return Task{ID: "test-task", Exchange: ex, Symbol: "TEST", Timeframe: tf, SourceTimeframe: sourceTimeframe(ex, tf), StartMS: starts[0], EndMS: timeframe.EndMS(starts[len(starts)-1], tf), TargetStarts: starts}
}

func testBar(ex, tf string, start int64) market.Bar {
	return market.Bar{Exchange: ex, Symbol: "TEST", Timeframe: tf, StartMS: start, EndMS: timeframe.EndMS(start, tf), OpenPrice: 100, HighPrice: 110, LowPrice: 90, ClosePrice: 105, Volume: 1, QuoteVolume: 100, LastTickMS: timeframe.EndMS(start, tf), IsFinal: true}
}

func TestRepairNativeInsertsOnlyRequestedMissingKeys(t *testing.T) {
	start := testStart("1H")
	existing := testBar("binance", "1H", start)
	existing.ClosePrice = 101
	predecessor := testBar("binance", "1H", start-timeframe.HourMS)
	store := &repairStore{bars: map[int64]market.Bar{start: existing, predecessor.StartMS: predecessor}}
	fetcher := &repairFetcher{bars: []market.Bar{testBar("binance", "1H", start), testBar("binance", "1H", start+timeframe.HourMS), testBar("binance", "1H", start+2*timeframe.HourMS), testBar("binance", "1H", start+3*timeframe.HourMS)}}
	result, err := Repair(context.Background(), store, fetcher, nil, testTask("binance", "1H", start, start+timeframe.HourMS, start+3*timeframe.HourMS), true)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "repaired" || result.Requested != 3 || result.AlreadyPresent != 1 || result.Fetched != 4 || result.Available != 2 || result.Inserted != 2 || len(result.MissingStarts) != 0 {
		t.Fatalf("unexpected result %+v", result)
	}
	if store.bars[start].ClosePrice != 101 || len(store.written) != 2 {
		t.Fatalf("existing bar was overwritten: %+v", store)
	}
	if _, found := store.bars[start+2*timeframe.HourMS]; found {
		t.Fatal("healthy timestamp between audited targets was inserted")
	}
	if store.bars[start+timeframe.HourMS].PrevClose != 101 {
		t.Fatalf("predecessor close was not applied: %+v", store.bars[start+timeframe.HourMS])
	}
	request := fetcher.requests[0]
	if request.Timeframe != "1H" || request.StartMS != start || request.EndMS != start+4*timeframe.HourMS-1 || request.Limit != 0 || request.Adjustment != exchange.KlineAdjustmentRaw {
		t.Fatalf("wrong REST request %+v", request)
	}
}

func TestRepairDryRunDoesNotWriteAndKeepsActualMissingStarts(t *testing.T) {
	start := testStart("1H")
	store := &repairStore{}
	fetcher := &repairFetcher{bars: []market.Bar{testBar("okx", "1H", start)}}
	result, err := Repair(context.Background(), store, fetcher, nil, testTask("okx", "1H", start), false)
	if err != nil || result.Status != "dry_run" || result.Available != 1 || result.Inserted != 0 || len(result.MissingStarts) != 1 || store.writes != 0 {
		t.Fatalf("bad dry run result %+v err=%v writes=%d", result, err, store.writes)
	}
}

func TestRepairAlreadyPresentDoesNotFetchOrWrite(t *testing.T) {
	start := testStart("1H")
	store := &repairStore{bars: map[int64]market.Bar{start: testBar("binance", "1H", start)}}
	fetcher := &repairFetcher{err: errors.New("must not fetch")}
	result, err := Repair(context.Background(), store, fetcher, nil, testTask("binance", "1H", start), true)
	if err != nil || result.Status != "already_present" || result.AlreadyPresent != 1 || len(result.MissingStarts) != 0 || fetcher.calls != 0 || store.writes != 0 {
		t.Fatalf("unexpected already present result %+v err=%v", result, err)
	}
}

func TestRepairLiveConflictNeverOverwritesAndRemainsMissing(t *testing.T) {
	for _, finalOnly := range []bool{false, true} {
		t.Run(map[bool]string{false: "visible", true: "final-only-store"}[finalOnly], func(t *testing.T) {
			start := testStart("1H")
			live := testBar("binance", "1H", start)
			live.IsFinal, live.ClosePrice = false, 91
			store := &repairStore{bars: map[int64]market.Bar{start: live}, finalOnly: finalOnly}
			fetcher := &repairFetcher{bars: []market.Bar{testBar("binance", "1H", start)}}
			result, err := Repair(context.Background(), store, fetcher, nil, testTask("binance", "1H", start), true)
			if err != nil || result.Status == "repaired" || result.Inserted != 0 || len(result.MissingStarts) != 1 || store.bars[start].IsFinal || store.bars[start].ClosePrice != 91 {
				t.Fatalf("live conflict overwritten or misreported: %+v err=%v row=%+v", result, err, store.bars[start])
			}
		})
	}
}

func TestRepairEmptyOfficialResponseIsUnresolved(t *testing.T) {
	start := testStart("1H")
	for _, apply := range []bool{false, true} {
		store := &repairStore{}
		result, err := Repair(context.Background(), store, &repairFetcher{}, nil, testTask("okx", "1H", start), apply)
		if err != nil || result.Status != "no_data" || result.Inserted != 0 || len(result.MissingStarts) != 1 || store.writes != 0 {
			t.Fatalf("empty response falsely repaired: %+v err=%v", result, err)
		}
	}
}

func TestRepairDerivedRequiresCompleteDailySources(t *testing.T) {
	start := testStart("2D")
	for _, complete := range []bool{false, true} {
		store := &repairStore{}
		bars := []market.Bar{testBar("binance", "1D", start)}
		if complete {
			bars = append(bars, testBar("binance", "1D", start+timeframe.DayMS))
		}
		fetcher := &repairFetcher{bars: bars}
		result, err := Repair(context.Background(), store, fetcher, nil, testTask("binance", "2D", start), true)
		if err != nil {
			t.Fatal(err)
		}
		if complete {
			got := store.bars[start]
			if result.Status != "repaired" || got.Timeframe != "2D" || got.StartMS != start || got.EndMS != start+2*timeframe.DayMS-1 || got.Volume != 2 {
				t.Fatalf("incorrect full-source rollup %+v %+v", result, got)
			}
		} else if result.Status != "no_data" || result.Available != 0 || store.writes != 0 || len(result.MissingStarts) != 1 {
			t.Fatalf("incomplete source fabricated final %+v", result)
		}
		if fetcher.requests[0].Timeframe != "1D" || fetcher.requests[0].EndMS != start+2*timeframe.DayMS-1 {
			t.Fatalf("source request truncated %+v", fetcher.requests[0])
		}
	}
}

func TestRepairDisabledThreeDayDoesNotReadFetchOrWrite(t *testing.T) {
	for _, exchange := range []string{"binance", "okx"} {
		for _, apply := range []bool{false, true} {
			store, fetcher := &repairStore{}, &repairFetcher{}
			result, err := Repair(context.Background(), store, fetcher, nil, testTask(exchange, "3D", testStart("3D")), apply)
			if !errors.Is(err, timeframe.ErrDisabledTimeframe) || result.Status != "error" || store.reads != 0 || store.writes != 0 || fetcher.calls != 0 {
				t.Fatalf("disabled repair accessed storage or REST: %+v err=%v reads=%d writes=%d requests=%d", result, err, store.reads, store.writes, fetcher.calls)
			}
		}
	}
}

func TestRepairQuarterlyRollupUsesCalendarMonths(t *testing.T) {
	start := testStart("3M")
	second := timeframe.NextStartMS(start, "1M")
	third := timeframe.NextStartMS(second, "1M")
	fetcher := &repairFetcher{bars: []market.Bar{testBar("binance", "1M", third), testBar("binance", "1M", start), testBar("binance", "1M", second)}}
	store := &repairStore{}
	result, err := Repair(context.Background(), store, fetcher, nil, testTask("binance", "3M", start), true)
	if err != nil || result.Status != "repaired" || store.bars[start].EndMS != timeframe.EndMS(start, "3M") || store.bars[start].Volume != 3 {
		t.Fatalf("bad calendar quarter rollup %+v err=%v bar=%+v", result, err, store.bars[start])
	}
}

func TestRepairRejectsInvalidOfficialBars(t *testing.T) {
	start := testStart("1H")
	tests := map[string]func(*market.Bar){
		"wrong exchange": func(b *market.Bar) { b.Exchange = "okx" },
		"wrong symbol":   func(b *market.Bar) { b.Symbol = "OTHER" },
		"wrong frame":    func(b *market.Bar) { b.Timeframe = "2H" },
		"nonfinal":       func(b *market.Bar) { b.IsFinal = false },
		"wrong end":      func(b *market.Bar) { b.EndMS-- },
		"unaligned":      func(b *market.Bar) { b.StartMS++ },
		"nan price":      func(b *market.Bar) { b.OpenPrice = math.NaN() },
		"inf price":      func(b *market.Bar) { b.HighPrice = math.Inf(1) },
		"negative price": func(b *market.Bar) { b.LowPrice = -1 },
		"bad ohlc":       func(b *market.Bar) { b.HighPrice = 99 },
		"nan volume":     func(b *market.Bar) { b.Volume = math.NaN() },
		"bad volume":     func(b *market.Bar) { b.QuoteVolume = -1 },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			bar := testBar("binance", "1H", start)
			mutate(&bar)
			store := &repairStore{}
			result, err := Repair(context.Background(), store, &repairFetcher{bars: []market.Bar{bar}}, nil, testTask("binance", "1H", start), true)
			if err != nil || result.Available != 0 || result.Status != "no_data" || store.writes != 0 {
				t.Fatalf("invalid official bar accepted %+v err=%v", result, err)
			}
		})
	}
}

func TestRepairRejectsDuplicateOrInvalidDailySource(t *testing.T) {
	start := testStart("2D")
	for _, duplicate := range []bool{false, true} {
		first, second := testBar("binance", "1D", start), testBar("binance", "1D", start+timeframe.DayMS)
		bars := []market.Bar{first, second}
		if duplicate {
			bars = append(bars, first)
		} else {
			bars[1].EndMS--
		}
		store := &repairStore{}
		result, err := Repair(context.Background(), store, &repairFetcher{bars: bars}, nil, testTask("binance", "2D", start), true)
		if err != nil || result.Available != 0 || store.writes != 0 || result.Status != "no_data" {
			t.Fatalf("invalid derived source accepted %+v err=%v", result, err)
		}
	}
}

func TestRepairVerifiesPersistedKeysInsteadOfTrustingWriterCount(t *testing.T) {
	start := testStart("1H")
	store := &repairStore{dropInsert: true}
	result, err := Repair(context.Background(), store, &repairFetcher{bars: []market.Bar{testBar("binance", "1H", start)}}, nil, testTask("binance", "1H", start), true)
	if err != nil || result.Status != "partial" || result.Available != 1 || result.Inserted != 0 || len(result.MissingStarts) != 1 {
		t.Fatalf("unpersisted row reported as repaired %+v err=%v", result, err)
	}
}

func TestRepairConcurrentInsertSkipsExistingWithoutOverwrite(t *testing.T) {
	start := testStart("1H")
	store := &repairStore{bars: make(map[int64]market.Bar), beforeInsert: func(s *repairStore) {
		bar := testBar("binance", "1H", start)
		bar.ClosePrice = 99
		s.bars[start] = bar
	}}
	result, err := Repair(context.Background(), store, &repairFetcher{bars: []market.Bar{testBar("binance", "1H", start)}}, nil, testTask("binance", "1H", start), true)
	if err != nil || result.Status != "repaired" || result.Inserted != 0 || store.bars[start].ClosePrice != 99 {
		t.Fatalf("concurrent row overwritten %+v err=%v", result, err)
	}
}

func TestRepairPartialOfficialResponseRetainsMissingKeys(t *testing.T) {
	start := testStart("1H")
	store := &repairStore{}
	result, err := Repair(context.Background(), store, &repairFetcher{bars: []market.Bar{testBar("binance", "1H", start)}}, nil, testTask("binance", "1H", start, start+timeframe.HourMS), true)
	if err != nil || result.Status != "partial" || result.Inserted != 1 || len(result.MissingStarts) != 1 || result.MissingStarts[0] != start+timeframe.HourMS {
		t.Fatalf("partial result wrong %+v err=%v", result, err)
	}
}

func TestRepairErrorsKeepUnrepairedTargets(t *testing.T) {
	start := testStart("1H")
	task := testTask("binance", "1H", start)
	for _, name := range []string{"read", "fetch", "insert", "verify"} {
		t.Run(name, func(t *testing.T) {
			store := &repairStore{}
			fetcher := &repairFetcher{bars: []market.Bar{testBar("binance", "1H", start)}}
			switch name {
			case "read":
				store.readErr = errors.New("read failed")
			case "fetch":
				fetcher.err = errors.New("fetch failed")
			case "insert":
				store.insertErr = errors.New("insert failed")
			case "verify":
				store.readErr, store.failReadAfter = errors.New("verify failed"), 2
			}
			result, err := Repair(context.Background(), store, fetcher, nil, task, true)
			if err == nil || result.Status != "error" || result.Error == "" {
				t.Fatalf("error swallowed %+v err=%v", result, err)
			}
		})
	}
}

func TestRepairRejectsInvalidTaskBeforeIO(t *testing.T) {
	start := testStart("1H")
	tasks := []Task{
		testTask("binance", "1H", start),
		testTask("binance", "1H", start),
		testTask("okx", "1H", start, start+80*timeframe.HourMS),
		testTask("binance", "1H", timeframe.FloorStartMS(time.Now().UnixMilli(), "1H")),
	}
	tasks[0].SourceTimeframe = "1D"
	tasks[1].EndMS--
	for _, task := range tasks {
		store := &repairStore{}
		fetcher := &repairFetcher{}
		result, err := Repair(context.Background(), store, fetcher, nil, task, true)
		if err == nil || result.Status != "error" || store.reads != 0 || fetcher.calls != 0 || store.writes != 0 {
			t.Fatalf("invalid task reached IO %+v err=%v", result, err)
		}
	}
}
