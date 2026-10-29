package timeaudit

import (
	"testing"
	"time"

	"crypto-ticket/internal/timeframe"
)

func stamp(s string) int64 {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t.UnixMilli()
}
func clock(s string) time.Time { return time.UnixMilli(stamp(s)).UTC() }
func add(t *testing.T, a *Accumulator, symbol string, starts ...int64) {
	t.Helper()
	for _, start := range starts {
		if err := a.Add(Row{symbol, start, timeframe.EndMS(start, a.tf)}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestFixedInternalGapAndRegistryNoData(t *testing.T) {
	a, err := New("binance", "15m", clock("2026-10-16T12:02:00Z"), []Metadata{{Exchange: "binance", Symbol: "BTCUSDT", Active: true}, {Exchange: "binance", Symbol: "EMPTY", Active: false}})
	if err != nil {
		t.Fatal(err)
	}
	add(t, a, "BTCUSDT", stamp("2026-10-16T10:00:00Z"), stamp("2026-10-16T10:45:00Z"))
	r := a.Finish()
	if r.BarCount != 2 || r.Missing != 2 || r.GapRanges != 1 || r.NoDataSeries != 1 || r.SeriesCount != 2 {
		t.Fatalf("result=%+v", r)
	}
	s := r.Series[0]
	if s.ExpectedInteriorCount != 4 || s.TailLagBuckets != 4 || !s.TailLagCandidate {
		t.Fatalf("series=%+v", s)
	}
	g := s.GapRanges[0]
	if g.FirstMS != stamp("2026-10-16T10:15:00Z") || g.LastMS != stamp("2026-10-16T10:30:00Z") {
		t.Fatalf("gap=%+v", g)
	}
}

func TestCalendarPeriodsAndLeapMonth(t *testing.T) {
	for _, test := range []struct {
		tf, first, last string
		missing         int64
	}{
		{"1W", "2026-06-01T00:00:00Z", "2026-06-22T00:00:00Z", 2},
		{"2W", "2026-05-25T00:00:00Z", "2026-07-06T00:00:00Z", 2},
		{"1M", "2024-01-01T00:00:00Z", "2024-04-01T00:00:00Z", 2},
		{"3M", "2024-01-01T00:00:00Z", "2024-10-01T00:00:00Z", 2},
	} {
		t.Run(test.tf, func(t *testing.T) {
			a, _ := New("okx", test.tf, clock("2026-10-16T00:02:00Z"), nil)
			add(t, a, "BTC", stamp(test.first), stamp(test.last))
			r := a.Finish()
			if r.Missing != test.missing || r.Misaligned != 0 || r.Series[0].ExpectedInteriorCount != 4 {
				t.Fatalf("result=%+v", r)
			}
		})
	}
}

func TestMixedPhasesDoNotBecomeGapAnchors(t *testing.T) {
	a, _ := New("binance", "1H", clock("2026-10-16T10:02:00Z"), nil)
	add(t, a, "BTC", stamp("2026-10-16T01:00:00Z"), stamp("2026-10-16T01:30:00Z"), stamp("2026-10-16T03:00:00Z"))
	r := a.Finish()
	if r.Missing != 1 || r.Misaligned != 1 || r.CanonicalBars != 2 || r.BarCount != 3 {
		t.Fatalf("result=%+v", r)
	}
	if r.Series[0].GapRanges[0].FirstMS != stamp("2026-10-16T02:00:00Z") {
		t.Fatal("shifted anchor manufactured gap")
	}
	b, _ := New("binance", "1H", clock("2026-10-16T10:02:00Z"), nil)
	add(t, b, "BTC", stamp("2026-10-16T01:30:00Z"), stamp("2026-10-16T03:30:00Z"))
	if r = b.Finish(); r.Missing != 0 || r.CanonicalBars != 0 || r.Misaligned != 2 {
		t.Fatalf("off-grid-only=%+v", r)
	}
}

func TestFinalityGraceExcludesOpenAndRecentlyClosed(t *testing.T) {
	for _, test := range []struct{ now, latest string }{
		{"2026-10-16T12:01:00Z", "2026-10-16T10:00:00Z"},
		{"2026-10-16T12:02:00Z", "2026-10-16T11:00:00Z"},
		{"2026-10-16T12:59:59Z", "2026-10-16T11:00:00Z"},
	} {
		if got := LatestCompletedStart("1H", clock(test.now)); got != stamp(test.latest) {
			t.Errorf("%s latest=%s", test.now, FormatMS(got))
		}
	}
	a, _ := New("okx", "1H", clock("2026-10-16T12:01:00Z"), nil)
	add(t, a, "BTC", stamp("2026-10-16T10:00:00Z"), stamp("2026-10-16T11:00:00Z"), stamp("2026-10-16T12:00:00Z"))
	if r := a.Finish(); r.BarCount != 1 {
		t.Fatalf("open/recent rows included: %+v", r)
	}
}

func TestRetentionEdgesAreNotInventedAsMissing(t *testing.T) {
	a, _ := New("okx", "30m", clock("2026-10-16T12:02:00Z"), []Metadata{{Exchange: "okx", Symbol: "BTC", Active: true, FirstSeenMS: stamp("2026-01-01T00:00:00Z")}})
	add(t, a, "BTC", stamp("2026-10-16T11:00:00Z"), stamp("2026-10-16T11:30:00Z"))
	r := a.Finish()
	if r.Missing != 0 || r.Series[0].ExpectedInteriorCount != 2 || r.TailLagSeries != 0 {
		t.Fatalf("retention edge became missing: %+v", r)
	}
}

func TestInvalidEndDuplicateAndOrder(t *testing.T) {
	a, _ := New("okx", "1H", clock("2026-10-16T12:02:00Z"), nil)
	start := stamp("2026-10-16T10:00:00Z")
	if err := a.Add(Row{"BTC", start, start}); err != nil {
		t.Fatal(err)
	}
	add(t, a, "BTC", start)
	r := a.Finish()
	if r.InvalidEnd != 1 || r.Duplicates != 1 || r.BarCount != 2 || r.CanonicalBars != 1 || r.Missing != 0 {
		t.Fatalf("result=%+v", r)
	}
	if err := a.Add(Row{"BTC", start - 3600000, start - 1}); err == nil {
		t.Fatal("unordered stream accepted")
	}
}

func TestWholeStoredUniverseIncludesNoDataAndInactive(t *testing.T) {
	a, _ := New("okx", "3M", clock("2026-10-16T12:02:00Z"), []Metadata{{Exchange: "okx", Symbol: "EMPTY", Active: false}})
	r := a.Finish()
	CompleteUniverse(&r, []Metadata{{Exchange: "okx", Symbol: "ORPHAN"}, {Exchange: "okx", Symbol: "EMPTY"}, {Exchange: "binance", Symbol: "OTHER"}})
	if r.SeriesCount != 2 || r.NoDataSeries != 2 || r.Missing != 0 || r.BarCount != 0 || len(r.IssueSeries) != 2 {
		t.Fatalf("result=%+v", r)
	}
	if r.Series[1].InRegistry {
		t.Fatal("stored orphan misrepresented as registry symbol")
	}
}

func TestMisalignedInLatestClosedBucketStillCounted(t *testing.T) {
	a, _ := New("okx", "1H", clock("2026-10-16T12:02:00Z"), nil)
	add(t, a, "BTC", stamp("2026-10-16T11:30:00Z"))
	r := a.Finish()
	if r.BarCount != 1 || r.Misaligned != 1 {
		t.Fatalf("off-grid latest bucket lost: %+v", r)
	}
}

func TestSymbolGroupOrderDoesNotAssumeMySQLCollationIsGoByteOrder(t *testing.T) {
	a, _ := New("okx", "1H", clock("2026-10-16T12:02:00Z"), nil)
	start := stamp("2026-10-16T10:00:00Z")
	add(t, a, "ACE-USDT-SWAP", start)
	add(t, a, "A-USDT-SWAP", start)
	if err := a.Add(Row{"ACE-USDT-SWAP", start + 3600000, start + 7200000 - 1}); err == nil {
		t.Fatal("noncontiguous symbol group accepted")
	}
}
