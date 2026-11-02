package aggregator

import (
	"reflect"
	"testing"
	"time"

	"crypto-ticket/internal/market"
	"crypto-ticket/internal/timeframe"
)

func TestRollupBarsFromOneMinuteBars(t *testing.T) {
	base := int64(1779262200000)
	bars := []market.Bar{
		{Exchange: "binance", Symbol: "BTCUSDT", MarginType: "umargin", Timeframe: "1m", StartMS: base, EndMS: base + 59_999, OpenPrice: 100, HighPrice: 105, LowPrice: 99, ClosePrice: 102, Volume: 1, QuoteVolume: 100, ContractVolume: 0, TradeCount: 2, LastTickMS: base + 30_000, IsFinal: true, VolumeUnit: "BTC", QuoteUnit: "USDT"},
		{Exchange: "binance", Symbol: "BTCUSDT", MarginType: "umargin", Timeframe: "1m", StartMS: base + 60_000, EndMS: base + 119_999, OpenPrice: 102, HighPrice: 110, LowPrice: 101, ClosePrice: 108, Volume: 3, QuoteVolume: 300, ContractVolume: 0, TradeCount: 4, LastTickMS: base + 90_000, IsFinal: true, VolumeUnit: "BTC", QuoteUnit: "USDT"},
	}

	rollup := RollupBars("5m", bars, false, "live", base+120_000)
	if rollup == nil {
		t.Fatal("expected rollup")
	}
	if rollup.OpenPrice != 100 || rollup.HighPrice != 110 || rollup.LowPrice != 99 || rollup.ClosePrice != 108 {
		t.Fatalf("unexpected OHLC: %+v", rollup)
	}
	if rollup.Volume != 4 || rollup.QuoteVolume != 400 || rollup.TradeCount != 6 {
		t.Fatalf("unexpected totals: %+v", rollup)
	}
	if rollup.MarginType != "umargin" || rollup.VolumeUnit != "BTC" || rollup.QuoteUnit != "USDT" {
		t.Fatalf("unexpected metadata: %+v", rollup)
	}
	if rollup.IsFinal {
		t.Fatalf("expected live rollup, got final: %+v", rollup)
	}
}

func TestRollupBarsExcludesNoTradePricesWithoutDroppingCoverage(t *testing.T) {
	base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	bars := []market.Bar{
		{Exchange: "binance", Symbol: "TESTUSDT", Timeframe: "1m", StartMS: base + 4*timeframe.MinuteMS, OpenPrice: 900, HighPrice: 900, LowPrice: 900, ClosePrice: 900, LastTickMS: base + 5*timeframe.MinuteMS - 1},
		{Exchange: "binance", Symbol: "TESTUSDT", Timeframe: "1m", StartMS: base + timeframe.MinuteMS, OpenPrice: 100, HighPrice: 105, LowPrice: 99, ClosePrice: 102, TradeCount: 1, Volume: 2, QuoteVolume: 200, ContractVolume: 3},
		{Exchange: "binance", Symbol: "TESTUSDT", Timeframe: "1m", StartMS: base, OpenPrice: 1, HighPrice: 1, LowPrice: 1, ClosePrice: 1},
		{Exchange: "binance", Symbol: "TESTUSDT", Timeframe: "1m", StartMS: base + 3*timeframe.MinuteMS, OpenPrice: 102, HighPrice: 110, LowPrice: 101, ClosePrice: 108, TradeCount: 2, Volume: 3, QuoteVolume: 300, ContractVolume: 4},
		{Exchange: "binance", Symbol: "TESTUSDT", Timeframe: "1m", StartMS: base + 2*timeframe.MinuteMS, OpenPrice: 800, HighPrice: 1000, LowPrice: 0.5, ClosePrice: 800},
	}
	original := append([]market.Bar(nil), bars...)
	rollup := RollupBars("5m", bars, true, "test", base+10*timeframe.MinuteMS)
	if rollup.OpenPrice != 100 || rollup.HighPrice != 110 || rollup.LowPrice != 99 || rollup.ClosePrice != 108 {
		t.Fatalf("no-trade prices polluted OHLC: %+v", rollup)
	}
	if rollup.StartMS != base || rollup.EndMS != timeframe.EndMS(base, "5m") || rollup.LastTickMS != base+5*timeframe.MinuteMS-1 {
		t.Fatalf("no-trade source coverage was dropped: %+v", rollup)
	}
	if rollup.Volume != 5 || rollup.QuoteVolume != 500 || rollup.ContractVolume != 7 || rollup.TradeCount != 3 {
		t.Fatalf("unexpected activity totals: %+v", rollup)
	}
	if !reflect.DeepEqual(bars, original) {
		t.Fatal("rollup mutated the caller's source bars")
	}
}

func TestRollupBarsRecognizesEveryAvailableTradeIndicator(t *testing.T) {
	tests := []struct {
		name string
		bar  market.Bar
	}{
		{name: "trade count", bar: market.Bar{TradeCount: 1}},
		{name: "base volume with no trade count", bar: market.Bar{Volume: 1}},
		{name: "quote volume only", bar: market.Bar{QuoteVolume: 1}},
		{name: "contract volume only", bar: market.Bar{ContractVolume: 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			traded := tt.bar
			traded.StartMS = 60_000
			traded.OpenPrice, traded.HighPrice, traded.LowPrice, traded.ClosePrice = 100, 110, 90, 105
			bars := []market.Bar{
				{StartMS: 0, OpenPrice: 1, HighPrice: 1, LowPrice: 1, ClosePrice: 1},
				traded,
				{StartMS: 120_000, OpenPrice: 900, HighPrice: 900, LowPrice: 900, ClosePrice: 900},
			}
			got := RollupBars("5m", bars, true, "test", 0)
			if got.OpenPrice != 100 || got.HighPrice != 110 || got.LowPrice != 90 || got.ClosePrice != 105 {
				t.Fatalf("available activity was not used for OHLC: %+v", got)
			}
		})
	}
}

func TestRollupBarsWhollyUntradedBucketKeepsCarryForwardPrices(t *testing.T) {
	bars := []market.Bar{
		{StartMS: 0, OpenPrice: 101, HighPrice: 102, LowPrice: 100, ClosePrice: 101},
		{StartMS: 60_000, OpenPrice: 101, HighPrice: 103, LowPrice: 99, ClosePrice: 102},
	}
	got := RollupBars("5m", bars, true, "test", 0)
	if got.OpenPrice != 101 || got.HighPrice != 103 || got.LowPrice != 99 || got.ClosePrice != 102 {
		t.Fatalf("empty bucket lost existing carry-forward behavior: %+v", got)
	}
	if got.Volume != 0 || got.QuoteVolume != 0 || got.ContractVolume != 0 || got.TradeCount != 0 {
		t.Fatalf("empty bucket acquired fabricated activity: %+v", got)
	}
}

func TestRollupBarsBinanceTradFiOpeningCarryPriceRegressions(t *testing.T) {
	// Official November 1 opening boundaries: preceding zero-trade minute
	// candles retain the previous price, while the official 1H uses trades.
	tests := []struct {
		symbol                 string
		carry, open, high, low float64
		close                  float64
		firstTradedMinute      int
	}{
		{symbol: "BACUSDT", carry: 50.26, open: 50.33, high: 50.39, low: 50.30, close: 50.39, firstTradedMinute: 3},
		{symbol: "BRKBUSDT", carry: 544.87, open: 544.85, high: 545.30, low: 544.85, close: 545.30, firstTradedMinute: 3},
		{symbol: "BSPUSDT", carry: 45.94, open: 45.97, high: 45.97, low: 45.90, close: 45.93, firstTradedMinute: 1},
	}
	for _, tt := range tests {
		t.Run(tt.symbol, func(t *testing.T) {
			base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
			bars := make([]market.Bar, 60)
			for i := range bars {
				price := tt.carry
				if i > tt.firstTradedMinute {
					price = tt.open
				}
				bars[i] = market.Bar{Exchange: "binance", Symbol: tt.symbol, Timeframe: "1m", StartMS: base + int64(i)*timeframe.MinuteMS,
					OpenPrice: price, HighPrice: price, LowPrice: price, ClosePrice: price}
			}
			bars[tt.firstTradedMinute].OpenPrice = tt.open
			bars[tt.firstTradedMinute].HighPrice = tt.open
			bars[tt.firstTradedMinute].LowPrice = tt.open
			bars[tt.firstTradedMinute].ClosePrice = tt.open
			bars[tt.firstTradedMinute].Volume, bars[tt.firstTradedMinute].TradeCount = 1, 1
			bars[59].OpenPrice, bars[59].HighPrice, bars[59].LowPrice, bars[59].ClosePrice = tt.open, tt.high, tt.low, tt.close
			bars[59].Volume, bars[59].TradeCount = 1, 1
			got := RollupBars("1H", bars, true, "test", 0)
			if got.OpenPrice != tt.open || got.HighPrice != tt.high || got.LowPrice != tt.low || got.ClosePrice != tt.close {
				t.Fatalf("opening carry-price regression: %+v", got)
			}
		})
	}
}

func TestRollupBarsZeroTradeHandlingIsAssociativeAcrossCascade(t *testing.T) {
	base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	bars := make([]market.Bar, 24*60)
	trades := map[int]bool{5: true, 37: true, 61: true, 121: true, 511: true, 1333: true}
	for i := range bars {
		price := float64(1)
		if i%2 == 0 {
			price = 1000
		}
		bar := market.Bar{Exchange: "binance", Symbol: "TESTUSDT", Timeframe: "1m", StartMS: base + int64(i)*timeframe.MinuteMS,
			EndMS: base + int64(i+1)*timeframe.MinuteMS - 1, OpenPrice: price, HighPrice: price, LowPrice: price, ClosePrice: price, IsFinal: true}
		if trades[i] {
			bar.OpenPrice, bar.HighPrice, bar.LowPrice, bar.ClosePrice = 100, 110, 90, 105
			bar.Volume, bar.QuoteVolume, bar.ContractVolume, bar.TradeCount = 1, 100, 2, 1
		}
		bars[i] = bar
	}
	direct := RollupBars("1D", bars, true, "test", 0)
	cascade := bars
	for _, step := range []struct {
		frame string
		size  int
	}{{"15m", 15}, {"30m", 2}, {"1H", 2}, {"1D", 24}} {
		var next []market.Bar
		for i := 0; i < len(cascade); i += step.size {
			next = append(next, *RollupBars(step.frame, cascade[i:i+step.size], true, "test", 0))
		}
		cascade = next
	}
	if len(cascade) != 1 || !reflect.DeepEqual(*direct, cascade[0]) {
		t.Fatalf("direct and chained aggregation differ: direct=%+v cascade=%+v", direct, cascade)
	}
}

func TestRollupSourceTimeframeUsesCascadeInputs(t *testing.T) {
	cases := map[string]string{
		"5m":  "1m",
		"15m": "1m",
		"30m": "15m",
		"1H":  "30m",
		"12H": "1H",
		"1D":  "1H",
		"1M":  "1D",
		"3M":  "1D",
	}
	for target, expected := range cases {
		if got := RollupSourceTimeframe(target); got != expected {
			t.Fatalf("target=%s expected source=%s got=%s", target, expected, got)
		}
	}
}

func TestApplyDerivedUsesPreviousCloseAndLow(t *testing.T) {
	bar := ApplyDerived(market.Bar{
		OpenPrice:  100,
		HighPrice:  120,
		LowPrice:   100,
		ClosePrice: 115,
	}, 105)
	if bar.PrevClose != 105 || bar.Chg != 9.52381 || bar.Amp != 20 {
		t.Fatalf("unexpected derived fields: %+v", bar)
	}
}
