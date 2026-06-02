package aggregator

import (
	"testing"

	"crypto-ticket/internal/market"
)

func TestOneMinuteBarUsesTickEventTimeForClose(t *testing.T) {
	base := int64(1779262200000)
	bar := NewOneMinuteBar(market.Tick{Exchange: "binance", Symbol: "BTCUSDT", TsMS: base + 30_000, Price: 100, Size: 1})

	bar = ApplyTick(bar, market.Tick{Exchange: "binance", Symbol: "BTCUSDT", TsMS: base + 10_000, Price: 90, Size: 2})
	if bar.LowPrice != 90 {
		t.Fatalf("expected low from late lower-price tick, got %+v", bar)
	}
	if bar.ClosePrice != 100 {
		t.Fatalf("close should remain latest event-time tick price, got %+v", bar)
	}
	if bar.Volume != 3 || bar.TradeCount != 2 {
		t.Fatalf("unexpected accumulated volume/count: %+v", bar)
	}
}

func TestGapBarsUsePreviousClose(t *testing.T) {
	base := int64(1779262200000)
	previous := NewOneMinuteBar(market.Tick{Exchange: "okx", Symbol: "BTC-USDT-SWAP", TsMS: base + 10_000, Price: 100, Size: 1})

	gaps := GapBars(previous, base+3*60_000, base+3*60_000)
	if len(gaps) != 2 {
		t.Fatalf("expected 2 gap bars, got %d", len(gaps))
	}
	for _, gap := range gaps {
		if !gap.IsFinal || gap.Reason != "gap" || gap.OpenPrice != 100 || gap.Volume != 0 {
			t.Fatalf("unexpected gap bar: %+v", gap)
		}
	}
}

func TestRollupBarsFromOneMinuteBars(t *testing.T) {
	base := int64(1779262200000)
	bars := []market.Bar{
		{Exchange: "binance", Symbol: "BTCUSDT", Timeframe: "1m", StartMS: base, EndMS: base + 59_999, OpenPrice: 100, HighPrice: 105, LowPrice: 99, ClosePrice: 102, Volume: 1, QuoteVolume: 100, TradeCount: 2, LastTickMS: base + 30_000, IsFinal: true},
		{Exchange: "binance", Symbol: "BTCUSDT", Timeframe: "1m", StartMS: base + 60_000, EndMS: base + 119_999, OpenPrice: 102, HighPrice: 110, LowPrice: 101, ClosePrice: 108, Volume: 3, QuoteVolume: 300, TradeCount: 4, LastTickMS: base + 90_000, IsFinal: true},
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
	if rollup.IsFinal {
		t.Fatalf("expected live rollup, got final: %+v", rollup)
	}
}
