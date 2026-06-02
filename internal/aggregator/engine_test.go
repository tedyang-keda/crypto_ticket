package aggregator

import (
	"testing"

	"crypto-ticket/internal/market"
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
