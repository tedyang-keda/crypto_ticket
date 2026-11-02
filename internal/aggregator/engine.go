package aggregator

import (
	"math"
	"sort"

	"crypto-ticket/internal/market"
	"crypto-ticket/internal/timeframe"
)

const OneMinute = "1m"

func RollupSourceTimeframe(target string) string {
	switch timeframe.MustNormalize(target) {
	case "5m", "15m":
		return "1m"
	case "30m":
		return "15m"
	case "1H":
		return "30m"
	case "2H", "4H", "6H", "12H", "1D":
		return "1H"
	case "2D", "3D", "5D", "1W", "2W", "1M", "3M":
		return "1D"
	default:
		return ""
	}
}

func RollupBars(tf string, bars []market.Bar, isFinal bool, reason string, updatedAtMS int64) *market.Bar {
	tf = timeframe.MustNormalize(tf)
	if len(bars) == 0 {
		return nil
	}
	ordered := append([]market.Bar(nil), bars...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].StartMS < ordered[j].StartMS })
	firstPrice, lastPrice := 0, len(ordered)-1
	hasTrades := false
	for i, bar := range ordered {
		if !hasTradeActivity(bar) {
			continue
		}
		if !hasTrades {
			firstPrice = i
			hasTrades = true
		}
		lastPrice = i
	}

	startMS := timeframe.FloorStartMS(ordered[0].StartMS, tf)
	rollup := market.Bar{
		Exchange:    ordered[0].Exchange,
		Symbol:      ordered[0].Symbol,
		MarginType:  ordered[0].MarginType,
		Timeframe:   tf,
		StartMS:     startMS,
		EndMS:       timeframe.EndMS(startMS, tf),
		OpenPrice:   ordered[firstPrice].OpenPrice,
		HighPrice:   ordered[firstPrice].HighPrice,
		LowPrice:    ordered[firstPrice].LowPrice,
		ClosePrice:  ordered[lastPrice].ClosePrice,
		LastTickMS:  ordered[len(ordered)-1].LastTickMS,
		IsFinal:     isFinal,
		Source:      "rollup",
		Reason:      reason,
		UpdatedAtMS: updatedAtMS,
		VolumeUnit:  ordered[0].VolumeUnit,
		QuoteUnit:   ordered[0].QuoteUnit,
	}
	for _, bar := range ordered {
		// No-trade candles preserve time coverage, but their carried prices are
		// not trades in this bucket. A wholly empty bucket retains the existing
		// carry-forward OHLC behavior instead of producing an invalid zero bar.
		if !hasTrades || hasTradeActivity(bar) {
			if bar.HighPrice > rollup.HighPrice {
				rollup.HighPrice = bar.HighPrice
			}
			if bar.LowPrice < rollup.LowPrice {
				rollup.LowPrice = bar.LowPrice
			}
		}
		rollup.Volume += bar.Volume
		rollup.QuoteVolume += bar.QuoteVolume
		rollup.ContractVolume += bar.ContractVolume
		rollup.TradeCount += bar.TradeCount
		if bar.LastTickMS > rollup.LastTickMS {
			rollup.LastTickMS = bar.LastTickMS
		}
	}
	return &rollup
}

func hasTradeActivity(bar market.Bar) bool {
	// Some exchanges (including OKX) do not supply trade counts for candles.
	// Any available positive volume therefore also establishes traded prices.
	return bar.TradeCount > 0 || bar.Volume > 0 || bar.QuoteVolume > 0 || bar.ContractVolume > 0
}

func ApplyDerived(bar market.Bar, previousClose float64) market.Bar {
	bar.PrevClose = previousClose
	if previousClose > 0 {
		bar.Chg = roundPercent((bar.ClosePrice - previousClose) / previousClose * 100)
	}
	if bar.LowPrice > 0 {
		bar.Amp = roundPercent((bar.HighPrice - bar.LowPrice) / bar.LowPrice * 100)
	}
	return market.DecorateBar(bar)
}

func roundPercent(value float64) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0
	}
	return math.Round(value*1_000_000) / 1_000_000
}
