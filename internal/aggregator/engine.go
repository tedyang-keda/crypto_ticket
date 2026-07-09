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

	startMS := timeframe.FloorStartMS(ordered[0].StartMS, tf)
	rollup := market.Bar{
		Exchange:       ordered[0].Exchange,
		SourceMarket:   ordered[0].SourceMarket,
		Symbol:         ordered[0].Symbol,
		InstrumentType: ordered[0].InstrumentType,
		AssetClass:     ordered[0].AssetClass,
		RuleType:       ordered[0].RuleType,
		LifecyclePhase: ordered[0].LifecyclePhase,
		MarginType:     ordered[0].MarginType,
		Timeframe:      tf,
		StartMS:        startMS,
		EndMS:          timeframe.EndMS(startMS, tf),
		OpenPrice:      ordered[0].OpenPrice,
		HighPrice:      ordered[0].HighPrice,
		LowPrice:       ordered[0].LowPrice,
		ClosePrice:     ordered[len(ordered)-1].ClosePrice,
		LastTickMS:     ordered[len(ordered)-1].LastTickMS,
		IsFinal:        isFinal,
		Source:         "rollup",
		Reason:         reason,
		UpdatedAtMS:    updatedAtMS,
		VolumeUnit:     ordered[0].VolumeUnit,
		QuoteUnit:      ordered[0].QuoteUnit,
	}
	for _, bar := range ordered {
		if bar.HighPrice > rollup.HighPrice {
			rollup.HighPrice = bar.HighPrice
		}
		if bar.LowPrice < rollup.LowPrice {
			rollup.LowPrice = bar.LowPrice
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
