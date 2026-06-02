package aggregator

import (
	"sort"
	"strings"

	"crypto-ticket/internal/market"
	"crypto-ticket/internal/timeframe"
)

const OneMinute = "1m"

func ValidTick(tick market.Tick) bool {
	return strings.TrimSpace(tick.Exchange) != "" &&
		strings.TrimSpace(tick.Symbol) != "" &&
		tick.TsMS > 0 &&
		tick.Price > 0
}

func OneMinuteStartMS(tsMS int64) int64 {
	return timeframe.FloorStartMS(tsMS, OneMinute)
}

func NewOneMinuteBar(tick market.Tick) market.Bar {
	startMS := OneMinuteStartMS(tick.TsMS)
	return market.Bar{
		Exchange:    strings.ToLower(strings.TrimSpace(tick.Exchange)),
		Symbol:      strings.ToUpper(strings.TrimSpace(tick.Symbol)),
		Timeframe:   OneMinute,
		StartMS:     startMS,
		EndMS:       timeframe.EndMS(startMS, OneMinute),
		OpenPrice:   tick.Price,
		HighPrice:   tick.Price,
		LowPrice:    tick.Price,
		ClosePrice:  tick.Price,
		Volume:      tick.Size,
		QuoteVolume: tick.Price * tick.Size,
		TradeCount:  1,
		LastTickMS:  tick.TsMS,
		IsFinal:     false,
		Source:      "aggregator",
		Reason:      "update",
		UpdatedAtMS: tick.RecvMS,
	}
}

func ApplyTick(bar market.Bar, tick market.Tick) market.Bar {
	if bar.OpenPrice <= 0 {
		return NewOneMinuteBar(tick)
	}
	if tick.Price > bar.HighPrice {
		bar.HighPrice = tick.Price
	}
	if tick.Price < bar.LowPrice {
		bar.LowPrice = tick.Price
	}
	if tick.TsMS >= bar.LastTickMS {
		bar.ClosePrice = tick.Price
		bar.LastTickMS = tick.TsMS
	}
	bar.Volume += tick.Size
	bar.QuoteVolume += tick.Price * tick.Size
	bar.TradeCount++
	bar.UpdatedAtMS = tick.RecvMS
	if bar.IsFinal {
		bar.Reason = "late_correction"
	} else {
		bar.Reason = "update"
	}
	bar.Source = "aggregator"
	return bar
}

func FinalizeBar(bar market.Bar, nowMS int64, reason string) market.Bar {
	bar.IsFinal = true
	bar.Source = "aggregator"
	bar.Reason = reason
	bar.UpdatedAtMS = nowMS
	return bar
}

func GapBars(previous market.Bar, nextStartMS int64, nowMS int64) []market.Bar {
	var gaps []market.Bar
	gapStart := timeframe.NextStartMS(previous.StartMS, OneMinute)
	for gapStart < nextStartMS {
		gapEnd := timeframe.EndMS(gapStart, OneMinute)
		gaps = append(gaps, market.Bar{
			Exchange:    previous.Exchange,
			Symbol:      previous.Symbol,
			Timeframe:   OneMinute,
			StartMS:     gapStart,
			EndMS:       gapEnd,
			OpenPrice:   previous.ClosePrice,
			HighPrice:   previous.ClosePrice,
			LowPrice:    previous.ClosePrice,
			ClosePrice:  previous.ClosePrice,
			Volume:      0,
			QuoteVolume: 0,
			TradeCount:  0,
			LastTickMS:  previous.EndMS,
			IsFinal:     true,
			Source:      "aggregator",
			Reason:      "gap",
			UpdatedAtMS: nowMS,
		})
		gapStart = timeframe.NextStartMS(gapStart, OneMinute)
	}
	return gaps
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
		Exchange:    strings.ToLower(ordered[0].Exchange),
		Symbol:      strings.ToUpper(ordered[0].Symbol),
		Timeframe:   tf,
		StartMS:     startMS,
		EndMS:       timeframe.EndMS(startMS, tf),
		OpenPrice:   ordered[0].OpenPrice,
		HighPrice:   ordered[0].HighPrice,
		LowPrice:    ordered[0].LowPrice,
		ClosePrice:  ordered[len(ordered)-1].ClosePrice,
		LastTickMS:  ordered[len(ordered)-1].LastTickMS,
		IsFinal:     isFinal,
		Source:      "rollup",
		Reason:      reason,
		UpdatedAtMS: updatedAtMS,
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
		rollup.TradeCount += bar.TradeCount
		if bar.LastTickMS > rollup.LastTickMS {
			rollup.LastTickMS = bar.LastTickMS
		}
	}
	return &rollup
}
