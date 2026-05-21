package aggregator

import (
	"sort"
	"strings"

	"crypto-ticket/internal/market"
	"crypto-ticket/internal/timeframe"
)

type Result struct {
	LiveBars  []market.Bar
	FinalBars []market.Bar
}

type Engine struct {
	timeframes []string
	states     map[stateKey]rollingState
}

type stateKey struct {
	exchange  string
	symbol    string
	timeframe string
}

type rollingState struct {
	bar market.Bar
}

func NewEngine(frames []string) *Engine {
	if len(frames) == 0 {
		frames = timeframe.Order
	}
	normalized := make([]string, 0, len(frames))
	seen := map[string]bool{}
	for _, tf := range frames {
		tf = timeframe.MustNormalize(tf)
		if seen[tf] {
			continue
		}
		seen[tf] = true
		normalized = append(normalized, tf)
	}
	sort.Slice(normalized, func(i, j int) bool {
		return timeframe.Index(normalized[i]) < timeframe.Index(normalized[j])
	})
	return &Engine{
		timeframes: normalized,
		states:     make(map[stateKey]rollingState),
	}
}

func (e *Engine) OnTick(tick market.Tick) Result {
	tick.Exchange = strings.ToLower(strings.TrimSpace(tick.Exchange))
	tick.Symbol = strings.ToUpper(strings.TrimSpace(tick.Symbol))
	if tick.EventType == "" {
		tick.EventType = "trade"
	}
	if tick.Source == "" {
		tick.Source = "ws"
	}
	if tick.RecvMS == 0 {
		tick.RecvMS = market.NowMS()
	}
	if tick.Exchange == "" || tick.Symbol == "" || tick.TsMS <= 0 || tick.Price <= 0 {
		return Result{}
	}

	result := Result{}
	for _, tf := range e.timeframes {
		live, finals := e.applyTickToTimeframe(tick, tf)
		result.FinalBars = append(result.FinalBars, finals...)
		if live != nil {
			result.LiveBars = append(result.LiveBars, *live)
		}
	}
	return result
}

func (e *Engine) CloseDue(nowMS int64, graceMS int64) Result {
	cutoff := nowMS - maxInt64(graceMS, 0)
	keys := make([]stateKey, 0, len(e.states))
	for key := range e.states {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if timeframe.Index(keys[i].timeframe) != timeframe.Index(keys[j].timeframe) {
			return timeframe.Index(keys[i].timeframe) < timeframe.Index(keys[j].timeframe)
		}
		if keys[i].exchange != keys[j].exchange {
			return keys[i].exchange < keys[j].exchange
		}
		return keys[i].symbol < keys[j].symbol
	})

	result := Result{}
	for _, key := range keys {
		state := e.states[key]
		if state.bar.IsFinal || state.bar.EndMS > cutoff {
			continue
		}
		bar := state.bar
		bar.IsFinal = true
		bar.Reason = "close"
		bar.UpdatedAtMS = nowMS
		state.bar = bar
		e.states[key] = state
		result.FinalBars = append(result.FinalBars, bar)
	}
	return result
}

func (e *Engine) applyTickToTimeframe(tick market.Tick, tf string) (*market.Bar, []market.Bar) {
	key := stateKey{exchange: tick.Exchange, symbol: tick.Symbol, timeframe: tf}
	start := timeframe.FloorStartMS(tick.TsMS, tf)
	end := timeframe.EndMS(start, tf)
	state, ok := e.states[key]
	if !ok {
		bar := barFromTick(tick, tf, start, end)
		e.states[key] = rollingState{bar: bar}
		return &bar, nil
	}

	if start < state.bar.StartMS {
		return nil, nil
	}

	if start == state.bar.StartMS {
		if state.bar.IsFinal {
			return nil, nil
		}
		bar := updateBarWithTick(state.bar, tick)
		e.states[key] = rollingState{bar: bar}
		return &bar, nil
	}

	finals := make([]market.Bar, 0, 1)
	if !state.bar.IsFinal {
		final := state.bar
		final.IsFinal = true
		final.Reason = "close"
		final.UpdatedAtMS = tick.RecvMS
		finals = append(finals, final)
	}
	finals = append(finals, fillGaps(state.bar, start)...)
	newBar := barFromTick(tick, tf, start, end)
	e.states[key] = rollingState{bar: newBar}
	return &newBar, finals
}

func barFromTick(tick market.Tick, tf string, startMS int64, endMS int64) market.Bar {
	return market.Bar{
		Exchange:    tick.Exchange,
		Symbol:      tick.Symbol,
		Timeframe:   tf,
		StartMS:     startMS,
		EndMS:       endMS,
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

func updateBarWithTick(bar market.Bar, tick market.Tick) market.Bar {
	if tick.Price > bar.HighPrice {
		bar.HighPrice = tick.Price
	}
	if tick.Price < bar.LowPrice {
		bar.LowPrice = tick.Price
	}
	bar.ClosePrice = tick.Price
	bar.Volume += tick.Size
	bar.QuoteVolume += tick.Price * tick.Size
	bar.TradeCount++
	if tick.TsMS > bar.LastTickMS {
		bar.LastTickMS = tick.TsMS
	}
	bar.UpdatedAtMS = tick.RecvMS
	bar.Reason = "update"
	return bar
}

func fillGaps(previous market.Bar, nextStartMS int64) []market.Bar {
	var gaps []market.Bar
	gapStart := timeframe.NextStartMS(previous.StartMS, previous.Timeframe)
	for gapStart < nextStartMS {
		gapEnd := timeframe.EndMS(gapStart, previous.Timeframe)
		gaps = append(gaps, market.Bar{
			Exchange:    previous.Exchange,
			Symbol:      previous.Symbol,
			Timeframe:   previous.Timeframe,
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
			UpdatedAtMS: market.NowMS(),
		})
		gapStart = timeframe.NextStartMS(gapStart, previous.Timeframe)
	}
	return gaps
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
