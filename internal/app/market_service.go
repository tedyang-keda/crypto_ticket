package app

import (
	"context"
	"strings"
	"sync"

	"crypto-ticket/internal/aggregator"
	"crypto-ticket/internal/market"
	"crypto-ticket/internal/realtime"
	"crypto-ticket/internal/storage"
	"crypto-ticket/internal/timeframe"
)

const (
	defaultRecentLimit      = 300
	officialOneMinuteSource = "exchange_kline"
)

type MarketService struct {
	store     storage.HistoricalStore
	hub       *realtime.Hub
	mu        sync.RWMutex
	frames    []string
	recentMax int
	liveBars  map[string]market.Bar
}

func NewMarketService(store storage.HistoricalStore, hub *realtime.Hub, frames []string, recentLimit int) *MarketService {
	if recentLimit <= 0 {
		recentLimit = defaultRecentLimit
	}
	normalized := normalizeFrames(frames)
	return &MarketService{
		store:     store,
		hub:       hub,
		frames:    normalized,
		recentMax: recentLimit,
		liveBars:  make(map[string]market.Bar),
	}
}

func (s *MarketService) IngestKline(ctx context.Context, bar market.Bar) error {
	bar = normalizeBar(bar)
	if !validBar(bar) {
		return nil
	}
	enriched, err := s.enrichBar(ctx, bar)
	if err != nil {
		return err
	}

	s.mu.Lock()
	liveKey := barKey(enriched.Exchange, enriched.Symbol, enriched.Timeframe)
	if enriched.IsFinal {
		delete(s.liveBars, liveKey)
	} else {
		s.liveBars[liveKey] = enriched
	}
	s.mu.Unlock()

	if enriched.IsFinal {
		if err := s.persistFinalBars(ctx, []market.Bar{enriched}); err != nil {
			return err
		}
		return nil
	}
	s.publishBar(enriched)
	if enriched.Timeframe == aggregator.OneMinute {
		if err := s.publishLiveRollups(ctx, enriched); err != nil {
			return err
		}
	}
	return nil
}

func normalizeBar(bar market.Bar) market.Bar {
	bar.Exchange = strings.ToLower(strings.TrimSpace(bar.Exchange))
	bar.Symbol = strings.ToUpper(strings.TrimSpace(bar.Symbol))
	bar.Timeframe = timeframe.MustNormalize(bar.Timeframe)
	bar.MarginType = normalizeMarginType(bar.MarginType)
	if bar.EndMS == 0 && bar.StartMS > 0 {
		bar.EndMS = timeframe.EndMS(bar.StartMS, bar.Timeframe)
	}
	if bar.LastTickMS == 0 {
		bar.LastTickMS = bar.EndMS
	}
	if bar.UpdatedAtMS == 0 {
		bar.UpdatedAtMS = market.NowMS()
	}
	if bar.Source == "" {
		bar.Source = officialOneMinuteSource
	}
	if bar.Reason == "" {
		if bar.IsFinal {
			bar.Reason = "final"
		} else {
			bar.Reason = "update"
		}
	}
	return market.DecorateBar(bar)
}

func normalizeMarginType(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	switch value {
	case "coinmargin", "coin_margin", "coin-m", "coin_futures":
		return "coinmargin"
	case "umargin", "u_margin", "usdsmargin", "um_futures", "linear":
		return "umargin"
	default:
		return value
	}
}

func validBar(bar market.Bar) bool {
	return bar.Exchange != "" &&
		bar.Symbol != "" &&
		bar.Timeframe != "" &&
		bar.StartMS > 0 &&
		bar.EndMS >= bar.StartMS &&
		bar.OpenPrice > 0 &&
		bar.HighPrice > 0 &&
		bar.LowPrice > 0 &&
		bar.ClosePrice > 0
}

func (s *MarketService) enrichBar(ctx context.Context, bar market.Bar) (market.Bar, error) {
	previousClose, err := s.previousClose(ctx, bar)
	if err != nil {
		return bar, err
	}
	return aggregator.ApplyDerived(bar, previousClose), nil
}

func (s *MarketService) previousClose(ctx context.Context, bar market.Bar) (float64, error) {
	previousStart := bar.StartMS - timeframe.DurationMS(bar.Timeframe)
	if previousStart < 0 {
		return 0, nil
	}
	bars, err := s.store.BarsInRange(ctx, bar.Exchange, bar.Symbol, bar.Timeframe, previousStart, previousStart)
	if err != nil {
		return 0, err
	}
	if len(bars) > 0 {
		return bars[len(bars)-1].ClosePrice, nil
	}
	if bar.Timeframe == aggregator.OneMinute {
		return 0, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	live, ok := s.liveBars[barKey(bar.Exchange, bar.Symbol, bar.Timeframe)]
	if ok && live.StartMS == previousStart && live.IsFinal {
		return live.ClosePrice, nil
	}
	return 0, nil
}

func (s *MarketService) persistFinalBars(ctx context.Context, bars []market.Bar) error {
	if len(bars) == 0 {
		return nil
	}
	for i := range bars {
		bars[i] = market.DecorateBar(bars[i])
	}
	if err := s.store.UpsertBars(ctx, bars); err != nil {
		return err
	}
	for _, bar := range bars {
		s.publishBar(bar)
	}
	return s.rollupFinalOneMinuteBars(ctx, bars)
}

func (s *MarketService) rollupFinalOneMinuteBars(ctx context.Context, bars []market.Bar) error {
	nowMS := market.NowMS()
	for _, bar := range bars {
		if bar.Timeframe != aggregator.OneMinute || !bar.IsFinal {
			continue
		}
		for _, tf := range s.frames {
			if tf == aggregator.OneMinute {
				continue
			}
			targetStart := timeframe.FloorStartMS(bar.StartMS, tf)
			targetEnd := timeframe.EndMS(targetStart, tf)
			if bar.EndMS < targetEnd {
				continue
			}
			oneMinuteBars, err := s.store.BarsInRange(ctx, bar.Exchange, bar.Symbol, aggregator.OneMinute, targetStart, targetEnd)
			if err != nil {
				return err
			}
			if len(oneMinuteBars) == 0 || oneMinuteBars[len(oneMinuteBars)-1].EndMS < targetEnd {
				continue
			}
			rollup := aggregator.RollupBars(tf, oneMinuteBars, true, "rollup", nowMS)
			if rollup == nil {
				continue
			}
			enriched, err := s.enrichBar(ctx, *rollup)
			if err != nil {
				return err
			}
			if err := s.store.UpsertBars(ctx, []market.Bar{enriched}); err != nil {
				return err
			}
			s.publishBar(enriched)
		}
	}
	return nil
}

func (s *MarketService) publishLiveRollups(ctx context.Context, oneMinute market.Bar) error {
	nowMS := market.NowMS()
	for _, tf := range s.frames {
		if tf == aggregator.OneMinute {
			continue
		}
		if !s.hub.HasSubscribers(realtime.KlineChannel(oneMinute.Exchange, oneMinute.Symbol, tf)) {
			continue
		}
		targetStart := timeframe.FloorStartMS(oneMinute.StartMS, tf)
		partialInputs, err := s.store.BarsInRange(ctx, oneMinute.Exchange, oneMinute.Symbol, aggregator.OneMinute, targetStart, oneMinute.StartMS-1)
		if err != nil {
			return err
		}
		partialInputs = append(partialInputs, oneMinute)
		rollup := aggregator.RollupBars(tf, partialInputs, false, "live", nowMS)
		if rollup == nil {
			continue
		}
		enriched, err := s.enrichBar(ctx, *rollup)
		if err != nil {
			return err
		}
		s.publishBar(enriched)
	}
	return nil
}

func (s *MarketService) publishBar(bar market.Bar) {
	barCopy := market.DecorateBar(bar)
	s.hub.Publish(market.Event{
		Type:      "kline",
		Exchange:  barCopy.Exchange,
		Symbol:    barCopy.Symbol,
		Timeframe: barCopy.Timeframe,
		Bar:       &barCopy,
	})
	if barCopy.Timeframe != aggregator.OneMinute {
		return
	}
	s.hub.Publish(market.Event{
		Type:     "ticker",
		Exchange: barCopy.Exchange,
		Symbol:   barCopy.Symbol,
		Tick:     tickFromBar(barCopy),
	})
}

func (s *MarketService) LatestTick(ctx context.Context, exchange string, symbol string) (*market.Tick, error) {
	exchange = strings.ToLower(strings.TrimSpace(exchange))
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	s.mu.RLock()
	live, ok := s.liveBars[barKey(exchange, symbol, aggregator.OneMinute)]
	s.mu.RUnlock()
	if ok {
		return tickFromBar(live), nil
	}
	bars, err := s.store.RecentBars(ctx, market.KlineQuery{Exchange: exchange, Symbol: symbol, Timeframe: aggregator.OneMinute, Limit: 1})
	if err != nil || len(bars) == 0 {
		return nil, err
	}
	return tickFromBar(bars[len(bars)-1]), nil
}

func tickFromBar(bar market.Bar) *market.Tick {
	return &market.Tick{
		Exchange:  bar.Exchange,
		Symbol:    bar.Symbol,
		TsMS:      bar.LastTickMS,
		Price:     bar.ClosePrice,
		Size:      bar.Volume,
		EventType: "kline",
		Source:    bar.Source,
		RecvMS:    bar.UpdatedAtMS,
	}
}

func (s *MarketService) Klines(ctx context.Context, query market.KlineQuery) ([]market.Bar, error) {
	query.Exchange = strings.ToLower(query.Exchange)
	query.Symbol = strings.ToUpper(query.Symbol)
	query.Timeframe = timeframe.MustNormalize(query.Timeframe)
	if query.Limit <= 0 {
		query.Limit = defaultRecentLimit
	}
	if query.Limit > 1000 {
		query.Limit = 1000
	}

	bars, err := s.store.RecentBars(ctx, query)
	if err != nil {
		return nil, err
	}
	for i := range bars {
		bars[i] = market.DecorateBar(bars[i])
	}
	if !query.IncludeLive {
		return trimBars(bars, query.Limit), nil
	}
	live := s.liveOneMinute(query.Exchange, query.Symbol)
	if live == nil {
		return trimBars(bars, query.Limit), nil
	}
	if query.Timeframe == aggregator.OneMinute {
		return trimBars(mergeLiveBar(bars, live), query.Limit+1), nil
	}

	partialStart := timeframe.FloorStartMS(live.StartMS, query.Timeframe)
	partialInputs, err := s.store.BarsInRange(ctx, query.Exchange, query.Symbol, aggregator.OneMinute, partialStart, live.StartMS-1)
	if err != nil {
		return nil, err
	}
	partialInputs = append(partialInputs, *live)
	partial := aggregator.RollupBars(query.Timeframe, partialInputs, false, "live", market.NowMS())
	if partial == nil {
		return trimBars(bars, query.Limit), nil
	}
	enriched, err := s.enrichBar(ctx, *partial)
	if err != nil {
		return nil, err
	}
	return trimBars(mergeLiveBar(bars, &enriched), query.Limit+1), nil
}

func (s *MarketService) liveOneMinute(exchange string, symbol string) *market.Bar {
	s.mu.RLock()
	defer s.mu.RUnlock()
	live, ok := s.liveBars[barKey(exchange, symbol, aggregator.OneMinute)]
	if !ok {
		return nil
	}
	live = market.DecorateBar(live)
	return &live
}

func (s *MarketService) ListSymbols(ctx context.Context, exchange string, activeOnly *bool) ([]market.SymbolInfo, error) {
	return s.store.ListSymbols(ctx, strings.ToLower(exchange), activeOnly)
}

func mergeLiveBar(bars []market.Bar, live *market.Bar) []market.Bar {
	if live == nil {
		return bars
	}
	for i := range bars {
		if bars[i].StartMS == live.StartMS {
			bars[i] = *live
			return bars
		}
	}
	if len(bars) == 0 || live.StartMS > bars[len(bars)-1].StartMS {
		return append(bars, *live)
	}
	return bars
}

func trimBars(bars []market.Bar, limit int) []market.Bar {
	if limit <= 0 || len(bars) <= limit {
		return bars
	}
	return bars[len(bars)-limit:]
}

func normalizeFrames(frames []string) []string {
	if len(frames) == 0 {
		frames = timeframe.Order
	}
	seen := map[string]bool{}
	out := make([]string, 0, len(frames))
	for _, tf := range frames {
		tf = timeframe.MustNormalize(tf)
		if seen[tf] {
			continue
		}
		seen[tf] = true
		out = append(out, tf)
	}
	return out
}

func barKey(exchange string, symbol string, tf string) string {
	return strings.ToLower(exchange) + ":" + strings.ToUpper(symbol) + ":" + timeframe.MustNormalize(tf)
}
