package app

import (
	"context"
	"strings"
	"sync"
	"time"

	"crypto-ticket/internal/aggregator"
	"crypto-ticket/internal/cache"
	"crypto-ticket/internal/market"
	"crypto-ticket/internal/realtime"
	"crypto-ticket/internal/storage"
	"crypto-ticket/internal/timeframe"
)

const (
	defaultRecentLimit       = 300
	defaultDedupeTTL         = 10 * time.Minute
	defaultRollingTTL        = 10 * time.Minute
	defaultAllowedLatenessMS = int64(30_000)
	defaultCleanupGraceMS    = int64(60_000)
)

type MarketService struct {
	cache     cache.MarketCache
	store     storage.HistoricalStore
	hub       *realtime.Hub
	mu        sync.Mutex
	frames    []string
	recentMax int
}

func NewMarketService(cache cache.MarketCache, store storage.HistoricalStore, hub *realtime.Hub, frames []string, recentLimit int) *MarketService {
	if recentLimit <= 0 {
		recentLimit = defaultRecentLimit
	}
	normalized := normalizeFrames(frames)
	return &MarketService{
		cache:     cache,
		store:     store,
		hub:       hub,
		frames:    normalized,
		recentMax: recentLimit,
	}
}

func (s *MarketService) IngestTick(ctx context.Context, tick market.Tick) error {
	tick = normalizeTick(tick, "api")
	tick, err := s.publishNormalizedTick(ctx, tick)
	if err != nil {
		return err
	}
	return s.aggregateNormalizedTick(ctx, tick)
}

func (s *MarketService) PublishTick(ctx context.Context, tick market.Tick) (market.Tick, error) {
	return s.publishNormalizedTick(ctx, normalizeTick(tick, "ws"))
}

func (s *MarketService) AggregateTick(ctx context.Context, tick market.Tick) error {
	return s.aggregateNormalizedTick(ctx, normalizeTick(tick, "stream"))
}

func (s *MarketService) publishNormalizedTick(ctx context.Context, tick market.Tick) (market.Tick, error) {
	if err := s.cache.SetLatestTick(ctx, tick); err != nil {
		return tick, err
	}
	s.hub.Publish(market.Event{
		Type:     "ticker",
		Exchange: tick.Exchange,
		Symbol:   tick.Symbol,
		Tick:     &tick,
	})
	return tick, nil
}

func (s *MarketService) aggregateNormalizedTick(ctx context.Context, tick market.Tick) error {
	if !aggregator.ValidTick(tick) {
		return nil
	}
	windowStart := aggregator.OneMinuteStartMS(tick.TsMS)
	seen, err := s.cache.MarkTickSeen(ctx, tick, windowStart, defaultDedupeTTL)
	if err != nil {
		return err
	}
	if !seen {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	nowMS := market.NowMS()
	if err := s.closePreviousLiveWindow(ctx, tick.Exchange, tick.Symbol, windowStart, nowMS); err != nil {
		return err
	}
	bar, err := s.loadOneMinuteBar(ctx, tick, windowStart)
	if err != nil {
		return err
	}
	if bar == nil {
		newBar := aggregator.NewOneMinuteBar(tick)
		bar = &newBar
	} else {
		updated := aggregator.ApplyTick(*bar, tick)
		bar = &updated
	}

	if err := s.saveRollingBar(ctx, *bar); err != nil {
		return err
	}
	if bar.IsFinal {
		return s.persistFinalBars(ctx, []market.Bar{*bar})
	}
	return s.publishLive1m(ctx, *bar)
}

func normalizeTick(tick market.Tick, defaultSource string) market.Tick {
	tick.Exchange = strings.ToLower(strings.TrimSpace(tick.Exchange))
	tick.Symbol = strings.ToUpper(strings.TrimSpace(tick.Symbol))
	if tick.RecvMS == 0 {
		tick.RecvMS = market.NowMS()
	}
	if tick.Source == "" {
		tick.Source = defaultSource
	}
	if tick.EventType == "" {
		tick.EventType = "trade"
	}
	return tick
}

func (s *MarketService) loadOneMinuteBar(ctx context.Context, tick market.Tick, startMS int64) (*market.Bar, error) {
	bar, err := s.cache.GetRollingBar(ctx, tick.Exchange, tick.Symbol, startMS)
	if err != nil || bar != nil {
		return bar, err
	}
	bars, err := s.store.BarsInRange(ctx, tick.Exchange, tick.Symbol, aggregator.OneMinute, startMS, startMS)
	if err != nil {
		return nil, err
	}
	if len(bars) == 0 {
		return nil, nil
	}
	bar = &bars[0]
	return bar, nil
}

func (s *MarketService) saveRollingBar(ctx context.Context, bar market.Bar) error {
	actionAtMS := bar.EndMS
	if bar.IsFinal {
		actionAtMS = bar.EndMS + defaultAllowedLatenessMS + defaultCleanupGraceMS
	}
	return s.cache.PutRollingBar(ctx, bar, actionAtMS, defaultRollingTTL)
}

func (s *MarketService) publishLive1m(ctx context.Context, bar market.Bar) error {
	live, err := s.cache.GetLive1mBar(ctx, bar.Exchange, bar.Symbol)
	if err != nil {
		return err
	}
	if live != nil && live.StartMS > bar.StartMS {
		return nil
	}
	if err := s.cache.SetLive1mBar(ctx, bar); err != nil {
		return err
	}
	s.publishBar(bar)
	return nil
}

func (s *MarketService) closePreviousLiveWindow(ctx context.Context, exchange string, symbol string, nextStartMS int64, nowMS int64) error {
	live, err := s.cache.GetLive1mBar(ctx, exchange, symbol)
	if err != nil || live == nil || live.StartMS >= nextStartMS {
		return err
	}
	final := aggregator.FinalizeBar(*live, nowMS, "close")
	finals := []market.Bar{final}
	finals = append(finals, aggregator.GapBars(final, nextStartMS, nowMS)...)
	if err := s.cache.DeleteRollingBar(ctx, final.Exchange, final.Symbol, final.StartMS); err != nil {
		return err
	}
	for _, gap := range finals[1:] {
		if err := s.cache.PutRollingBar(ctx, gap, gap.EndMS+defaultAllowedLatenessMS+defaultCleanupGraceMS, defaultRollingTTL); err != nil {
			return err
		}
	}
	return s.persistFinalBars(ctx, finals)
}

func (s *MarketService) CloseDue(ctx context.Context, nowMS int64, graceMS int64) error {
	if graceMS < 0 {
		graceMS = 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoffMS := nowMS - graceMS
	for {
		bars, err := s.cache.DueRollingBars(ctx, cutoffMS, 500)
		if err != nil {
			return err
		}
		if len(bars) == 0 {
			return nil
		}
		for _, bar := range bars {
			if bar.IsFinal {
				if err := s.cache.DeleteRollingBar(ctx, bar.Exchange, bar.Symbol, bar.StartMS); err != nil {
					return err
				}
				continue
			}
			final := aggregator.FinalizeBar(bar, nowMS, "close")
			if err := s.saveRollingBar(ctx, final); err != nil {
				return err
			}
			if err := s.persistFinalBars(ctx, []market.Bar{final}); err != nil {
				return err
			}
		}
		if len(bars) < 500 {
			return nil
		}
	}
}

func (s *MarketService) persistFinalBars(ctx context.Context, bars []market.Bar) error {
	if len(bars) == 0 {
		return nil
	}
	if err := s.store.UpsertBars(ctx, bars); err != nil {
		return err
	}
	if err := s.cache.PutFinalBars(ctx, bars, s.recentMax); err != nil {
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
			if targetEnd > nowMS {
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
			if err := s.store.UpsertBars(ctx, []market.Bar{*rollup}); err != nil {
				return err
			}
			if err := s.cache.PutFinalBars(ctx, []market.Bar{*rollup}, s.recentMax); err != nil {
				return err
			}
			s.publishBar(*rollup)
		}
	}
	return nil
}

func (s *MarketService) publishBar(bar market.Bar) {
	barCopy := bar
	s.hub.Publish(market.Event{
		Type:      "kline",
		Exchange:  barCopy.Exchange,
		Symbol:    barCopy.Symbol,
		Timeframe: barCopy.Timeframe,
		Bar:       &barCopy,
	})
}

func (s *MarketService) LatestTick(ctx context.Context, exchange string, symbol string) (*market.Tick, error) {
	return s.cache.GetLatestTick(ctx, strings.ToLower(exchange), strings.ToUpper(symbol))
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
	if !query.IncludeLive {
		return trimBars(bars, query.Limit), nil
	}
	live, err := s.cache.GetLive1mBar(ctx, query.Exchange, query.Symbol)
	if err != nil || live == nil {
		return trimBars(bars, query.Limit), err
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
	return trimBars(mergeLiveBar(bars, partial), query.Limit+1), nil
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
