package app

import (
	"context"
	"strings"
	"sync"

	"crypto-ticket/internal/aggregator"
	"crypto-ticket/internal/cache"
	"crypto-ticket/internal/market"
	"crypto-ticket/internal/realtime"
	"crypto-ticket/internal/storage"
)

type MarketService struct {
	cache             cache.MarketCache
	store             storage.HistoricalStore
	hub               *realtime.Hub
	engineMu          sync.Mutex
	engine            *aggregator.Engine
	liveMu            sync.Mutex
	lastLiveWriteMS   map[string]int64
	liveWriteInterval int64
	recentLimit       int
}

func NewMarketService(cache cache.MarketCache, store storage.HistoricalStore, hub *realtime.Hub, frames []string, recentLimit int) *MarketService {
	if recentLimit <= 0 {
		recentLimit = 300
	}
	return &MarketService{
		cache:             cache,
		store:             store,
		hub:               hub,
		engine:            aggregator.NewEngine(frames),
		lastLiveWriteMS:   make(map[string]int64),
		liveWriteInterval: 250,
		recentLimit:       recentLimit,
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
	s.engineMu.Lock()
	result := s.engine.OnTick(tick)
	s.engineMu.Unlock()

	return s.handleAggregationResult(ctx, result)
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

func (s *MarketService) CloseDue(ctx context.Context, nowMS int64, graceMS int64) error {
	s.engineMu.Lock()
	result := s.engine.CloseDue(nowMS, graceMS)
	s.engineMu.Unlock()
	return s.handleAggregationResult(ctx, result)
}

func (s *MarketService) handleAggregationResult(ctx context.Context, result aggregator.Result) error {
	for _, bar := range result.LiveBars {
		barCopy := bar
		if !s.shouldWriteLiveBar(barCopy) {
			continue
		}
		if err := s.cache.SetLiveBar(ctx, barCopy); err != nil {
			return err
		}
		s.hub.Publish(market.Event{
			Type:      "kline",
			Exchange:  barCopy.Exchange,
			Symbol:    barCopy.Symbol,
			Timeframe: barCopy.Timeframe,
			Bar:       &barCopy,
		})
	}
	if len(result.FinalBars) > 0 {
		if err := s.store.UpsertBars(ctx, result.FinalBars); err != nil {
			return err
		}
		if err := s.cache.PutFinalBars(ctx, result.FinalBars, s.recentLimit); err != nil {
			return err
		}
		for _, bar := range result.FinalBars {
			barCopy := bar
			s.hub.Publish(market.Event{
				Type:      "kline",
				Exchange:  barCopy.Exchange,
				Symbol:    barCopy.Symbol,
				Timeframe: barCopy.Timeframe,
				Bar:       &barCopy,
			})
		}
	}
	return nil
}

func (s *MarketService) shouldWriteLiveBar(bar market.Bar) bool {
	key := strings.ToLower(bar.Exchange) + ":" + strings.ToUpper(bar.Symbol) + ":" + bar.Timeframe
	nowMS := bar.UpdatedAtMS
	if nowMS == 0 {
		nowMS = market.NowMS()
	}
	s.liveMu.Lock()
	defer s.liveMu.Unlock()
	last := s.lastLiveWriteMS[key]
	if last != 0 && nowMS-last < s.liveWriteInterval {
		return false
	}
	s.lastLiveWriteMS[key] = nowMS
	return true
}

func (s *MarketService) LatestTick(ctx context.Context, exchange string, symbol string) (*market.Tick, error) {
	return s.cache.GetLatestTick(ctx, strings.ToLower(exchange), strings.ToUpper(symbol))
}

func (s *MarketService) Klines(ctx context.Context, query market.KlineQuery) ([]market.Bar, error) {
	query.Exchange = strings.ToLower(query.Exchange)
	query.Symbol = strings.ToUpper(query.Symbol)
	if query.Limit <= 0 {
		query.Limit = 300
	}
	if query.Limit > 1000 {
		query.Limit = 1000
	}

	bars, err := s.cache.RecentBars(ctx, query)
	if err != nil {
		return nil, err
	}
	if len(bars) < query.Limit {
		dbBars, err := s.store.RecentBars(ctx, query)
		if err != nil {
			return nil, err
		}
		if len(dbBars) > 0 {
			bars = dbBars
			_ = s.cache.PutFinalBars(ctx, dbBars, s.recentLimit)
		}
	}
	if query.IncludeLive {
		live, err := s.cache.GetLiveBar(ctx, query.Exchange, query.Symbol, query.Timeframe)
		if err != nil {
			return nil, err
		}
		bars = mergeLiveBar(bars, live)
	}
	if len(bars) > query.Limit+1 {
		bars = bars[len(bars)-query.Limit-1:]
	}
	return bars, nil
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
