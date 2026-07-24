package app

import (
	"context"
	"sort"
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
	store          storage.HistoricalStore
	hub            *realtime.Hub
	mu             sync.RWMutex
	frames         []string
	recentMax      int
	liveBars       map[string]market.Bar
	finalObservers []FinalBarObserver
	corpGuard      CorporateActionGuard
}

type FinalBarObserver interface {
	ObserveFinalBar(ctx context.Context, bar market.Bar) error
}

// CorporateActionGuard reports, for a bar, whether it straddles an active
// corporate action so its spurious change can be suppressed (layer C). It is
// implemented by *corpaction.Registry; kept as an interface here to avoid a
// hard dependency.
type CorporateActionGuard interface {
	AssessBar(exchange string, symbol string, barStartMS int64, barEndMS int64, chgPct float64, nowMS int64) (liveRaw bool, neutralize bool)
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

// SetCorporateActionGuard attaches the real-time corporate-action guard (layer
// C). Optional; when unset bars pass through unmodified.
func (s *MarketService) SetCorporateActionGuard(guard CorporateActionGuard) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.corpGuard = guard
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
		if err := s.persistFinalBars(ctx, []market.Bar{enriched}, false); err != nil {
			return err
		}
		s.notifyFinalBarObservers(ctx, enriched)
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

func (s *MarketService) AddFinalBarObserver(observer FinalBarObserver) {
	if observer == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.finalObservers = append(s.finalObservers, observer)
}

func (s *MarketService) notifyFinalBarObservers(ctx context.Context, bar market.Bar) {
	s.mu.RLock()
	observers := append([]FinalBarObserver(nil), s.finalObservers...)
	s.mu.RUnlock()
	for _, observer := range observers {
		_ = observer.ObserveFinalBar(ctx, bar)
	}
}

func (s *MarketService) RepairFinalBars(ctx context.Context, bars []market.Bar) error {
	if len(bars) == 0 {
		return nil
	}
	ordered := append([]market.Bar(nil), bars...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Exchange != ordered[j].Exchange {
			return ordered[i].Exchange < ordered[j].Exchange
		}
		if ordered[i].Symbol != ordered[j].Symbol {
			return ordered[i].Symbol < ordered[j].Symbol
		}
		if ordered[i].Timeframe != ordered[j].Timeframe {
			return ordered[i].Timeframe < ordered[j].Timeframe
		}
		return ordered[i].StartMS < ordered[j].StartMS
	})
	for _, bar := range ordered {
		bar.IsFinal = true
		if bar.Source == "" {
			bar.Source = "rest"
		}
		if bar.Reason == "" {
			bar.Reason = "guardian_repair"
		}
		enriched, err := s.prepareFinalBar(ctx, bar)
		if err != nil {
			return err
		}
		if !validBar(enriched) {
			continue
		}
		s.mu.Lock()
		delete(s.liveBars, barKey(enriched.Exchange, enriched.Symbol, enriched.Timeframe))
		s.mu.Unlock()
		if err := s.persistFinalBars(ctx, []market.Bar{enriched}, true); err != nil {
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
	if mode, err := market.NormalizePriceMode(bar.PriceMode); err == nil {
		bar.PriceMode = mode
	} else {
		bar.PriceMode = market.PriceModeRaw
	}
	if bar.PriceMode == market.PriceModeRaw && bar.AdjustmentStatus == "" {
		bar.AdjustmentStatus = market.AdjustmentStatusRaw
	}
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
	bar = aggregator.ApplyDerived(bar, previousClose)
	return s.applyCorporateActionGuard(bar), nil
}

// applyCorporateActionGuard suppresses the spurious change on a bar that
// straddles an active corporate action and flags it live_raw, so the rebase
// discontinuity is not published or aggregated as real volatility.
func (s *MarketService) applyCorporateActionGuard(bar market.Bar) market.Bar {
	s.mu.RLock()
	guard := s.corpGuard
	s.mu.RUnlock()
	if guard == nil {
		return bar
	}
	liveRaw, neutralize := guard.AssessBar(bar.Exchange, bar.Symbol, bar.StartMS, bar.EndMS, bar.Chg, market.NowMS())
	if neutralize {
		bar.Chg = 0
		bar.PrevClose = 0
	}
	if liveRaw && bar.PriceMode == market.PriceModeRaw {
		bar.AdjustmentStatus = market.AdjustmentStatusLiveRaw
	}
	return market.DecorateBar(bar)
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

func (s *MarketService) prepareFinalBar(ctx context.Context, bar market.Bar) (market.Bar, error) {
	bar = normalizeBar(bar)
	if !validBar(bar) {
		return bar, nil
	}
	return s.enrichBar(ctx, bar)
}

func (s *MarketService) persistFinalBars(ctx context.Context, bars []market.Bar, repairRollups bool) error {
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
	return s.rollupFinalBars(ctx, bars, repairRollups)
}

func (s *MarketService) rollupFinalBars(ctx context.Context, bars []market.Bar, repairRollups bool) error {
	nowMS := market.NowMS()
	queue := append([]market.Bar(nil), bars...)
	for len(queue) > 0 {
		bar := queue[0]
		queue = queue[1:]
		if !bar.IsFinal {
			continue
		}
		for _, tf := range s.frames {
			if tf == aggregator.OneMinute {
				continue
			}
			if aggregator.RollupSourceTimeframe(tf) != bar.Timeframe {
				continue
			}
			targetStart := timeframe.FloorStartMS(bar.StartMS, tf)
			targetEnd := timeframe.EndMS(targetStart, tf)
			if !repairRollups && bar.EndMS < targetEnd {
				continue
			}
			sourceBars, err := s.store.BarsInRange(ctx, bar.Exchange, bar.Symbol, bar.Timeframe, targetStart, targetEnd)
			if err != nil {
				return err
			}
			if !sourceBarsComplete(sourceBars, bar.Timeframe, targetStart, targetEnd) {
				continue
			}
			rollup := aggregator.RollupBars(tf, sourceBars, true, "rollup", nowMS)
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
			queue = append(queue, enriched)
		}
	}
	return nil
}

func sourceBarsComplete(bars []market.Bar, sourceTF string, targetStart int64, targetEnd int64) bool {
	if len(bars) == 0 {
		return false
	}
	expectedStart := targetStart
	for _, bar := range bars {
		if !bar.IsFinal || bar.StartMS != expectedStart {
			return false
		}
		expectedStart = timeframe.NextStartMS(bar.StartMS, sourceTF)
	}
	return expectedStart > targetEnd && bars[len(bars)-1].EndMS >= targetEnd
}

func (s *MarketService) publishLiveRollups(ctx context.Context, oneMinute market.Bar) error {
	nowMS := market.NowMS()
	for _, tf := range timeframe.Order {
		if tf == aggregator.OneMinute {
			continue
		}
		if !s.hub.HasSubscribers(realtime.KlineChannel(oneMinute.Exchange, oneMinute.Symbol, tf)) {
			continue
		}
		enriched, err := s.buildLiveRollup(ctx, tf, oneMinute, nowMS)
		if err != nil {
			return err
		}
		if enriched != nil {
			s.publishBar(*enriched)
		}
	}
	return nil
}

func (s *MarketService) buildLiveRollup(ctx context.Context, target string, liveOneMinute market.Bar, nowMS int64) (*market.Bar, error) {
	target = timeframe.MustNormalize(target)
	if target == aggregator.OneMinute {
		bar := market.DecorateBar(liveOneMinute)
		return &bar, nil
	}
	source := aggregator.RollupSourceTimeframe(target)
	if source == "" {
		return nil, nil
	}

	var liveSource market.Bar
	if source == aggregator.OneMinute {
		liveSource = liveOneMinute
	} else {
		partial, err := s.buildLiveRollup(ctx, source, liveOneMinute, nowMS)
		if err != nil {
			return nil, err
		}
		if partial == nil {
			return nil, nil
		}
		liveSource = *partial
	}

	targetStart := timeframe.FloorStartMS(liveOneMinute.StartMS, target)
	partialInputs, err := s.store.BarsInRange(ctx, liveOneMinute.Exchange, liveOneMinute.Symbol, source, targetStart, liveSource.StartMS-1)
	if err != nil {
		return nil, err
	}
	partialInputs = append(partialInputs, liveSource)
	rollup := aggregator.RollupBars(target, partialInputs, false, "live", nowMS)
	if rollup == nil {
		return nil, nil
	}
	enriched, err := s.enrichBar(ctx, *rollup)
	if err != nil {
		return nil, err
	}
	return &enriched, nil
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

func (s *MarketService) LatestTick(ctx context.Context, exchange string, symbol string, priceMode string) (*market.Tick, error) {
	exchange = strings.ToLower(strings.TrimSpace(exchange))
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	if _, err := market.NormalizePriceMode(priceMode); err != nil {
		return nil, err
	}
	s.mu.RLock()
	live, ok := s.liveBars[barKey(exchange, symbol, aggregator.OneMinute)]
	s.mu.RUnlock()
	if ok {
		return tickFromBar(market.MarkBarAdjustmentStatus(live, market.PriceModeRaw, market.AdjustmentStatusRaw)), nil
	}
	bars, err := s.store.RecentBars(ctx, market.KlineQuery{Exchange: exchange, Symbol: symbol, Timeframe: aggregator.OneMinute, Limit: 1, PriceMode: market.PriceModeRaw})
	if err != nil || len(bars) == 0 {
		return nil, err
	}
	return tickFromBar(bars[len(bars)-1]), nil
}

func tickFromBar(bar market.Bar) *market.Tick {
	tick := &market.Tick{
		Exchange:         bar.Exchange,
		SourceMarket:     bar.SourceMarket,
		Symbol:           bar.Symbol,
		InstrumentType:   bar.InstrumentType,
		AssetClass:       bar.AssetClass,
		RuleType:         bar.RuleType,
		LifecyclePhase:   bar.LifecyclePhase,
		PriceMode:        bar.PriceMode,
		AdjustmentStatus: bar.AdjustmentStatus,
		TsMS:             bar.LastTickMS,
		Price:            bar.ClosePrice,
		Size:             bar.Volume,
		EventType:        "kline",
		Source:           bar.Source,
		RecvMS:           bar.UpdatedAtMS,
	}
	return tick
}

func (s *MarketService) Klines(ctx context.Context, query market.KlineQuery) ([]market.Bar, error) {
	query.Exchange = strings.ToLower(query.Exchange)
	query.Symbol = strings.ToUpper(query.Symbol)
	query.Timeframe = timeframe.MustNormalize(query.Timeframe)
	if _, err := market.NormalizePriceMode(query.PriceMode); err != nil {
		return nil, err
	}
	query.PriceMode = market.PriceModeRaw
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
		queryLive := market.MarkBarAdjustmentStatus(*live, market.PriceModeRaw, market.AdjustmentStatusRaw)
		return trimBars(mergeLiveBar(bars, &queryLive), query.Limit+1), nil
	}

	partial, err := s.buildLiveRollup(ctx, query.Timeframe, *live, market.NowMS())
	if err != nil {
		return nil, err
	}
	if partial == nil {
		return trimBars(bars, query.Limit), nil
	}
	queryPartial := market.MarkBarAdjustmentStatus(*partial, market.PriceModeRaw, market.AdjustmentStatusRaw)
	return trimBars(mergeLiveBar(bars, &queryPartial), query.Limit+1), nil
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
	requested := map[string]bool{}
	for _, tf := range frames {
		tf = timeframe.MustNormalize(tf)
		requested[tf] = true
		addRollupDependencies(tf, requested)
	}
	out := make([]string, 0, len(requested))
	for _, tf := range timeframe.Order {
		if !requested[tf] || seen[tf] {
			continue
		}
		seen[tf] = true
		out = append(out, tf)
	}
	return out
}

func addRollupDependencies(tf string, out map[string]bool) {
	source := aggregator.RollupSourceTimeframe(tf)
	if source == "" || source == aggregator.OneMinute {
		return
	}
	if out[source] {
		return
	}
	out[source] = true
	addRollupDependencies(source, out)
}

func barKey(exchange string, symbol string, tf string) string {
	return strings.ToLower(exchange) + ":" + strings.ToUpper(symbol) + ":" + timeframe.MustNormalize(tf)
}
