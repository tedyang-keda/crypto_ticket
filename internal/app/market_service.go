package app

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

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
	store              storage.HistoricalStore
	hub                *realtime.Hub
	mu                 sync.RWMutex
	frames             []string
	recentMax          int
	liveBars           map[string]market.Bar
	finalObservers     []FinalBarObserver
	telemetry          Telemetry
	symbolLocks        [1024]sync.Mutex
	latestVerification sync.Map
}

type FinalBarObserver interface {
	ObserveFinalBar(ctx context.Context, bar market.Bar) error
}

type Telemetry interface {
	BarPersisted(bar market.Bar)
	BarPublished(bar market.Bar)
	StorageOperation(operation string, duration time.Duration, err error)
}

type rollupBucketKey struct {
	exchange  string
	symbol    string
	timeframe string
	startMS   int64
}

type rollupBucket struct {
	exchange        string
	symbol          string
	sourceTimeframe string
	timeframe       string
	startMS         int64
	endMS           int64
}

type rollupSeriesKey struct {
	exchange        string
	symbol          string
	sourceTimeframe string
	timeframe       string
}

type rollupSourceKey struct {
	exchange  string
	symbol    string
	timeframe string
}

type rollupSourceRange struct {
	startMS int64
	endMS   int64
}

func NewMarketService(store storage.HistoricalStore, hub *realtime.Hub, frames []string, recentLimit int, telemetry ...Telemetry) *MarketService {
	if recentLimit <= 0 {
		recentLimit = defaultRecentLimit
	}
	normalized := normalizeFrames(frames)
	service := &MarketService{
		store:     store,
		hub:       hub,
		frames:    normalized,
		recentMax: recentLimit,
		liveBars:  make(map[string]market.Bar),
	}
	if len(telemetry) > 0 {
		service.telemetry = telemetry[0]
	}
	return service
}

func (s *MarketService) IngestKline(ctx context.Context, bar market.Bar) error {
	if err := timeframe.ValidateForExchange(bar.Exchange, bar.Timeframe); err != nil {
		return err
	}
	bar = normalizeBar(bar)
	if !validBar(bar) {
		return nil
	}
	unlock := s.lockBarSymbols([]market.Bar{bar})
	defer unlock()
	if accepted, err := s.acceptUnverifiedFinal(ctx, bar); err != nil || !accepted {
		return err
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
	unlock := s.lockBarSymbols(bars)
	defer unlock()
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
		if err := timeframe.ValidateForExchange(bar.Exchange, bar.Timeframe); err != nil {
			return err
		}
		bar.IsFinal = true
		if bar.Source == "" {
			bar.Source = "rest"
		}
		if bar.Reason == "" {
			bar.Reason = "guardian_repair"
		}
		bar = normalizeBar(bar)
		if accepted, err := s.acceptUnverifiedFinal(ctx, bar); err != nil {
			return err
		} else if !accepted {
			continue
		}
		enriched, err := s.prepareFinalBar(ctx, bar)
		if err != nil {
			return err
		}
		if !validBar(enriched) {
			continue
		}
		s.mu.Lock()
		key := barKey(enriched.Exchange, enriched.Symbol, enriched.Timeframe)
		if live, ok := s.liveBars[key]; ok && live.StartMS == enriched.StartMS {
			delete(s.liveBars, key)
		}
		s.mu.Unlock()
		if err := s.persistFinalBars(ctx, []market.Bar{enriched}, true); err != nil {
			return err
		}
	}
	return nil
}

// RepairRollups rebuilds configured aggregate timeframes that depend on the
// supplied, already-verified source bars. Unlike RepairFinalBars, it also
// repairs a missing derived bar when the source bars themselves were already
// correct and therefore did not need a REST write.
func (s *MarketService) RepairRollups(ctx context.Context, sourceBars []market.Bar) error {
	if len(sourceBars) == 0 {
		return nil
	}
	unlock := s.lockBarSymbols(sourceBars)
	defer unlock()
	return s.repairRollupsLocked(ctx, sourceBars)
}

func (s *MarketService) repairRollupsLocked(ctx context.Context, sourceBars []market.Bar) error {
	bucketsBySeries := make(map[rollupSeriesKey]map[int64]rollupBucket)
	for _, raw := range sourceBars {
		if err := timeframe.ValidateForExchange(raw.Exchange, raw.Timeframe); err != nil {
			return err
		}
		bar := normalizeBar(raw)
		if !bar.IsFinal || !validBar(bar) {
			continue
		}
		for _, target := range s.frames {
			if timeframe.ValidateForExchange(bar.Exchange, target) != nil {
				continue
			}
			if target == aggregator.OneMinute || aggregator.RollupSourceTimeframe(target) != bar.Timeframe {
				continue
			}
			startMS := timeframe.FloorStartMS(bar.StartMS, target)
			key := rollupSeriesKey{exchange: bar.Exchange, symbol: bar.Symbol, sourceTimeframe: bar.Timeframe, timeframe: target}
			if bucketsBySeries[key] == nil {
				bucketsBySeries[key] = make(map[int64]rollupBucket)
			}
			bucketsBySeries[key][startMS] = rollupBucket{
				exchange: bar.Exchange, symbol: bar.Symbol, sourceTimeframe: bar.Timeframe,
				timeframe: target, startMS: startMS, endMS: timeframe.EndMS(startMS, target),
			}
		}
	}
	seriesKeys := make([]rollupSeriesKey, 0, len(bucketsBySeries))
	sourceRanges := make(map[rollupSourceKey]rollupSourceRange)
	for key, buckets := range bucketsBySeries {
		seriesKeys = append(seriesKeys, key)
		for _, bucket := range buckets {
			sourceKey := rollupSourceKey{exchange: key.exchange, symbol: key.symbol, timeframe: key.sourceTimeframe}
			rangeForSource, exists := sourceRanges[sourceKey]
			if !exists || bucket.startMS < rangeForSource.startMS {
				rangeForSource.startMS = bucket.startMS
			}
			if !exists || bucket.endMS > rangeForSource.endMS {
				rangeForSource.endMS = bucket.endMS
			}
			sourceRanges[sourceKey] = rangeForSource
		}
	}
	sort.Slice(seriesKeys, func(i, j int) bool {
		if timeframe.Index(seriesKeys[i].timeframe) != timeframe.Index(seriesKeys[j].timeframe) {
			return timeframe.Index(seriesKeys[i].timeframe) < timeframe.Index(seriesKeys[j].timeframe)
		}
		if seriesKeys[i].exchange != seriesKeys[j].exchange {
			return seriesKeys[i].exchange < seriesKeys[j].exchange
		}
		return seriesKeys[i].symbol < seriesKeys[j].symbol
	})
	sourceKeys := make([]rollupSourceKey, 0, len(sourceRanges))
	for key := range sourceRanges {
		sourceKeys = append(sourceKeys, key)
	}
	sort.Slice(sourceKeys, func(i, j int) bool {
		if sourceKeys[i].exchange != sourceKeys[j].exchange {
			return sourceKeys[i].exchange < sourceKeys[j].exchange
		}
		if sourceKeys[i].symbol != sourceKeys[j].symbol {
			return sourceKeys[i].symbol < sourceKeys[j].symbol
		}
		return timeframe.Index(sourceKeys[i].timeframe) < timeframe.Index(sourceKeys[j].timeframe)
	})
	sourceBarsBySeries := make(map[rollupSourceKey]map[int64]market.Bar, len(sourceRanges))
	for _, key := range sourceKeys {
		sourceRange := sourceRanges[key]
		started := time.Now()
		localSources, err := s.store.BarsInRange(ctx, key.exchange, key.symbol, key.timeframe, sourceRange.startMS, sourceRange.endMS)
		s.observeStorage("rollup_source", started, err)
		if err != nil {
			return err
		}
		sourceByStart := make(map[int64]market.Bar, len(localSources))
		for _, bar := range localSources {
			sourceByStart[bar.StartMS] = bar
		}
		sourceBarsBySeries[key] = sourceByStart
	}
	var repairs []market.Bar
	var dependentSources []market.Bar
	for _, key := range seriesKeys {
		buckets := bucketsBySeries[key]
		starts := make([]int64, 0, len(buckets))
		for startMS := range buckets {
			starts = append(starts, startMS)
		}
		sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
		if len(starts) == 0 {
			continue
		}
		sourceKey := rollupSourceKey{exchange: key.exchange, symbol: key.symbol, timeframe: key.sourceTimeframe}
		sourceByStart := sourceBarsBySeries[sourceKey]
		previousStart := timeframe.PreviousStartMS(starts[0], key.timeframe)
		started := time.Now()
		localRollups, err := s.store.BarsInRange(ctx, key.exchange, key.symbol, key.timeframe, previousStart, starts[len(starts)-1])
		s.observeStorage("rollup_existing", started, err)
		if err != nil {
			return err
		}
		rollupByStart := make(map[int64]market.Bar, len(localRollups))
		for _, bar := range localRollups {
			rollupByStart[bar.StartMS] = bar
		}
		for _, startMS := range starts {
			bucket := buckets[startMS]
			var sourceBars []market.Bar
			for cursor := bucket.startMS; cursor <= bucket.endMS; cursor = timeframe.NextStartMS(cursor, bucket.sourceTimeframe) {
				bar, exists := sourceByStart[cursor]
				if !exists {
					sourceBars = nil
					break
				}
				sourceBars = append(sourceBars, bar)
			}
			if !sourceBarsComplete(sourceBars, bucket.sourceTimeframe, bucket.startMS, bucket.endMS) {
				continue
			}
			rollup := aggregator.RollupBars(bucket.timeframe, sourceBars, true, "rollup_repair", market.NowMS())
			if rollup == nil {
				continue
			}
			previousClose := 0.0
			if previous, ok := rollupByStart[timeframe.PreviousStartMS(bucket.startMS, bucket.timeframe)]; ok {
				previousClose = previous.ClosePrice
			}
			enriched := aggregator.ApplyDerived(*rollup, previousClose)
			dependentSources = append(dependentSources, enriched)
			if existing, ok := rollupByStart[bucket.startMS]; ok && sameRollup(existing, enriched) {
				rollupByStart[bucket.startMS] = enriched
				continue
			}
			repairs = append(repairs, enriched)
			rollupByStart[bucket.startMS] = enriched
		}
	}
	if len(repairs) > 0 {
		started := time.Now()
		if err := s.store.UpsertBars(ctx, repairs); err != nil {
			s.observeStorage("rollup_upsert", started, err)
			return err
		}
		s.observeStorage("rollup_upsert", started, nil)
		for _, bar := range repairs {
			if s.telemetry != nil {
				s.telemetry.BarPersisted(bar)
			}
			s.publishBar(bar)
		}
	}
	if len(dependentSources) > 0 {
		return s.repairRollupsLocked(ctx, dependentSources)
	}
	return nil
}

func sameRollup(left market.Bar, right market.Bar) bool {
	return left.IsFinal && left.Exchange == right.Exchange && left.Symbol == right.Symbol &&
		left.Timeframe == right.Timeframe && left.StartMS == right.StartMS && left.EndMS == right.EndMS &&
		left.MarginType == right.MarginType && left.OpenPrice == right.OpenPrice &&
		left.HighPrice == right.HighPrice && left.LowPrice == right.LowPrice &&
		left.ClosePrice == right.ClosePrice && left.Volume == right.Volume &&
		left.QuoteVolume == right.QuoteVolume && left.ContractVolume == right.ContractVolume &&
		left.TradeCount == right.TradeCount && left.VolumeUnit == right.VolumeUnit &&
		left.QuoteUnit == right.QuoteUnit && left.LastTickMS == right.LastTickMS && left.PrevClose == right.PrevClose &&
		left.Chg == right.Chg && left.Amp == right.Amp
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
	started := time.Now()
	bars, err := s.store.BarsInRange(ctx, bar.Exchange, bar.Symbol, bar.Timeframe, previousStart, previousStart)
	s.observeStorage("previous_close", started, err)
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
		if err := timeframe.ValidateForExchange(bars[i].Exchange, bars[i].Timeframe); err != nil {
			return err
		}
		bars[i] = market.DecorateBar(bars[i])
	}
	started := time.Now()
	if err := s.store.UpsertBars(ctx, bars); err != nil {
		s.observeStorage("upsert_bars", started, err)
		return err
	}
	s.observeStorage("upsert_bars", started, nil)
	if s.telemetry != nil {
		for _, bar := range bars {
			s.telemetry.BarPersisted(bar)
		}
	}
	for _, bar := range bars {
		s.publishBar(bar)
	}
	return s.rollupFinalBars(ctx, bars, repairRollups)
}

func (s *MarketService) rollupFinalBars(ctx context.Context, bars []market.Bar, repairRollups bool) error {
	nowMS := market.NowMS()
	queue := append([]market.Bar(nil), bars...)
	processed := make(map[rollupBucketKey]bool)
	for len(queue) > 0 {
		bar := queue[0]
		queue = queue[1:]
		if !bar.IsFinal {
			continue
		}
		for _, tf := range s.frames {
			if timeframe.ValidateForExchange(bar.Exchange, tf) != nil {
				continue
			}
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
			key := rollupBucketKey{exchange: bar.Exchange, symbol: bar.Symbol, timeframe: tf, startMS: targetStart}
			if processed[key] {
				continue
			}
			rollup, err := s.rebuildRollupBucket(ctx, rollupBucket{
				exchange: bar.Exchange, symbol: bar.Symbol, sourceTimeframe: bar.Timeframe,
				timeframe: tf, startMS: targetStart, endMS: targetEnd,
			}, "rollup", nowMS)
			if err != nil {
				return err
			}
			if rollup == nil {
				continue
			}
			processed[key] = true
			queue = append(queue, *rollup)
		}
	}
	return nil
}

func (s *MarketService) rebuildRollupBucket(ctx context.Context, bucket rollupBucket, reason string, nowMS int64) (*market.Bar, error) {
	if err := timeframe.ValidateForExchange(bucket.exchange, bucket.timeframe); err != nil {
		return nil, err
	}
	started := time.Now()
	sourceBars, err := s.store.BarsInRange(ctx, bucket.exchange, bucket.symbol, bucket.sourceTimeframe, bucket.startMS, bucket.endMS)
	s.observeStorage("rollup_source", started, err)
	if err != nil {
		return nil, err
	}
	if !sourceBarsComplete(sourceBars, bucket.sourceTimeframe, bucket.startMS, bucket.endMS) {
		return nil, nil
	}
	rollup := aggregator.RollupBars(bucket.timeframe, sourceBars, true, reason, nowMS)
	if rollup == nil {
		return nil, nil
	}
	enriched, err := s.enrichBar(ctx, *rollup)
	if err != nil {
		return nil, err
	}
	started = time.Now()
	if err := s.store.UpsertBars(ctx, []market.Bar{enriched}); err != nil {
		s.observeStorage("rollup_upsert", started, err)
		return nil, err
	}
	s.observeStorage("rollup_upsert", started, nil)
	if s.telemetry != nil {
		s.telemetry.BarPersisted(enriched)
	}
	s.publishBar(enriched)
	return &enriched, nil
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
	for _, tf := range timeframe.SupportedForExchange(oneMinute.Exchange) {
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
	if err := timeframe.ValidateForExchange(liveOneMinute.Exchange, target); err != nil {
		return nil, err
	}
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
	started := time.Now()
	partialInputs, err := s.store.BarsInRange(ctx, liveOneMinute.Exchange, liveOneMinute.Symbol, source, targetStart, liveSource.StartMS-1)
	s.observeStorage("live_rollup_source", started, err)
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
	if s.telemetry != nil {
		s.telemetry.BarPublished(barCopy)
	}
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
	started := time.Now()
	bars, err := s.store.RecentBars(ctx, market.KlineQuery{Exchange: exchange, Symbol: symbol, Timeframe: aggregator.OneMinute, Limit: 1})
	s.observeStorage("latest_tick", started, err)
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
	if err := timeframe.ValidateForExchange(query.Exchange, query.Timeframe); err != nil {
		return nil, err
	}
	query.Timeframe = timeframe.MustNormalize(query.Timeframe)
	if query.Limit <= 0 {
		query.Limit = defaultRecentLimit
	}
	if query.Limit > 1000 {
		query.Limit = 1000
	}

	started := time.Now()
	bars, err := s.store.RecentBars(ctx, query)
	s.observeStorage("recent_bars", started, err)
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

	partial, err := s.buildLiveRollup(ctx, query.Timeframe, *live, market.NowMS())
	if err != nil {
		return nil, err
	}
	if partial == nil {
		return trimBars(bars, query.Limit), nil
	}
	return trimBars(mergeLiveBar(bars, partial), query.Limit+1), nil
}

func (s *MarketService) observeStorage(operation string, started time.Time, err error) {
	if s.telemetry != nil {
		s.telemetry.StorageOperation(operation, time.Since(started), err)
	}
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
		if timeframe.ValidateForExchange("", tf) != nil {
			continue
		}
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
