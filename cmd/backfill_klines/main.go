package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"crypto-ticket/internal/aggregator"
	"crypto-ticket/internal/cache"
	"crypto-ticket/internal/config"
	"crypto-ticket/internal/corporateaction"
	"crypto-ticket/internal/exchange"
	"crypto-ticket/internal/market"
	"crypto-ticket/internal/retention"
	mysqlstore "crypto-ticket/internal/storage/mysql"
	"crypto-ticket/internal/timeframe"
)

type exchangeRuntime struct {
	config  config.ExchangeConfig
	adapter interface {
		exchange.Adapter
		exchange.RESTKlineFetcher
	}
}

type runOptions struct {
	exchanges        []string
	marketTypes      map[string]bool
	symbols          map[string]bool
	timeframes       []string
	limit            int
	startMS          int64
	endMS            int64
	batchSize        int
	requestDelay     time.Duration
	backfill         bool
	clearRedis       bool
	clearLive        bool
	clearAllBars     bool
	clearAllRedis    bool
	refreshSymbols   bool
	continueOnErr    bool
	dryRun           bool
	redisScanCount   int64
	adjustment       exchange.KlineAdjustment
	replaceExisting  bool
	respectRetention bool
}

type corporateAction struct {
	EffectiveMS       int64
	ObservedRatio     float64
	AppliedMultiplier float64
}

type forwardAdjustmentSymbolProvider interface {
	ForwardAdjustmentSymbols(context.Context, *http.Client) (map[string]bool, error)
}

type continuousBackfillFetcher struct {
	name    string
	fetcher exchange.ContinuousKlineFetcher
}

func (f *continuousBackfillFetcher) Name() string { return f.name + "-continuous" }

func (f *continuousBackfillFetcher) FetchKlines(ctx context.Context, client *http.Client, request exchange.KlineRequest) ([]market.Bar, error) {
	bars, err := f.fetcher.FetchContinuousKlines(ctx, client, request)
	if err != nil {
		return nil, err
	}
	for i := range bars {
		bars[i].Source = "rest_forward_adjusted"
		bars[i].Reason = "binance_tradifi_continuous_kline"
	}
	return bars, nil
}

func main() {
	cfg := config.Load()
	options, err := parseOptions(cfg)
	if err != nil {
		log.Fatalf("invalid options: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := mysqlstore.New(cfg.MySQLDSN)
	if err != nil {
		log.Fatalf("connect mysql: %v", err)
	}
	defer store.Close()
	if err := store.EnsureSchema(ctx); err != nil {
		log.Fatalf("ensure schema: %v", err)
	}

	var redisCache *cache.RedisMarketCache
	if (options.clearRedis || options.clearAllRedis) && !options.dryRun {
		redisCache, err = cache.NewRedisMarketCache(cfg.RedisURL)
		if err != nil {
			log.Fatalf("connect redis: %v", err)
		}
		defer redisCache.Close()
	}

	httpClient := &http.Client{Timeout: 15 * time.Second}
	runtimes := makeExchangeRuntimes(cfg.Exchanges, options.exchanges, options.marketTypes)
	if len(runtimes) == 0 {
		log.Fatal("no enabled exchanges selected")
	}

	var totalBars int
	var totalCacheKeys int64

	if options.clearAllBars {
		if options.dryRun {
			log.Printf("dry-run clear all bar_history")
		} else {
			count, err := store.ClearBars(ctx)
			if err != nil {
				log.Fatalf("clear bar history: %v", err)
			}
			log.Printf("cleared MySQL bar tables rows=%d", count)
		}
	}

	if options.clearAllRedis {
		if options.dryRun {
			log.Printf("dry-run clear all Redis kline recent/live keys include_live=%t", options.clearLive)
		} else {
			count, err := redisCache.ClearKlineCache(ctx, cache.KlineCacheClearOptions{
				IncludeRecent: true,
				IncludeLive:   options.clearLive,
				ScanCount:     options.redisScanCount,
			})
			if err != nil {
				log.Fatalf("clear all redis kline cache: %v", err)
			}
			totalCacheKeys += count
			log.Printf("cleared all Redis kline cache keys=%d", count)
		}
	}

	for _, runtime := range runtimes {
		symbols, err := loadSymbols(ctx, store, httpClient, runtime.adapter, runtime.config.Name, runtime.config.MarketType, options.symbols, options.refreshSymbols)
		if err != nil {
			log.Fatalf("load symbols exchange=%s: %v", runtime.config.Name, err)
		}
		if len(symbols) == 0 {
			log.Printf("exchange=%s no symbols selected", runtime.config.Name)
			continue
		}
		log.Printf("exchange=%s symbols=%d timeframes=%d backfill=%t clear_redis=%t dry_run=%t", runtime.config.Name, len(symbols), len(options.timeframes), options.backfill, options.clearRedis, options.dryRun)

		if options.backfill {
			count, err := backfillExchange(ctx, store, httpClient, runtime.adapter, symbols, options)
			if err != nil {
				log.Fatalf("backfill exchange=%s: %v", runtime.config.Name, err)
			}
			totalBars += count
		}
		if options.clearRedis && !options.clearAllRedis {
			count, err := clearRedisKlineCache(ctx, redisCache, runtime.config.Name, symbols, options)
			if err != nil {
				log.Fatalf("clear redis exchange=%s: %v", runtime.config.Name, err)
			}
			totalCacheKeys += count
		}
	}
	log.Printf("done bars=%d deleted_cache_keys=%d dry_run=%t", totalBars, totalCacheKeys, options.dryRun)
}

func parseOptions(cfg config.Config) (runOptions, error) {
	var exchangesRaw string
	var marketTypesRaw string
	var symbolsRaw string
	var timeframesRaw string
	var startRaw string
	var endRaw string
	var adjustmentRaw string
	var requestDelayMS int
	var options runOptions
	var err error

	flag.StringVar(&exchangesRaw, "exchanges", enabledExchangeCSV(cfg.Exchanges), "comma-separated exchanges, default enabled exchanges from env")
	flag.StringVar(&marketTypesRaw, "market-types", "", "optional comma-separated market types, for example um_futures,SWAP")
	flag.StringVar(&symbolsRaw, "symbols", "", "comma-separated exact symbols; default active symbols from symbol_registry")
	flag.StringVar(&timeframesRaw, "timeframes", strings.Join(cfg.Timeframes, ","), "comma-separated timeframes")
	flag.IntVar(&options.limit, "limit", cfg.RecentCacheLimit, "max bars to fetch per exchange/symbol/timeframe; 0 means no cap when -start is set")
	flag.StringVar(&startRaw, "start", "", "optional inclusive start time: unix ms, RFC3339, or YYYY-MM-DD")
	flag.StringVar(&endRaw, "end", "", "optional inclusive end time: unix ms, RFC3339, or YYYY-MM-DD")
	flag.StringVar(&adjustmentRaw, "adjustment", string(exchange.KlineAdjustmentAuto), "price adjustment: auto, forward, or raw; auto prefers official forward adjustment and falls back to raw")
	flag.IntVar(&options.batchSize, "batch-size", 500, "MySQL upsert batch size")
	flag.IntVar(&requestDelayMS, "request-delay-ms", 100, "delay between exchange REST requests")
	flag.BoolVar(&options.backfill, "backfill", true, "fetch official REST klines and upsert bar_history")
	flag.BoolVar(&options.replaceExisting, "replace-existing", true, "delete existing bars in each successfully fetched range before writing the replacement set")
	flag.BoolVar(&options.respectRetention, "respect-retention", true, "clamp each timeframe start to the maintain_klines retention cutoff")
	flag.BoolVar(&options.clearRedis, "clear-redis", true, "clear Redis kline recent cache after processing")
	flag.BoolVar(&options.clearLive, "clear-livebar", true, "also clear Redis livebar keys")
	flag.BoolVar(&options.clearAllBars, "clear-all-bar-history", false, "delete all rows from bar_history before backfill")
	flag.BoolVar(&options.clearAllRedis, "clear-all-redis-kline", false, "delete all Redis kline recent/live keys with wildcard SCAN")
	flag.BoolVar(&options.refreshSymbols, "refresh-symbols", false, "fetch the exchange symbol list before backfill and use that current list")
	flag.BoolVar(&options.continueOnErr, "continue-on-error", false, "log individual symbol/timeframe fetch errors and continue")
	flag.BoolVar(&options.dryRun, "dry-run", false, "fetch and log only; do not write MySQL or delete Redis keys")
	flag.Int64Var(&options.redisScanCount, "redis-scan-count", 500, "Redis SCAN count for cache cleanup")
	flag.Parse()

	exchanges := normalizeCSV(exchangesRaw)
	if len(exchanges) == 0 {
		return options, errors.New("at least one exchange is required")
	}
	options.exchanges = exchanges
	options.marketTypes = normalizeLowerSet(marketTypesRaw)
	options.symbols = normalizeSymbolSet(symbolsRaw)

	frames := normalizeTimeframeCSV(timeframesRaw)
	if len(frames) == 0 {
		return options, errors.New("at least one timeframe is required")
	}
	for _, tf := range frames {
		normalized, err := timeframe.Normalize(tf)
		if err != nil {
			return options, err
		}
		options.timeframes = append(options.timeframes, normalized)
	}
	options.adjustment, err = exchange.ParseKlineAdjustment(adjustmentRaw)
	if err != nil {
		return options, err
	}
	options.startMS = 0
	options.endMS = 0
	if strings.TrimSpace(startRaw) != "" {
		options.startMS, err = parseTimeMS(startRaw)
		if err != nil {
			return options, fmt.Errorf("parse -start: %w", err)
		}
	}
	if strings.TrimSpace(endRaw) != "" {
		options.endMS, err = parseTimeMS(endRaw)
		if err != nil {
			return options, fmt.Errorf("parse -end: %w", err)
		}
	}
	if options.startMS > 0 && options.endMS > 0 && options.startMS > options.endMS {
		return options, errors.New("-start must be <= -end")
	}
	if options.limit <= 0 && options.startMS == 0 {
		return options, errors.New("-limit=0 requires -start to avoid unbounded latest backfill")
	}
	if options.batchSize <= 0 {
		options.batchSize = 500
	}
	if requestDelayMS < 0 {
		requestDelayMS = 0
	}
	options.requestDelay = time.Duration(requestDelayMS) * time.Millisecond
	return options, nil
}

func makeExchangeRuntimes(configs []config.ExchangeConfig, selected []string, selectedMarketTypes map[string]bool) []exchangeRuntime {
	selectedSet := make(map[string]bool, len(selected))
	for _, name := range selected {
		selectedSet[strings.ToLower(name)] = true
	}
	var runtimes []exchangeRuntime
	for _, cfg := range configs {
		name := strings.ToLower(strings.TrimSpace(cfg.Name))
		if !cfg.Enabled || !selectedSet[name] {
			continue
		}
		if len(selectedMarketTypes) > 0 && !selectedMarketTypes[strings.ToLower(strings.TrimSpace(cfg.MarketType))] {
			continue
		}
		switch name {
		case "binance":
			runtimes = append(runtimes, exchangeRuntime{
				config:  cfg,
				adapter: exchange.NewBinanceFuturesAdapter(cfg.MarketType, cfg.RestURL, cfg.WSURL),
			})
		case "okx":
			runtimes = append(runtimes, exchangeRuntime{
				config:  cfg,
				adapter: exchange.NewOKXAdapter(cfg.MarketType, cfg.RestURL, cfg.WSURL),
			})
		}
	}
	return runtimes
}

func loadSymbols(
	ctx context.Context,
	store interface {
		UpsertSymbols(context.Context, []market.SymbolInfo) error
		ListSymbols(context.Context, string, *bool) ([]market.SymbolInfo, error)
	},
	client *http.Client,
	adapter exchange.Adapter,
	exchangeName string,
	marketType string,
	filter map[string]bool,
	refresh bool,
) ([]string, error) {
	active := true
	var infos []market.SymbolInfo
	if refresh {
		var err error
		infos, err = adapter.FetchSymbols(ctx, client)
		if err != nil {
			return nil, err
		}
		if err := store.UpsertSymbols(ctx, infos); err != nil {
			return nil, err
		}
	} else {
		var err error
		infos, err = store.ListSymbols(ctx, exchangeName, &active)
		if err != nil {
			return nil, err
		}
		if len(infos) == 0 {
			infos, err = adapter.FetchSymbols(ctx, client)
			if err != nil {
				return nil, err
			}
			if err := store.UpsertSymbols(ctx, infos); err != nil {
				return nil, err
			}
		}
	}
	symbols := make([]string, 0, len(infos))
	for _, info := range infos {
		if !info.IsActive {
			continue
		}
		if marketType != "" && !strings.EqualFold(strings.TrimSpace(info.MarketType), strings.TrimSpace(marketType)) {
			continue
		}
		symbol := strings.ToUpper(strings.TrimSpace(info.Symbol))
		if len(filter) > 0 && !filter[symbol] {
			continue
		}
		if symbol != "" {
			symbols = append(symbols, symbol)
		}
	}
	if len(filter) > 0 && len(symbols) == 0 && len(infos) == 0 {
		for symbol := range filter {
			symbols = append(symbols, symbol)
		}
	}
	sort.Strings(symbols)
	return symbols, nil
}

func backfillExchange(
	ctx context.Context,
	store interface {
		UpsertBars(context.Context, []market.Bar) error
		DeleteBarsInRange(context.Context, string, string, string, int64, int64) (int64, error)
	},
	client *http.Client,
	fetcher exchange.RESTKlineFetcher,
	symbols []string,
	options runOptions,
) (int, error) {
	var total int
	forwardSymbols := map[string]bool(nil)
	if options.adjustment != exchange.KlineAdjustmentRaw {
		if provider, ok := fetcher.(forwardAdjustmentSymbolProvider); ok {
			var err error
			forwardSymbols, err = provider.ForwardAdjustmentSymbols(ctx, client)
			if err != nil {
				if options.adjustment == exchange.KlineAdjustmentForward {
					return 0, fmt.Errorf("load forward-adjustment symbols: %w", err)
				}
				log.Printf("forward-adjustment symbol detection failed exchange=%s fallback=raw err=%v", fetcher.Name(), err)
				forwardSymbols = nil
			}
		}
	}
	batch := make([]market.Bar, 0, options.batchSize)
	flush := func() error {
		if len(batch) == 0 || options.dryRun {
			batch = batch[:0]
			return nil
		}
		if err := store.UpsertBars(ctx, batch); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}

	for _, symbol := range symbols {
		symbolFetcher := fetcher
		var binanceActions []corporateAction
		binanceFactorFallback := false
		if forwardSymbols[strings.ToUpper(symbol)] {
			if continuous, actions, validated, err := prepareBinanceContinuous(ctx, client, fetcher, symbol, options); err != nil {
				if options.adjustment == exchange.KlineAdjustmentForward && len(actions) == 0 {
					return total, fmt.Errorf("validate Binance continuous forward-adjustment source symbol=%s: %w", symbol, err)
				}
				log.Printf("Binance continuous validation failed exchange=%s symbol=%s fallback=factor/raw actions=%d err=%v", fetcher.Name(), symbol, len(actions), err)
				binanceActions = actions
				binanceFactorFallback = len(actions) > 0
			} else if validated {
				symbolFetcher = continuous
				log.Printf("using Binance continuous forward-adjusted K-lines exchange=%s symbol=%s actions=%d", fetcher.Name(), symbol, len(actions))
			} else {
				binanceActions = actions
				binanceFactorFallback = len(actions) > 0
				log.Printf("Binance continuous unavailable exchange=%s symbol=%s fallback=factor/raw actions=%d", fetcher.Name(), symbol, len(actions))
			}
		}
		for _, tf := range options.timeframes {
			requestStartMS := options.startMS
			requestLimit := options.limit
			if options.respectRetention {
				requestStartMS, requestLimit = retainedRequest(tf, requestStartMS, options.endMS, requestLimit, time.Now())
			}
			request := exchange.KlineRequest{
				Symbol:     symbol,
				Timeframe:  tf,
				StartMS:    requestStartMS,
				EndMS:      options.endMS,
				Limit:      requestLimit,
				Adjustment: options.adjustment,
				PageDelay:  options.requestDelay,
			}
			var bars []market.Bar
			var strategy string
			var err error
			bars, strategy, err = fetchBackfillBars(ctx, client, symbolFetcher, request)
			if err == nil && binanceFactorFallback {
				bars, err = adjustBinanceBackfillBars(ctx, client, fetcher, request, bars, binanceActions, options.requestDelay)
				strategy = "binance-factor-fallback/" + strategy
			}
			if err != nil {
				if options.continueOnErr {
					log.Printf("skip failed exchange=%s symbol=%s timeframe=%s err=%v", fetcher.Name(), symbol, tf, err)
					continue
				}
				return total, err
			}
			deleted := int64(0)
			if options.replaceExisting && !options.dryRun && len(bars) > 0 {
				if err := flush(); err != nil {
					return total, err
				}
				startMS, endMS := barRange(bars)
				if request.StartMS > 0 {
					startMS = request.StartMS
				}
				if options.endMS > 0 {
					endMS = options.endMS
				} else {
					endMS = timeframe.EndMS(endMS, tf)
				}
				deleted, err = store.DeleteBarsInRange(ctx, bars[0].Exchange, bars[0].Symbol, bars[0].Timeframe, startMS, endMS)
				if err != nil {
					return total, err
				}
			}
			total += len(bars)
			log.Printf("fetched exchange=%s symbol=%s timeframe=%s bars=%d deleted=%d strategy=%s adjustment=%s", fetcher.Name(), symbol, tf, len(bars), deleted, strategy, options.adjustment)
			for _, bar := range bars {
				batch = append(batch, bar)
				if len(batch) >= options.batchSize {
					if err := flush(); err != nil {
						return total, err
					}
				}
			}
			if err := flush(); err != nil {
				return total, err
			}
			if options.requestDelay > 0 {
				timer := time.NewTimer(options.requestDelay)
				select {
				case <-ctx.Done():
					timer.Stop()
					return total, ctx.Err()
				case <-timer.C:
				}
			}
		}
	}
	if err := flush(); err != nil {
		return total, err
	}
	return total, nil
}

func retainedStartMS(tf string, requestedStartMS int64, now time.Time) int64 {
	start, _ := retainedRequest(tf, requestedStartMS, 0, 0, now)
	return start
}

func retainedRequest(tf string, requestedStartMS int64, requestedEndMS int64, requestedLimit int, now time.Time) (int64, int) {
	rule := retention.RuleFor(tf)
	if rule.KeepBars > 0 {
		limit := requestedLimit
		if limit <= 0 || limit > rule.KeepBars {
			limit = rule.KeepBars
		}
		reference := requestedEndMS
		if reference <= 0 {
			reference = now.UnixMilli()
		}
		windowStart := retention.BarWindowStartMS(tf, reference, rule.KeepBars)
		if requestedStartMS <= 0 || requestedStartMS < windowStart {
			return 0, limit
		}
		return requestedStartMS, limit
	}
	cutoffMS, ok := retention.CutoffMS(rule, now)
	if !ok || requestedStartMS >= cutoffMS {
		return requestedStartMS, requestedLimit
	}
	return cutoffMS, requestedLimit
}

func forwardAdjustmentLookbackStart(options runOptions) int64 {
	endMS := options.endMS
	if endMS <= 0 {
		endMS = market.NowMS()
	}
	var earliest int64
	for _, tf := range options.timeframes {
		startMS, limit := retainedRequest(tf, options.startMS, options.endMS, options.limit, time.UnixMilli(endMS).UTC())
		if startMS > 0 {
			if earliest == 0 || startMS < earliest {
				earliest = startMS
			}
			continue
		}
		if limit <= 0 {
			continue
		}
		windowStart := retention.BarWindowStartMS(tf, endMS, limit+2)
		if windowStart <= 0 {
			windowStart = endMS - timeframe.DurationMS(tf)*int64(limit+2)
		}
		if earliest == 0 || windowStart < earliest {
			earliest = windowStart
		}
	}
	if earliest <= 1 || earliest >= endMS {
		return 1
	}
	return timeframe.FloorStartMS(earliest, "1D")
}

func prepareBinanceContinuous(ctx context.Context, client *http.Client, fetcher exchange.RESTKlineFetcher, symbol string, options runOptions) (exchange.RESTKlineFetcher, []corporateAction, bool, error) {
	continuous, ok := fetcher.(exchange.ContinuousKlineFetcher)
	if !ok {
		return nil, nil, false, fmt.Errorf("continuousKlines is not supported")
	}
	startMS := forwardAdjustmentLookbackStart(options)
	request := exchange.KlineRequest{
		Symbol: symbol, Timeframe: "1D", StartMS: startMS, EndMS: options.endMS,
		Limit: 0, Adjustment: exchange.KlineAdjustmentRaw, PageDelay: options.requestDelay,
	}
	rawDaily, err := fetcher.FetchKlines(ctx, client, request)
	if err != nil {
		return nil, nil, false, err
	}
	continuousDaily, err := continuous.FetchContinuousKlines(ctx, client, request)
	if err != nil {
		return nil, nil, false, err
	}
	actions := detectCorporateActions(rawDaily)
	candidates := candidateCorporateActionBuckets(rawDaily, continuousDaily)
	for _, candidate := range candidates {
		minuteBars, minuteErr := fetcher.FetchKlines(ctx, client, exchange.KlineRequest{
			Symbol: symbol, Timeframe: "1m", StartMS: candidate.StartMS, EndMS: candidate.EndMS,
			Limit: 0, Adjustment: exchange.KlineAdjustmentRaw, PageDelay: options.requestDelay,
		})
		if minuteErr != nil {
			return nil, actions, false, fmt.Errorf("inspect candidate corporate-action bucket %d: %w", candidate.StartMS, minuteErr)
		}
		_, minuteActions := forwardAdjustOneMinuteBars(minuteBars)
		if len(minuteActions) > 0 {
			actions = append(actions, minuteActions...)
		} else if factor, ok := candidateBucketFactor(candidate, rawDaily, continuousDaily); ok {
			actions = append(actions, corporateAction{EffectiveMS: candidate.StartMS, AppliedMultiplier: factor})
		}
	}
	actions = uniqueCorporateActions(actions)
	if !validateContinuousKlines(rawDaily, continuousDaily, actions) {
		return nil, actions, false, fmt.Errorf("continuous daily data did not pass corporate-action validation")
	}
	return &continuousBackfillFetcher{name: fetcher.Name(), fetcher: continuous}, actions, true, nil
}

func candidateCorporateActionBuckets(raw []market.Bar, continuous []market.Bar) []market.Bar {
	continuousByStart := make(map[int64]market.Bar, len(continuous))
	for _, bar := range continuous {
		continuousByStart[bar.StartMS] = bar
	}
	ordered := append([]market.Bar(nil), raw...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].StartMS < ordered[j].StartMS })
	seen := make(map[int64]bool)
	var candidates []market.Bar
	previousRatio := float64(0)
	for _, bar := range ordered {
		matched, ok := continuousByStart[bar.StartMS]
		if !ok || bar.ClosePrice <= 0 || matched.ClosePrice <= 0 {
			continue
		}
		ratio := matched.ClosePrice / bar.ClosePrice
		if previousRatio > 0 {
			if _, ok := snapCorporateActionMultiplier(ratio / previousRatio); ok && !seen[bar.StartMS] {
				seen[bar.StartMS] = true
				candidates = append(candidates, bar)
			}
		}
		previousRatio = ratio
		if bar.LowPrice > 0 {
			if _, ok := snapCorporateActionMultiplier(bar.HighPrice / bar.LowPrice); ok && !seen[bar.StartMS] {
				seen[bar.StartMS] = true
				candidates = append(candidates, bar)
			}
		}
	}
	return candidates
}

func candidateBucketFactor(candidate market.Bar, raw []market.Bar, continuous []market.Bar) (float64, bool) {
	rawByStart := make(map[int64]market.Bar, len(raw))
	continuousByStart := make(map[int64]market.Bar, len(continuous))
	for _, bar := range raw {
		rawByStart[bar.StartMS] = bar
	}
	for _, bar := range continuous {
		continuousByStart[bar.StartMS] = bar
	}
	var previousStart int64
	for start := range rawByStart {
		if start < candidate.StartMS && start > previousStart {
			previousStart = start
		}
	}
	previousRaw, rawOK := rawByStart[previousStart]
	previousContinuous, continuousOK := continuousByStart[previousStart]
	if !rawOK || !continuousOK || previousRaw.ClosePrice <= 0 || previousContinuous.ClosePrice <= 0 || candidate.ClosePrice <= 0 {
		return 0, false
	}
	candidateContinuous, ok := continuousByStart[candidate.StartMS]
	if !ok || candidateContinuous.ClosePrice <= 0 {
		return 0, false
	}
	beforeRatio := previousContinuous.ClosePrice / previousRaw.ClosePrice
	afterRatio := candidateContinuous.ClosePrice / candidate.ClosePrice
	return snapCorporateActionMultiplier(beforeRatio / afterRatio)
}

func uniqueCorporateActions(actions []corporateAction) []corporateAction {
	sort.Slice(actions, func(i, j int) bool { return actions[i].EffectiveMS < actions[j].EffectiveMS })
	out := make([]corporateAction, 0, len(actions))
	for _, action := range actions {
		duplicate := false
		for _, existing := range out {
			if existing.EffectiveMS == action.EffectiveMS ||
				(absInt64(existing.EffectiveMS-action.EffectiveMS) < timeframe.DayMS && relativeDifference(existing.AppliedMultiplier, action.AppliedMultiplier) < 0.01) {
				duplicate = true
				break
			}
		}
		if !duplicate {
			out = append(out, action)
		}
	}
	return out
}

func detectCorporateActions(bars []market.Bar) []corporateAction {
	ordered := append([]market.Bar(nil), bars...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].StartMS < ordered[j].StartMS })
	actions := make([]corporateAction, 0)
	for i := 1; i < len(ordered); i++ {
		if ordered[i-1].ClosePrice <= 0 || ordered[i].OpenPrice <= 0 {
			continue
		}
		ratio := ordered[i].OpenPrice / ordered[i-1].ClosePrice
		factor, ok := snapCorporateActionMultiplier(ratio)
		if !ok {
			continue
		}
		actions = append(actions, corporateAction{EffectiveMS: ordered[i].StartMS, ObservedRatio: ratio, AppliedMultiplier: factor})
	}
	return actions
}

func validateContinuousKlines(raw []market.Bar, continuous []market.Bar, actions []corporateAction) bool {
	if len(raw) > 0 && len(continuous) == 0 {
		return false
	}
	if len(actions) == 0 {
		return true
	}
	continuousByStart := make(map[int64]market.Bar, len(continuous))
	for _, bar := range continuous {
		continuousByStart[bar.StartMS] = bar
	}
	for _, action := range actions {
		var previous, next market.Bar
		foundPrevious, foundNext := false, false
		for _, bar := range raw {
			if bar.EndMS < action.EffectiveMS && (!foundPrevious || bar.StartMS > previous.StartMS) {
				previous, foundPrevious = bar, true
			}
			if bar.StartMS > action.EffectiveMS && (!foundNext || bar.StartMS < next.StartMS) {
				next, foundNext = bar, true
			}
		}
		continuousPrevious, continuousPreviousOK := continuousByStart[previous.StartMS]
		continuousNext, continuousNextOK := continuousByStart[next.StartMS]
		if !foundPrevious || !foundNext || !continuousPreviousOK || !continuousNextOK ||
			previous.ClosePrice <= 0 || next.OpenPrice <= 0 || continuousPrevious.ClosePrice <= 0 || continuousNext.OpenPrice <= 0 {
			return false
		}
		beforeRatio := continuousPrevious.ClosePrice / previous.ClosePrice
		afterRatio := continuousNext.OpenPrice / next.OpenPrice
		if afterRatio <= 0 || relativeDifference(beforeRatio/afterRatio, action.AppliedMultiplier) > 0.12 {
			return false
		}
	}
	return true
}

func absInt64(value int64) int64 {
	if value < 0 {
		return -value
	}
	return value
}

func adjustBinanceBackfillBars(ctx context.Context, client *http.Client, fetcher exchange.RESTKlineFetcher, request exchange.KlineRequest, bars []market.Bar, actions []corporateAction, pageDelay time.Duration) ([]market.Bar, error) {
	if len(actions) == 0 || len(bars) == 0 {
		return bars, nil
	}
	out := append([]market.Bar(nil), bars...)
	sort.Slice(out, func(i, j int) bool { return out[i].StartMS < out[j].StartMS })
	for i := range out {
		crossesAction := false
		multiplier := float64(1)
		for _, action := range actions {
			if out[i].StartMS < action.EffectiveMS && out[i].EndMS >= action.EffectiveMS {
				crossesAction = true
			}
			if out[i].EndMS < action.EffectiveMS {
				multiplier *= action.AppliedMultiplier
			}
		}
		if crossesAction && request.Timeframe != "1m" {
			raw, err := fetcher.FetchKlines(ctx, client, exchange.KlineRequest{
				Symbol: request.Symbol, Timeframe: "1m", StartMS: out[i].StartMS, EndMS: out[i].EndMS,
				Limit: 0, Adjustment: exchange.KlineAdjustmentRaw, PageDelay: pageDelay,
			})
			if err != nil {
				return nil, fmt.Errorf("fetch crossing %s bucket at %d: %w", request.Timeframe, out[i].StartMS, err)
			}
			adjusted := applyBinanceFactors(raw, actions)
			rollup := aggregator.RollupBars(request.Timeframe, adjusted, true, "binance_corporate_action_crossing_rollup", market.NowMS())
			if rollup == nil {
				return nil, fmt.Errorf("empty crossing %s bucket at %d", request.Timeframe, out[i].StartMS)
			}
			out[i] = *rollup
			continue
		}
		out[i].OpenPrice *= multiplier
		out[i].HighPrice *= multiplier
		out[i].LowPrice *= multiplier
		out[i].ClosePrice *= multiplier
		if multiplier != 0 {
			out[i].Volume /= multiplier
			out[i].ContractVolume /= multiplier
		}
		out[i].Source = "rest_forward_adjusted"
		out[i].Reason = "binance_corporate_action_forward_adjustment"
		out[i].UpdatedAtMS = market.NowMS()
	}
	previousClose := float64(0)
	for i := range out {
		out[i] = aggregator.ApplyDerived(out[i], previousClose)
		previousClose = out[i].ClosePrice
	}
	return out, nil
}

func applyBinanceFactors(bars []market.Bar, actions []corporateAction) []market.Bar {
	out := append([]market.Bar(nil), bars...)
	sort.Slice(out, func(i, j int) bool { return out[i].StartMS < out[j].StartMS })
	for i := range out {
		multiplier := float64(1)
		for _, action := range actions {
			if out[i].StartMS < action.EffectiveMS {
				multiplier *= action.AppliedMultiplier
			}
		}
		out[i].OpenPrice *= multiplier
		out[i].HighPrice *= multiplier
		out[i].LowPrice *= multiplier
		out[i].ClosePrice *= multiplier
		if multiplier != 0 {
			out[i].Volume /= multiplier
			out[i].ContractVolume /= multiplier
		}
		out[i].Source = "rest_forward_adjusted"
		out[i].Reason = "binance_corporate_action_forward_adjustment"
		out[i].UpdatedAtMS = market.NowMS()
	}
	return out
}

func relativeDifference(left float64, right float64) float64 {
	scale := math.Max(math.Abs(left), math.Abs(right))
	if scale == 0 {
		return 0
	}
	return math.Abs(left-right) / scale
}

func forwardAdjustOneMinuteBars(raw []market.Bar) ([]market.Bar, []corporateAction) {
	if len(raw) == 0 {
		return []market.Bar{}, nil
	}
	ordered := append([]market.Bar(nil), raw...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].StartMS < ordered[j].StartMS })
	adjusted := make([]market.Bar, len(ordered))
	actions := make([]corporateAction, 0)
	cumulativeMultiplier := float64(1)
	for i := len(ordered) - 1; i >= 0; i-- {
		bar := ordered[i]
		bar.OpenPrice *= cumulativeMultiplier
		bar.HighPrice *= cumulativeMultiplier
		bar.LowPrice *= cumulativeMultiplier
		bar.ClosePrice *= cumulativeMultiplier
		bar.Volume /= cumulativeMultiplier
		bar.ContractVolume /= cumulativeMultiplier
		bar.Source = "rest_forward_adjusted"
		bar.Reason = "binance_corporate_action_forward_adjustment"
		adjusted[i] = bar

		if i == 0 || ordered[i].StartMS-ordered[i-1].StartMS != timeframe.MinuteMS ||
			ordered[i-1].ClosePrice <= 0 || ordered[i].OpenPrice <= 0 {
			continue
		}
		observedRatio := ordered[i].OpenPrice / ordered[i-1].ClosePrice
		multiplier, ok := snapCorporateActionMultiplier(observedRatio)
		if !ok {
			continue
		}
		cumulativeMultiplier *= multiplier
		actions = append(actions, corporateAction{
			EffectiveMS:       ordered[i].StartMS,
			ObservedRatio:     observedRatio,
			AppliedMultiplier: multiplier,
		})
	}

	previousClose := float64(0)
	for i := range adjusted {
		adjusted[i] = aggregator.ApplyDerived(adjusted[i], previousClose)
		previousClose = adjusted[i].ClosePrice
	}
	sort.Slice(actions, func(i, j int) bool { return actions[i].EffectiveMS < actions[j].EffectiveMS })
	return adjusted, actions
}

func snapCorporateActionMultiplier(observedRatio float64) (float64, bool) {
	return corporateaction.SnapFactor(observedRatio)
}

func selectBackfillBars(request exchange.KlineRequest, bars []market.Bar) []market.Bar {
	out := make([]market.Bar, 0, len(bars))
	for _, bar := range bars {
		if request.StartMS > 0 && bar.StartMS < request.StartMS {
			continue
		}
		if request.EndMS > 0 && bar.StartMS > request.EndMS {
			continue
		}
		out = append(out, bar)
	}
	if request.Limit > 0 && len(out) > request.Limit {
		out = out[len(out)-request.Limit:]
	}
	return out
}

func barRange(bars []market.Bar) (int64, int64) {
	startMS := bars[0].StartMS
	endMS := bars[0].StartMS
	for _, bar := range bars[1:] {
		if bar.StartMS < startMS {
			startMS = bar.StartMS
		}
		if bar.StartMS > endMS {
			endMS = bar.StartMS
		}
	}
	return startMS, endMS
}

func fetchBackfillBars(
	ctx context.Context,
	client *http.Client,
	fetcher exchange.RESTKlineFetcher,
	request exchange.KlineRequest,
) ([]market.Bar, string, error) {
	bars, err := fetcher.FetchKlines(ctx, client, request)
	if err == nil {
		return bars, "official", nil
	}
	if !errors.Is(err, exchange.ErrUnsupportedKlineInterval) {
		return nil, "", err
	}

	sourceTF := aggregator.RollupSourceTimeframe(request.Timeframe)
	if sourceTF == "" {
		return nil, "", err
	}
	sourceRequest := request
	sourceRequest.Timeframe = sourceTF
	if request.StartMS > 0 {
		sourceRequest.StartMS = timeframe.FloorStartMS(request.StartMS, request.Timeframe)
		sourceRequest.Limit = 0
	} else if request.Limit > 0 {
		reference := request.EndMS
		if reference <= 0 {
			reference = market.NowMS()
		}
		windowStart := retention.BarWindowStartMS(request.Timeframe, reference, request.Limit+1)
		sourceRequest.StartMS = timeframe.FloorStartMS(windowStart, sourceTF)
		if sourceRequest.StartMS <= 0 {
			sourceRequest.StartMS = 1
		}
		sourceRequest.Limit = 0
	}
	if request.EndMS > 0 {
		targetStart := timeframe.FloorStartMS(request.EndMS, request.Timeframe)
		sourceRequest.EndMS = timeframe.EndMS(targetStart, request.Timeframe)
	}
	sourceBars, sourceErr := fetcher.FetchKlines(ctx, client, sourceRequest)
	if sourceErr != nil {
		return nil, "", fmt.Errorf("derive %s from %s after target interval error: %w", request.Timeframe, sourceTF, sourceErr)
	}
	return deriveBackfillBars(request, sourceBars), "rollup:" + sourceTF, nil
}

func deriveBackfillBars(request exchange.KlineRequest, sourceBars []market.Bar) []market.Bar {
	groups := make(map[int64][]market.Bar)
	for _, bar := range sourceBars {
		if !bar.IsFinal {
			continue
		}
		startMS := timeframe.FloorStartMS(bar.StartMS, request.Timeframe)
		groups[startMS] = append(groups[startMS], bar)
	}
	starts := make([]int64, 0, len(groups))
	for startMS := range groups {
		starts = append(starts, startMS)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })

	nowMS := market.NowMS()
	out := make([]market.Bar, 0, len(starts))
	previousClose := float64(0)
	for _, startMS := range starts {
		if timeframe.EndMS(startMS, request.Timeframe) >= nowMS {
			continue
		}
		bar := aggregator.RollupBars(request.Timeframe, groups[startMS], true, "exchange_kline_backfill_rollup", nowMS)
		if bar == nil {
			continue
		}
		bar.Source = "rest_rollup"
		decorated := aggregator.ApplyDerived(*bar, previousClose)
		bar = &decorated
		previousClose = bar.ClosePrice
		if request.StartMS > 0 && bar.StartMS < request.StartMS {
			continue
		}
		if request.EndMS > 0 && bar.StartMS > request.EndMS {
			continue
		}
		out = append(out, *bar)
	}
	if request.Limit > 0 && len(out) > request.Limit {
		out = out[len(out)-request.Limit:]
	}
	return out
}

func sourceLimitForTarget(targetTF string, sourceTF string, targetLimit int) int {
	if targetLimit <= 0 {
		return 0
	}
	targetStart := timeframe.FloorStartMS(market.NowMS(), targetTF)
	targetEnd := timeframe.EndMS(targetStart, targetTF)
	sourceStart := timeframe.FloorStartMS(targetStart, sourceTF)
	perTarget := 0
	for sourceStart <= targetEnd && perTarget < 10_000 {
		perTarget++
		next := timeframe.NextStartMS(sourceStart, sourceTF)
		if next <= sourceStart {
			break
		}
		sourceStart = next
	}
	limit := (targetLimit + 1) * perTarget
	if limit > 100_000 {
		return 100_000
	}
	return limit
}

func clearRedisKlineCache(ctx context.Context, redisCache *cache.RedisMarketCache, exchangeName string, symbols []string, options runOptions) (int64, error) {
	if options.dryRun {
		log.Printf("dry-run clear redis exchange=%s symbols=%d timeframes=%d include_recent=true include_live=%t", exchangeName, len(symbols), len(options.timeframes), options.clearLive)
		return 0, nil
	}
	var total int64
	for _, symbol := range symbols {
		for _, tf := range options.timeframes {
			count, err := redisCache.ClearKlineCache(ctx, cache.KlineCacheClearOptions{
				Exchange:      exchangeName,
				Symbol:        symbol,
				Timeframe:     tf,
				IncludeRecent: true,
				IncludeLive:   options.clearLive,
				ScanCount:     options.redisScanCount,
			})
			if err != nil {
				return total, err
			}
			total += count
		}
	}
	log.Printf("cleared redis exchange=%s keys=%d", exchangeName, total)
	return total, nil
}

func enabledExchangeCSV(configs []config.ExchangeConfig) string {
	var out []string
	for _, cfg := range configs {
		if cfg.Enabled {
			out = append(out, cfg.Name)
		}
	}
	return strings.Join(out, ",")
}

func normalizeCSV(raw string) []string {
	var out []string
	seen := map[string]bool{}
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		key := strings.ToLower(item)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, item)
	}
	return out
}

func normalizeTimeframeCSV(raw string) []string {
	var out []string
	seen := map[string]bool{}
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" || seen[item] {
			continue
		}
		seen[item] = true
		out = append(out, item)
	}
	return out
}

func normalizeSymbolSet(raw string) map[string]bool {
	items := normalizeCSV(raw)
	if len(items) == 0 {
		return nil
	}
	out := make(map[string]bool, len(items))
	for _, item := range items {
		out[strings.ToUpper(item)] = true
	}
	return out
}

func normalizeLowerSet(raw string) map[string]bool {
	items := normalizeCSV(raw)
	if len(items) == 0 {
		return nil
	}
	out := make(map[string]bool, len(items))
	for _, item := range items {
		out[strings.ToLower(strings.TrimSpace(item))] = true
	}
	return out
}

func parseTimeMS(raw string) (int64, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return 0, nil
	}
	if parsed, err := strconv.ParseInt(value, 10, 64); err == nil {
		return parsed, nil
	}
	formats := []string{
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	for _, format := range formats {
		if parsed, err := time.ParseInLocation(format, value, time.UTC); err == nil {
			return parsed.UnixMilli(), nil
		}
	}
	return 0, fmt.Errorf("unsupported time format %q", raw)
}
