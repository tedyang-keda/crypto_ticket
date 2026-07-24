package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"crypto-ticket/internal/cache"
	"crypto-ticket/internal/config"
	"crypto-ticket/internal/exchange"
	"crypto-ticket/internal/market"
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
	exchanges      []string
	symbols        map[string]bool
	timeframes     []string
	limit          int
	startMS        int64
	endMS          int64
	batchSize      int
	requestDelay   time.Duration
	backfill       bool
	clearRedis     bool
	clearLive      bool
	clearAllBars   bool
	clearAllRedis  bool
	refreshSymbols bool
	continueOnErr  bool
	dryRun         bool
	redisScanCount int64
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
	runtimes := makeExchangeRuntimes(cfg.Exchanges, options.exchanges)
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
		symbols, err := loadSymbols(ctx, store, httpClient, runtime.adapter, runtime.config.Name, options.symbols, options.refreshSymbols)
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
	var symbolsRaw string
	var timeframesRaw string
	var startRaw string
	var endRaw string
	var requestDelayMS int
	var options runOptions

	flag.StringVar(&exchangesRaw, "exchanges", enabledExchangeCSV(cfg.Exchanges), "comma-separated exchanges, default enabled exchanges from env")
	flag.StringVar(&symbolsRaw, "symbols", "", "comma-separated exact symbols; default active symbols from symbol_registry")
	flag.StringVar(&timeframesRaw, "timeframes", strings.Join(cfg.Timeframes, ","), "comma-separated timeframes")
	flag.IntVar(&options.limit, "limit", cfg.RecentCacheLimit, "max bars to fetch per exchange/symbol/timeframe; 0 means no cap when -start is set")
	flag.StringVar(&startRaw, "start", "", "optional inclusive start time: unix ms, RFC3339, or YYYY-MM-DD")
	flag.StringVar(&endRaw, "end", "", "optional inclusive end time: unix ms, RFC3339, or YYYY-MM-DD")
	flag.IntVar(&options.batchSize, "batch-size", 500, "MySQL upsert batch size")
	flag.IntVar(&requestDelayMS, "request-delay-ms", 100, "delay between exchange REST requests")
	flag.BoolVar(&options.backfill, "backfill", true, "fetch official REST klines and upsert bar_history")
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
	options.symbols = normalizeSymbolSet(symbolsRaw)

	frames := normalizeCSV(timeframesRaw)
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
	options.startMS = 0
	options.endMS = 0
	var err error
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

func makeExchangeRuntimes(configs []config.ExchangeConfig, selected []string) []exchangeRuntime {
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
	},
	client *http.Client,
	fetcher exchange.RESTKlineFetcher,
	symbols []string,
	options runOptions,
) (int, error) {
	var total int
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
		for _, tf := range options.timeframes {
			bars, err := fetcher.FetchKlines(ctx, client, exchange.KlineRequest{
				Symbol:          symbol,
				Timeframe:       tf,
				StartMS:         options.startMS,
				EndMS:           options.endMS,
				Limit:           options.limit,
				ForwardAdjusted: true,
			})
			if err != nil {
				if errors.Is(err, exchange.ErrUnsupportedKlineInterval) {
					log.Printf("skip unsupported exchange=%s symbol=%s timeframe=%s err=%v", fetcher.Name(), symbol, tf, err)
					continue
				}
				if options.continueOnErr {
					log.Printf("skip failed exchange=%s symbol=%s timeframe=%s err=%v", fetcher.Name(), symbol, tf, err)
					continue
				}
				return total, err
			}
			total += len(bars)
			log.Printf("fetched exchange=%s symbol=%s timeframe=%s bars=%d", fetcher.Name(), symbol, tf, len(bars))
			for _, bar := range bars {
				batch = append(batch, bar)
				if len(batch) >= options.batchSize {
					if err := flush(); err != nil {
						return total, err
					}
				}
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
