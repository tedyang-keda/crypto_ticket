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
	"strconv"
	"strings"
	"syscall"
	"time"

	"crypto-ticket/internal/config"
	"crypto-ticket/internal/exchange"
	"crypto-ticket/internal/market"
	mysqlstore "crypto-ticket/internal/storage/mysql"
	"crypto-ticket/internal/timeframe"
)

type options struct {
	exchange     string
	symbol       string
	boundaryMS   int64
	timeframes   []string
	before       int
	after        int
	requestDelay time.Duration
	tolerance    float64
}

type comparison struct {
	timeframe       string
	officialRows    int
	comparedRows    int
	missingLocal    int
	missingOfficial int
	mismatches      int
}

func main() {
	opts, err := parseOptions()
	if err != nil {
		log.Fatalf("invalid options: %v", err)
	}
	cfg := config.Load()
	exchangeCfg, err := findExchangeConfig(cfg.Exchanges, opts.exchange)
	if err != nil {
		log.Fatal(err)
	}
	fetcher := newFetcher(exchangeCfg)
	store, err := mysqlstore.New(cfg.MySQLDSN)
	if err != nil {
		log.Fatalf("connect mysql: %v", err)
	}
	defer store.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	client := &http.Client{Timeout: 30 * time.Second}
	totalMismatch := 0
	for _, tf := range opts.timeframes {
		result, err := compareTimeframe(ctx, store, fetcher, client, opts, tf)
		if err != nil {
			log.Fatalf("compare %s/%s/%s: %v", opts.exchange, opts.symbol, tf, err)
		}
		totalMismatch += result.missingLocal + result.missingOfficial + result.mismatches
		status := "PASS"
		if result.missingLocal+result.missingOfficial+result.mismatches > 0 {
			status = "FAIL"
		}
		log.Printf("%s exchange=%s symbol=%s timeframe=%s official=%d compared=%d missing_local=%d missing_official=%d mismatches=%d",
			status, opts.exchange, opts.symbol, tf, result.officialRows, result.comparedRows,
			result.missingLocal, result.missingOfficial, result.mismatches)
		if err := wait(ctx, opts.requestDelay); err != nil {
			log.Fatal(err)
		}
	}
	if totalMismatch > 0 {
		log.Fatalf("verification failed exchange=%s symbol=%s total_issues=%d", opts.exchange, opts.symbol, totalMismatch)
	}
	log.Printf("verification passed exchange=%s symbol=%s timeframes=%d", opts.exchange, opts.symbol, len(opts.timeframes))
}

type rangeStore interface {
	BarsInRange(ctx context.Context, exchange string, symbol string, timeframe string, startMS int64, endMS int64) ([]market.Bar, error)
}

func compareTimeframe(ctx context.Context, store rangeStore, fetcher exchange.RESTKlineFetcher, client *http.Client, opts options, tf string) (comparison, error) {
	result := comparison{timeframe: tf}
	startMS := timeframe.FloorStartMS(opts.boundaryMS, tf)
	for i := 0; i < opts.before; i++ {
		startMS = timeframe.FloorStartMS(startMS-1, tf)
	}
	endStartMS := timeframe.FloorStartMS(opts.boundaryMS, tf)
	for i := 0; i < opts.after; i++ {
		endStartMS = timeframe.NextStartMS(endStartMS, tf)
	}
	endMS := timeframe.EndMS(endStartMS, tf)

	local, err := store.BarsInRange(ctx, opts.exchange, opts.symbol, tf, startMS, endMS)
	if err != nil {
		return result, fmt.Errorf("load local bars: %w", err)
	}
	official, err := fetchWithRetry(ctx, fetcher, client, exchange.KlineRequest{
		Symbol: opts.symbol, Timeframe: tf, StartMS: startMS, EndMS: endMS,
	})
	if err != nil {
		return result, fmt.Errorf("load official bars: %w", err)
	}
	result.officialRows = len(official)
	localByStart := make(map[int64]market.Bar, len(local))
	for _, bar := range local {
		localByStart[bar.StartMS] = bar
	}
	officialByStart := make(map[int64]market.Bar, len(official))
	for _, bar := range official {
		officialByStart[bar.StartMS] = bar
		localBar, ok := localByStart[bar.StartMS]
		if !ok {
			result.missingLocal++
			log.Printf("MISSING_LOCAL exchange=%s symbol=%s timeframe=%s start_ms=%d", opts.exchange, opts.symbol, tf, bar.StartMS)
			continue
		}
		result.comparedRows++
		for _, mismatch := range compareBar(localBar, bar, opts.tolerance) {
			result.mismatches++
			log.Printf("MISMATCH exchange=%s symbol=%s timeframe=%s start_ms=%d field=%s local=%.12g official=%.12g",
				opts.exchange, opts.symbol, tf, bar.StartMS, mismatch.field, mismatch.local, mismatch.official)
		}
	}
	for _, bar := range local {
		if _, ok := officialByStart[bar.StartMS]; !ok {
			result.missingOfficial++
			log.Printf("MISSING_OFFICIAL exchange=%s symbol=%s timeframe=%s start_ms=%d", opts.exchange, opts.symbol, tf, bar.StartMS)
		}
	}
	return result, nil
}

type fieldMismatch struct {
	field    string
	local    float64
	official float64
}

func compareBar(local market.Bar, official market.Bar, tolerance float64) []fieldMismatch {
	fields := []struct {
		name     string
		local    float64
		official float64
	}{
		{"open", local.OpenPrice, official.OpenPrice},
		{"high", local.HighPrice, official.HighPrice},
		{"low", local.LowPrice, official.LowPrice},
		{"close", local.ClosePrice, official.ClosePrice},
		{"volume", local.Volume, official.Volume},
		{"quote_volume", local.QuoteVolume, official.QuoteVolume},
		{"contract_volume", local.ContractVolume, official.ContractVolume},
		{"trade_count", float64(local.TradeCount), float64(official.TradeCount)},
	}
	out := make([]fieldMismatch, 0)
	for _, field := range fields {
		scale := math.Max(1, math.Abs(field.official))
		if math.Abs(field.local-field.official) > tolerance*scale {
			out = append(out, fieldMismatch{field: field.name, local: field.local, official: field.official})
		}
	}
	return out
}

func fetchWithRetry(ctx context.Context, fetcher exchange.RESTKlineFetcher, client *http.Client, request exchange.KlineRequest) ([]market.Bar, error) {
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		bars, err := fetcher.FetchKlines(ctx, client, request)
		if err == nil {
			return bars, nil
		}
		lastErr = err
		if err := wait(ctx, time.Duration(1<<attempt)*500*time.Millisecond); err != nil {
			return nil, err
		}
	}
	return nil, lastErr
}

func parseOptions() (options, error) {
	var opts options
	var boundaryRaw string
	var timeframesRaw string
	var delayMS int
	flag.StringVar(&opts.exchange, "exchange", "", "required exchange: binance or okx")
	flag.StringVar(&opts.symbol, "symbol", "", "required exact exchange symbol")
	flag.StringVar(&boundaryRaw, "boundary", "", "required action boundary: unix ms, RFC3339, or YYYY-MM-DD")
	flag.StringVar(&timeframesRaw, "timeframes", "1m,5m,15m,30m,1H,2H,4H,6H,12H,1D", "comma-separated timeframes")
	flag.IntVar(&opts.before, "before", 2, "number of buckets before the boundary bucket")
	flag.IntVar(&opts.after, "after", 2, "number of buckets after the boundary bucket")
	flag.IntVar(&delayMS, "request-delay-ms", 300, "delay between official REST requests")
	flag.Float64Var(&opts.tolerance, "tolerance", 1e-8, "relative numeric comparison tolerance")
	flag.Parse()

	opts.exchange = strings.ToLower(strings.TrimSpace(opts.exchange))
	opts.symbol = strings.ToUpper(strings.TrimSpace(opts.symbol))
	if opts.exchange != "binance" && opts.exchange != "okx" {
		return opts, errors.New("-exchange must be binance or okx")
	}
	if opts.symbol == "" {
		return opts, errors.New("-symbol is required")
	}
	if strings.TrimSpace(boundaryRaw) == "" {
		return opts, errors.New("-boundary is required")
	}
	var err error
	opts.boundaryMS, err = parseTimeMS(boundaryRaw)
	if err != nil {
		return opts, err
	}
	if opts.before < 0 || opts.after < 0 || delayMS < 0 || opts.tolerance <= 0 {
		return opts, errors.New("before/after/delay must be non-negative and tolerance must be positive")
	}
	opts.requestDelay = time.Duration(delayMS) * time.Millisecond
	for _, item := range strings.Split(timeframesRaw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		tf, err := timeframe.Normalize(item)
		if err != nil {
			return opts, err
		}
		opts.timeframes = append(opts.timeframes, tf)
	}
	if len(opts.timeframes) == 0 {
		return opts, errors.New("at least one timeframe is required")
	}
	return opts, nil
}

func findExchangeConfig(configs []config.ExchangeConfig, exchangeName string) (config.ExchangeConfig, error) {
	for _, cfg := range configs {
		if cfg.Name != exchangeName {
			continue
		}
		if exchangeName == "binance" && strings.Contains(strings.ToLower(cfg.MarketType), "coin") {
			continue
		}
		if exchangeName == "okx" && !strings.EqualFold(cfg.MarketType, "SWAP") {
			continue
		}
		return cfg, nil
	}
	return config.ExchangeConfig{}, fmt.Errorf("%s exchange config not found", exchangeName)
}

func newFetcher(cfg config.ExchangeConfig) exchange.RESTKlineFetcher {
	if cfg.Name == "okx" {
		return exchange.NewOKXAdapter(cfg.MarketType, cfg.RestURL, cfg.WSURL)
	}
	return exchange.NewBinanceFuturesAdapter(cfg.MarketType, cfg.RestURL, cfg.WSURL)
}

func parseTimeMS(raw string) (int64, error) {
	value := strings.TrimSpace(raw)
	if parsed, err := strconv.ParseInt(value, 10, 64); err == nil {
		return parsed, nil
	}
	for _, format := range []string{time.RFC3339, "2006-01-02 15:04:05", "2006-01-02"} {
		if parsed, err := time.ParseInLocation(format, value, time.UTC); err == nil {
			return parsed.UnixMilli(), nil
		}
	}
	return 0, fmt.Errorf("unsupported time format %q", raw)
}

func wait(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
