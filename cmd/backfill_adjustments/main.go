package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"crypto-ticket/internal/adjustment"
	"crypto-ticket/internal/config"
	"crypto-ticket/internal/exchange"
	"crypto-ticket/internal/market"
	mysqlstore "crypto-ticket/internal/storage/mysql"
)

type options struct {
	exchange             string
	symbols              []string
	startMS              int64
	endMS                int64
	maxPages             int
	requestDelay         time.Duration
	boundaryTolerancePct float64
	dryRun               bool
	continueOnError      bool
}

func main() {
	cfg := config.Load()
	opts, err := parseOptions()
	if err != nil {
		log.Fatalf("invalid options: %v", err)
	}
	exchangeConfig, err := adjustmentExchangeConfig(cfg.Exchanges, opts.exchange)
	if err != nil {
		log.Fatal(err)
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

	client := &http.Client{Timeout: 30 * time.Second}
	actions, source, err := historicalActions(ctx, cfg, exchangeConfig, client, opts)
	if err != nil {
		log.Fatalf("scan %s announcements: %v", opts.exchange, err)
	}
	if !opts.dryRun {
		if err := recordNoActionCoverage(ctx, store, opts, exchangeConfig, actions); err != nil {
			log.Fatalf("record %s no-action coverage: %v", opts.exchange, err)
		}
	}
	if len(actions) == 0 {
		log.Printf("no %s corporate actions found range=[%d,%d] symbols=%v", opts.exchange, opts.startMS, opts.endMS, opts.symbols)
		return
	}

	backfiller := adjustment.NewHistoricalBackfiller(store, source, client, adjustment.HistoricalBackfillConfig{
		BoundaryTolerancePct: opts.boundaryTolerancePct,
		RequestDelay:         opts.requestDelay,
		DryRun:               opts.dryRun,
	})
	succeeded := 0
	skipped := 0
	failed := 0
	for _, action := range actions {
		result, err := backfiller.Backfill(ctx, action)
		if err != nil {
			failed++
			if opts.continueOnError {
				log.Printf("FAILED symbol=%s code=%s ratio=%.8f: %v", action.Symbol, action.AnnouncementCode, action.Ratio, err)
				continue
			}
			log.Fatalf("backfill symbol=%s code=%s: %v", action.Symbol, action.AnnouncementCode, err)
		}
		if result.AlreadyExists {
			skipped++
			log.Printf("SKIP existing symbol=%s boundary=%d code=%s", action.Symbol, result.Boundary.BoundaryMS, action.AnnouncementCode)
			continue
		}
		succeeded++
		log.Printf("%s symbol=%s boundary=%d official_ratio=%.8f observed_ratio=%.8f factors=%d bars=%d code=%s",
			writeMode(opts.dryRun), action.Symbol, result.Boundary.BoundaryMS, action.Ratio,
			result.Boundary.Ratio, len(result.Segments), result.BarsFetched, action.AnnouncementCode)
		if opts.requestDelay > 0 {
			if err := wait(ctx, opts.requestDelay); err != nil {
				log.Fatal(err)
			}
		}
	}
	log.Printf("done found=%d succeeded=%d existing=%d failed=%d dry_run=%v", len(actions), succeeded, skipped, failed, opts.dryRun)
}

func recordNoActionCoverage(ctx context.Context, store adjustment.HistoricalBackfillStore, opts options, exchangeConfig config.ExchangeConfig, actions []adjustment.HistoricalAction) error {
	if opts.exchange != "okx" || len(opts.symbols) == 0 {
		return nil
	}
	actionSymbols := make(map[string]bool, len(actions)*2)
	for _, action := range actions {
		actionSymbols[strings.ToUpper(action.Symbol)] = true
		if action.PredecessorSymbol != "" {
			actionSymbols[strings.ToUpper(action.PredecessorSymbol)] = true
		}
	}
	sourceMarket := market.SourceMarket(opts.exchange, exchangeConfig.MarketType)
	for _, requested := range opts.symbols {
		symbol := strings.ToUpper(strings.TrimSpace(requested))
		if opts.exchange == "okx" {
			symbol = exchange.NormalizeOKXInstrumentID(symbol)
		}
		if symbol == "" || actionSymbols[symbol] {
			continue
		}
		raw, _ := json.Marshal(map[string]any{
			"method": "historical_announcement_scan", "exchange": opts.exchange,
			"start_ms": opts.startMS, "end_ms": opts.endMS, "result": "no_corporate_action",
		})
		event := market.CorporateActionEvent{
			ActionID: fmt.Sprintf("%s|%s|%s|coverage|%d|%d", opts.exchange, sourceMarket, symbol, opts.startMS, opts.endMS),
			Exchange: opts.exchange, SourceMarket: sourceMarket, Symbol: symbol,
			EventType: market.CorporateActionEventHistoricalCoverage, State: market.CorporateActionStateNotRequired,
			FirstSeenMS: opts.startMS, LastEventMS: opts.endMS, Raw: raw, UpdatedAtMS: market.NowMS(),
		}
		if err := store.UpsertCorporateActionEvent(ctx, event); err != nil {
			return err
		}
		log.Printf("COVERED no-action symbol=%s range=[%d,%d]", symbol, opts.startMS, opts.endMS)
	}
	return nil
}

func parseOptions() (options, error) {
	var out options
	var symbolsRaw string
	var startRaw string
	var endRaw string
	var requestDelayMS int
	flag.StringVar(&out.exchange, "exchange", "binance", "announcement and kline exchange: binance or okx")
	flag.StringVar(&symbolsRaw, "symbols", "", "optional comma-separated exact exchange symbols")
	flag.StringVar(&startRaw, "start", "", "required inclusive start: unix ms, RFC3339, or YYYY-MM-DD")
	flag.StringVar(&endRaw, "end", "", "optional inclusive end; default now")
	flag.IntVar(&out.maxPages, "max-pages", 50, "maximum announcement pages to scan")
	flag.IntVar(&requestDelayMS, "request-delay-ms", 200, "delay between announcement requests")
	flag.Float64Var(&out.boundaryTolerancePct, "boundary-tolerance-pct", 0.25, "maximum observed/official ratio divergence as a fraction")
	flag.BoolVar(&out.dryRun, "dry-run", false, "calculate and log without writing bars, factors, or events")
	flag.BoolVar(&out.continueOnError, "continue-on-error", false, "continue when one announcement cannot be backfilled")
	flag.Parse()
	out.exchange = strings.ToLower(strings.TrimSpace(out.exchange))
	if out.exchange != "binance" && out.exchange != "okx" {
		return out, errors.New("-exchange must be binance or okx")
	}

	if strings.TrimSpace(startRaw) == "" {
		return out, errors.New("-start is required")
	}
	var err error
	out.startMS, err = parseTimeMS(startRaw)
	if err != nil {
		return out, fmt.Errorf("parse -start: %w", err)
	}
	if strings.TrimSpace(endRaw) == "" {
		out.endMS = time.Now().UnixMilli()
	} else if out.endMS, err = parseTimeMS(endRaw); err != nil {
		return out, fmt.Errorf("parse -end: %w", err)
	}
	if out.startMS > out.endMS {
		return out, errors.New("-start must be <= -end")
	}
	if out.maxPages <= 0 {
		return out, errors.New("-max-pages must be positive")
	}
	if out.boundaryTolerancePct <= 0 || out.boundaryTolerancePct > 1 {
		return out, errors.New("-boundary-tolerance-pct must be in (0,1]")
	}
	if requestDelayMS < 0 {
		return out, errors.New("-request-delay-ms must be non-negative")
	}
	out.requestDelay = time.Duration(requestDelayMS) * time.Millisecond
	for _, item := range strings.Split(symbolsRaw, ",") {
		if symbol := strings.ToUpper(strings.TrimSpace(item)); symbol != "" {
			out.symbols = append(out.symbols, symbol)
		}
	}
	return out, nil
}

func adjustmentExchangeConfig(configs []config.ExchangeConfig, exchangeName string) (config.ExchangeConfig, error) {
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
	return config.ExchangeConfig{}, fmt.Errorf("%s adjustment exchange config not found", exchangeName)
}

func historicalActions(ctx context.Context, cfg config.Config, exchangeConfig config.ExchangeConfig, client *http.Client, opts options) ([]adjustment.HistoricalAction, adjustment.HistoricalKlineSource, error) {
	sourceMarket := market.SourceMarket(opts.exchange, exchangeConfig.MarketType)
	if opts.exchange == "okx" {
		verifier := exchange.NewOKXAnnouncementVerifier(exchangeConfig.RestURL, client)
		found, err := verifier.ListCorporateActions(ctx, exchange.OKXAnnouncementQuery{
			StartMS: opts.startMS, EndMS: opts.endMS, Symbols: opts.symbols,
			MaxPages: opts.maxPages, RequestDelay: opts.requestDelay,
		})
		actions := make([]adjustment.HistoricalAction, 0, len(found))
		for _, action := range found {
			actions = append(actions, adjustment.HistoricalAction{
				Exchange: "okx", SourceMarket: sourceMarket, Symbol: action.Symbol, PredecessorSymbol: action.PredecessorSymbol,
				Ratio: action.Ratio, WindowStartMS: action.WindowStartMS, WindowEndMS: action.WindowEndMS,
				PublishedMS: action.PublishedMS, AnnouncementCode: action.AnnouncementCode, Title: action.Title, Raw: action.Raw,
			})
		}
		return actions, exchange.NewOKXAdapter(exchangeConfig.MarketType, exchangeConfig.RestURL, exchangeConfig.WSURL), err
	}

	verifier := exchange.NewBinanceAnnouncementVerifier(cfg.BinanceCMSURL, client)
	found, err := verifier.ListCorporateActions(ctx, exchange.BinanceAnnouncementQuery{
		StartMS: opts.startMS, EndMS: opts.endMS, Symbols: opts.symbols,
		MaxPages: opts.maxPages, RequestDelay: opts.requestDelay,
	})
	actions := make([]adjustment.HistoricalAction, 0, len(found))
	for _, action := range found {
		actions = append(actions, adjustment.HistoricalAction{
			Exchange: "binance", SourceMarket: sourceMarket, Symbol: action.Symbol, Ratio: action.Ratio,
			WindowStartMS: action.WindowStartMS, WindowEndMS: action.WindowEndMS, PublishedMS: action.PublishedMS,
			AnnouncementCode: action.AnnouncementCode, Title: action.Title, Raw: action.Raw,
		})
	}
	return actions, exchange.NewBinanceFuturesAdapter(exchangeConfig.MarketType, exchangeConfig.RestURL, exchangeConfig.WSURL), err
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
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func writeMode(dryRun bool) string {
	if dryRun {
		return "DRY-RUN"
	}
	return "WRITE"
}
