package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"crypto-ticket/internal/api"
	"crypto-ticket/internal/app"
	"crypto-ticket/internal/cache"
	"crypto-ticket/internal/collector"
	"crypto-ticket/internal/config"
	"crypto-ticket/internal/corporateaction"
	"crypto-ticket/internal/exchange"
	"crypto-ticket/internal/guardian"
	"crypto-ticket/internal/market"
	"crypto-ticket/internal/monitoring"
	"crypto-ticket/internal/realtime"
	"crypto-ticket/internal/storage"
	mysqlstore "crypto-ticket/internal/storage/mysql"
)

func main() {
	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var store storage.HistoricalStore
	var storeCloser interface{ Close() error }
	if cfg.UseMemory {
		store = storage.NewMemoryHistoricalStore()
	} else {
		mysql, err := mysqlstore.New(cfg.MySQLDSN)
		if err != nil {
			log.Fatalf("connect mysql: %v", err)
		}
		store = mysql
		storeCloser = mysql
	}
	if storeCloser != nil {
		defer storeCloser.Close()
	}
	if err := store.EnsureSchema(ctx); err != nil {
		log.Fatalf("ensure schema: %v", err)
	}
	if cfg.EnableMockSymbols {
		_ = store.UpsertSymbols(ctx, []market.SymbolInfo{
			{Exchange: "binance", Symbol: "BTCUSDT", MarketType: "um_futures", Status: "TRADING", IsActive: true},
			{Exchange: "binance", Symbol: "ETHUSDT", MarketType: "um_futures", Status: "TRADING", IsActive: true},
			{Exchange: "okx", Symbol: "BTC-USDT-SWAP", MarketType: "SWAP", Status: "live", IsActive: true},
		})
	}

	registry := monitoring.NewRegistry()
	var pinger monitoring.Pinger
	if candidate, ok := store.(monitoring.Pinger); ok {
		pinger = candidate
	}
	var activityStore monitoring.ActivityStore
	if candidate, ok := store.(monitoring.ActivityStore); ok {
		activityStore = candidate
	}
	var monitorDependencies monitoring.Dependencies
	if cfg.MonitorRedisEnabled {
		redisMonitor, err := cache.NewRedisMonitor(cfg.RedisURL)
		if err != nil {
			log.Printf("Redis monitoring disabled: %v", err)
		} else {
			defer redisMonitor.Close()
			monitorDependencies.Redis = redisMonitor
		}
	}
	monitorService := monitoring.NewService(registry, pinger, activityStore, cfg.FeishuWebhookURL, monitoring.Config{
		Enabled: cfg.EnableHealthMonitor, CollectorEnabled: cfg.EnableCollector, P1AlertsEnabled: cfg.MonitorP1AlertsEnabled,
		EvaluationInterval:   time.Duration(cfg.MonitorEvaluationSeconds) * time.Second,
		DailyReportHour:      cfg.MonitorDailyReportHour,
		MarketReportInterval: time.Duration(cfg.MonitorMarketReportIntervalMinutes) * time.Minute,
		DiskPath:             cfg.MonitorDiskPath,
	}, monitorDependencies)
	hub := realtime.NewHub(registry)
	marketService := app.NewMarketService(store, hub, cfg.Timeframes, cfg.RecentCacheLimit, registry)
	server := api.NewServer(marketService, hub, cfg.DashboardDir, monitorService)

	errCh := make(chan error, 8)
	startBackgroundWorkers(ctx, cfg, store, marketService, monitorService, registry, errCh)
	httpServer := &http.Server{Addr: cfg.HTTPAddr, Handler: server.Handler(), ConnState: registry.HTTPConnectionState}
	go func() {
		log.Printf("marketd listening on http://%s", cfg.HTTPAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
	case err := <-errCh:
		log.Printf("marketd worker stopped: %v", err)
		stop()
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("http shutdown failed: %v", err)
	}
}

func startBackgroundWorkers(
	ctx context.Context,
	cfg config.Config,
	store storage.HistoricalStore,
	marketService *app.MarketService,
	monitorService *monitoring.Service,
	registry *monitoring.Registry,
	errCh chan<- error,
) {
	if cfg.EnableHealthMonitor {
		go func() {
			if err := monitorService.Run(ctx); err != nil && ctx.Err() == nil {
				errCh <- err
			}
		}()
		integrity := monitoring.NewIntegrityAuditor(store, registry, monitoring.IntegrityConfig{
			Enabled: true, Exchanges: enabledExchangeNames(cfg.Exchanges), Timeframes: cfg.MonitorIntegrityTimeframes,
			Interval:      time.Duration(cfg.MonitorIntegrityIntervalSeconds) * time.Second,
			SymbolsPerRun: cfg.MonitorIntegritySymbolsPerRun,
		})
		go func() {
			if err := integrity.Run(ctx); err != nil && ctx.Err() == nil {
				errCh <- err
			}
		}()
		log.Printf("health monitoring started interval=%ds p1_alerts=%t integrity=%ds/%d",
			cfg.MonitorEvaluationSeconds, cfg.MonitorP1AlertsEnabled,
			cfg.MonitorIntegrityIntervalSeconds, cfg.MonitorIntegritySymbolsPerRun)
	}
	if cfg.EnableCollector {
		runtimes := makeCollectorRuntimes(cfg.Exchanges, cfg)
		runner := collector.NewRunner(runtimes, store, marketService, registry)
		go func() {
			if err := runner.Run(ctx); err != nil && ctx.Err() == nil {
				errCh <- err
			}
		}()
		log.Printf("kline collector started runtimes=%d", len(runtimes))
	}
	if cfg.EnableKlineGuardian {
		guardianStore, ok := store.(guardian.Store)
		if !ok {
			log.Printf("kline guardian disabled: store does not implement guardian state interface")
			return
		}
		fetchers := makeKlineGuardianFetchers(cfg.Exchanges)
		if len(fetchers) == 0 {
			log.Printf("kline guardian disabled: no REST kline fetchers")
			return
		}
		worker := guardian.New(guardianStore, marketService, fetchers, guardian.Config{
			Enabled:       true,
			AuditInterval: time.Duration(cfg.KlineGuardianAuditIntervalSeconds) * time.Second,
			AuditWindow:   time.Duration(cfg.KlineGuardianWindowMinutes) * time.Minute,
			AuditDelay:    time.Duration(cfg.KlineGuardianDelaySeconds) * time.Second,
			SymbolsPerRun: cfg.KlineGuardianSymbolsPerRun,
			RequestDelay:  time.Duration(cfg.KlineGuardianRequestDelayMS) * time.Millisecond,
			RequestIntervals: map[string]time.Duration{
				"binance": requestInterval(cfg.KlineGuardianBinanceRPS),
				"okx":     requestInterval(cfg.KlineGuardianOKXRPS),
			},
			SymbolMaxAge: time.Duration(cfg.KlineGuardianSymbolMaxAgeSeconds) * time.Second,
			Observer:     registry,
		})
		marketService.AddFinalBarObserver(worker)
		go func() {
			if err := worker.Run(ctx); err != nil && ctx.Err() == nil {
				errCh <- err
			}
		}()
		log.Printf("kline guardian started fetchers=%d interval=%ds window=%dm delay=%ds symbols_per_run=%d binance_rps=%d okx_rps=%d",
			len(fetchers),
			cfg.KlineGuardianAuditIntervalSeconds,
			cfg.KlineGuardianWindowMinutes,
			cfg.KlineGuardianDelaySeconds,
			cfg.KlineGuardianSymbolsPerRun,
			cfg.KlineGuardianBinanceRPS,
			cfg.KlineGuardianOKXRPS,
		)
	}
	if cfg.EnableCorporateAction {
		corporateStore, ok := store.(corporateaction.Store)
		if !ok {
			log.Printf("corporate action worker disabled: store does not implement corporate action interface")
			return
		}
		fetchers := makeCorporateActionFetchers(cfg.Exchanges)
		if len(fetchers) == 0 {
			log.Printf("corporate action worker disabled: no REST kline fetchers")
			return
		}
		var cacheClearer corporateaction.CacheClearer
		redisCache, err := cache.NewRedisMarketCache(cfg.RedisURL)
		if err != nil {
			log.Printf("corporate action Redis cache clearing disabled: %v", err)
		} else {
			cacheClearer = redisCache
		}
		notifier := corporateaction.NewFeishuNotifier(cfg.FeishuWebhookURL, nil)
		worker := corporateaction.New(corporateStore, fetchers, notifier, cacheClearer, corporateaction.Config{
			Enabled:             true,
			Timeframes:          cfg.Timeframes,
			PollInterval:        time.Duration(cfg.CorporateActionPollSeconds) * time.Second,
			MaxAttempts:         cfg.CorporateActionMaxAttempts,
			RetryBaseDelay:      time.Duration(cfg.CorporateActionRetryBaseSeconds) * time.Second,
			JobsPerRun:          cfg.CorporateActionJobsPerRun,
			RequestDelay:        time.Duration(cfg.CorporateActionRequestDelayMS) * time.Millisecond,
			AnchorInterval:      time.Duration(cfg.CorporateActionAnchorSeconds) * time.Second,
			AnchorSymbolsPerRun: cfg.CorporateActionAnchorSymbolsPerRun,
		})
		marketService.AddFinalBarObserver(worker)
		go func() {
			if err := worker.Run(ctx); err != nil && ctx.Err() == nil {
				errCh <- err
			}
		}()
		log.Printf("corporate action worker started fetchers=%d poll=%ds max_attempts=%d anchor=%ds/%d",
			len(fetchers), cfg.CorporateActionPollSeconds, cfg.CorporateActionMaxAttempts,
			cfg.CorporateActionAnchorSeconds, cfg.CorporateActionAnchorSymbolsPerRun)
	}
}

func requestInterval(requestsPerSecond int) time.Duration {
	if requestsPerSecond <= 0 {
		return 0
	}
	return time.Second / time.Duration(requestsPerSecond)
}

func enabledExchangeNames(configs []config.ExchangeConfig) []string {
	seen := make(map[string]bool)
	var out []string
	for _, exchangeConfig := range configs {
		if exchangeConfig.Enabled && !seen[exchangeConfig.Name] {
			seen[exchangeConfig.Name] = true
			out = append(out, exchangeConfig.Name)
		}
	}
	return out
}

func makeCollectorRuntimes(configs []config.ExchangeConfig, cfg config.Config) []collector.Runtime {
	runtimes := make([]collector.Runtime, 0, len(configs))
	for _, exchangeConfig := range configs {
		if !exchangeConfig.Enabled {
			continue
		}
		var adapter exchange.Adapter
		switch exchangeConfig.Name {
		case "binance":
			adapter = exchange.NewBinanceFuturesAdapter(exchangeConfig.MarketType, exchangeConfig.RestURL, exchangeConfig.WSURL)
		case "okx":
			adapter = exchange.NewOKXAdapter(exchangeConfig.MarketType, exchangeConfig.RestURL, exchangeConfig.WSURL)
		}
		if adapter == nil {
			continue
		}
		runtimes = append(runtimes, collector.Runtime{
			Adapter: adapter,
			Config: collector.Config{
				SymbolRefreshInterval: time.Duration(cfg.SymbolRefreshIntervalSeconds) * time.Second,
				ReconnectBaseDelay:    time.Duration(cfg.ReconnectBaseDelaySeconds) * time.Second,
				ReconnectMaxDelay:     time.Duration(cfg.ReconnectMaxDelaySeconds) * time.Second,
				SubscriptionChunkSize: exchangeConfig.SubscriptionChunkSize,
			},
		})
	}
	return runtimes
}

func makeKlineGuardianFetchers(configs []config.ExchangeConfig) []guardian.Fetcher {
	fetchers := make([]guardian.Fetcher, 0, len(configs))
	for _, exchangeConfig := range configs {
		if !exchangeConfig.Enabled {
			continue
		}
		switch exchangeConfig.Name {
		case "binance":
			fetchers = append(fetchers, exchange.NewBinanceFuturesAdapter(exchangeConfig.MarketType, exchangeConfig.RestURL, exchangeConfig.WSURL))
		case "okx":
			fetchers = append(fetchers, exchange.NewOKXAdapter(exchangeConfig.MarketType, exchangeConfig.RestURL, exchangeConfig.WSURL))
		}
	}
	return fetchers
}

func makeCorporateActionFetchers(configs []config.ExchangeConfig) []corporateaction.Fetcher {
	fetchers := make([]corporateaction.Fetcher, 0, len(configs))
	for _, exchangeConfig := range configs {
		if !exchangeConfig.Enabled {
			continue
		}
		switch exchangeConfig.Name {
		case "binance":
			fetchers = append(fetchers, exchange.NewBinanceFuturesAdapter(exchangeConfig.MarketType, exchangeConfig.RestURL, exchangeConfig.WSURL))
		case "okx":
			fetchers = append(fetchers, exchange.NewOKXAdapter(exchangeConfig.MarketType, exchangeConfig.RestURL, exchangeConfig.WSURL))
		}
	}
	return fetchers
}
