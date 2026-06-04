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
	"crypto-ticket/internal/collector"
	"crypto-ticket/internal/config"
	"crypto-ticket/internal/exchange"
	"crypto-ticket/internal/guardian"
	"crypto-ticket/internal/market"
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

	hub := realtime.NewHub()
	marketService := app.NewMarketService(store, hub, cfg.Timeframes, cfg.RecentCacheLimit)
	server := api.NewServer(marketService, hub, cfg.DashboardDir)

	errCh := make(chan error, 8)
	startBackgroundWorkers(ctx, cfg, store, marketService, errCh)
	httpServer := &http.Server{Addr: cfg.HTTPAddr, Handler: server.Handler()}
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
	errCh chan<- error,
) {
	if cfg.EnableCollector {
		runtimes := makeCollectorRuntimes(cfg.Exchanges, cfg)
		runner := collector.NewRunner(runtimes, store, marketService)
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
		})
		marketService.AddFinalBarObserver(worker)
		go func() {
			if err := worker.Run(ctx); err != nil && ctx.Err() == nil {
				errCh <- err
			}
		}()
		log.Printf("kline guardian started fetchers=%d interval=%ds window=%dm delay=%ds",
			len(fetchers),
			cfg.KlineGuardianAuditIntervalSeconds,
			cfg.KlineGuardianWindowMinutes,
			cfg.KlineGuardianDelaySeconds,
		)
	}
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
