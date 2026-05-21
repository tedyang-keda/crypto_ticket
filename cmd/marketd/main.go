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
	"crypto-ticket/internal/exchange"
	"crypto-ticket/internal/market"
	"crypto-ticket/internal/realtime"
	"crypto-ticket/internal/storage"
	mysqlstore "crypto-ticket/internal/storage/mysql"
	tickstream "crypto-ticket/internal/stream"
)

func main() {
	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var marketCache cache.MarketCache
	var cacheCloser interface{ Close() error }
	if cfg.UseMemory {
		marketCache = cache.NewMemoryMarketCache()
	} else {
		redisCache, err := cache.NewRedisMarketCache(cfg.RedisURL)
		if err != nil {
			log.Fatalf("create redis cache: %v", err)
		}
		marketCache = redisCache
		cacheCloser = redisCache
	}
	if cacheCloser != nil {
		defer cacheCloser.Close()
	}

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

	var tickStream *tickstream.RedisTickStream
	if !cfg.UseMemory && (cfg.EnableCollector || cfg.EnableStreamConsumer) {
		redisStream, err := tickstream.NewRedisTickStream(cfg.RedisURL)
		if err != nil {
			log.Fatalf("create redis tick stream: %v", err)
		}
		tickStream = redisStream
		defer tickStream.Close()
	}

	hub := realtime.NewHub()
	marketService := app.NewMarketService(marketCache, store, hub, cfg.Timeframes, cfg.RecentCacheLimit)
	server := api.NewServer(marketService, hub, cfg.DashboardDir)

	errCh := make(chan error, 8)
	startBackgroundWorkers(ctx, cfg, store, tickStream, marketService, errCh)
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
	ticks tickstream.TickStream,
	marketService *app.MarketService,
	errCh chan<- error,
) {
	if cfg.EnableStreamConsumer && ticks != nil {
		for _, exchangeConfig := range cfg.Exchanges {
			if !exchangeConfig.Enabled {
				continue
			}
			streamName := tickstream.NameForExchange(exchangeConfig.Name, exchangeConfig.Shard)
			consumerName := cfg.RedisConsumerName + "-" + exchangeConfig.Name
			consumer := app.NewStreamConsumer(ticks, marketService, app.StreamConsumerConfig{
				StreamName: streamName,
				Group:      cfg.RedisConsumerGroup,
				Consumer:   consumerName,
				ReadCount:  int64(cfg.StreamReadCount),
				Block:      time.Duration(cfg.StreamBlockMS) * time.Millisecond,
			})
			go func() {
				if err := consumer.Run(ctx); err != nil && ctx.Err() == nil {
					errCh <- err
				}
			}()
			log.Printf("stream consumer started stream=%s group=%s consumer=%s", streamName, cfg.RedisConsumerGroup, consumerName)
		}
	}

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := marketService.CloseDue(ctx, market.NowMS(), int64(cfg.BarCloseGraceSeconds)*1000); err != nil && ctx.Err() == nil {
					errCh <- err
					return
				}
			}
		}
	}()

	if cfg.EnableCollector && ticks != nil {
		adapters := makeExchangeAdapters(cfg.Exchanges)
		configs := make(map[string]collector.Config, len(cfg.Exchanges))
		for _, exchangeConfig := range cfg.Exchanges {
			configs[exchangeConfig.Name] = collector.Config{
				SymbolRefreshInterval: time.Duration(cfg.SymbolRefreshIntervalSeconds) * time.Second,
				ReconnectBaseDelay:    time.Duration(cfg.ReconnectBaseDelaySeconds) * time.Second,
				ReconnectMaxDelay:     time.Duration(cfg.ReconnectMaxDelaySeconds) * time.Second,
				StreamMaxLen:          cfg.RedisStreamMaxLen,
				SubscriptionChunkSize: exchangeConfig.SubscriptionChunkSize,
				Shard:                 exchangeConfig.Shard,
			}
		}
		runner := collector.NewRunner(adapters, ticks, store, configs)
		go func() {
			if err := runner.Run(ctx); err != nil && ctx.Err() == nil {
				errCh <- err
			}
		}()
		log.Printf("collector started exchanges=%d", len(adapters))
	}
}

func makeExchangeAdapters(configs []config.ExchangeConfig) []exchange.Adapter {
	adapters := make([]exchange.Adapter, 0, len(configs))
	for _, cfg := range configs {
		if !cfg.Enabled {
			continue
		}
		switch cfg.Name {
		case "binance":
			adapters = append(adapters, exchange.NewBinanceFuturesAdapter(cfg.MarketType, cfg.RestURL, cfg.WSURL))
		case "okx":
			adapters = append(adapters, exchange.NewOKXAdapter(cfg.MarketType, cfg.RestURL, cfg.WSURL))
		}
	}
	return adapters
}
