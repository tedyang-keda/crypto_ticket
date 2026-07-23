package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"crypto-ticket/internal/adjustment"
	"crypto-ticket/internal/api"
	"crypto-ticket/internal/app"
	"crypto-ticket/internal/collector"
	"crypto-ticket/internal/config"
	"crypto-ticket/internal/corpaction"
	"crypto-ticket/internal/exchange"
	"crypto-ticket/internal/guardian"
	"crypto-ticket/internal/instrument"
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

	var corpRegistry *corpaction.Registry
	var aliasRegistry *corpaction.AliasRegistry
	if cfg.EnableInstrumentMonitor {
		corpRegistry = corpaction.NewRegistry(corpaction.Config{
			PendingTTL:    time.Duration(cfg.CorpActionPendingTTLSeconds) * time.Second,
			ResolvedTTL:   time.Duration(cfg.CorpActionResolvedTTLSeconds) * time.Second,
			NeutralizePct: cfg.CorpActionNeutralizePct,
		})
		marketService.SetCorporateActionGuard(corpRegistry)
		aliasRegistry = corpaction.NewAliasRegistry()
		for _, spec := range cfg.InstrumentAliases {
			aliasRegistry.Link(spec.Exchange, "", spec.Successor, spec.Predecessor, 0)
		}
		marketService.SetAliasResolver(aliasRegistry)
		if len(cfg.InstrumentAliases) > 0 {
			log.Printf("seeded %d instrument rename aliases", len(cfg.InstrumentAliases))
		}
	}

	errCh := make(chan error, 8)
	startBackgroundWorkers(ctx, cfg, store, marketService, corpRegistry, aliasRegistry, errCh)
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
	corpRegistry *corpaction.Registry,
	aliasRegistry *corpaction.AliasRegistry,
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
	if cfg.EnableInstrumentMonitor {
		var sink instrument.EventSink = instrument.LogSink{}
		if cfg.EnableFactorDerivation {
			deriver := adjustment.New(store, store, adjustment.Config{
				ConfirmDelay:             time.Duration(cfg.FactorConfirmDelaySeconds) * time.Second,
				Interval:                 time.Duration(cfg.FactorDeriveIntervalSeconds) * time.Second,
				Lookback:                 time.Duration(cfg.FactorLookbackSeconds) * time.Second,
				MinMovePct:               cfg.FactorMinMovePct,
				MaxAttempts:              cfg.FactorMaxAttempts,
				MaxWait:                  time.Duration(cfg.FactorMaxWaitSeconds) * time.Second,
				OfficialDivergencePct:    cfg.FactorOfficialDivergencePct,
				AnnouncementTolerancePct: cfg.FactorAnnouncementTolerancePct,
				RequireAnnouncement:      cfg.FactorRequireAnnouncement,
			})
			if corpRegistry != nil {
				deriver.SetRegistry(corpRegistry)
			}
			if aliasRegistry != nil {
				deriver.SetAliasResolver(aliasRegistry)
			}
			if cfg.FactorUseOfficialKlines {
				if official := makeOfficialKlineSource(cfg); official != nil {
					deriver.SetOfficialSource(official)
					log.Printf("adjustment factor deriver using official REST klines (divergence>%.3f flagged)", cfg.FactorOfficialDivergencePct)
				}
			}
			if cfg.FactorVerifyAnnouncement {
				if verifier := makeAnnouncementVerifier(cfg); verifier != nil {
					deriver.SetAnnouncementVerifier(verifier)
					log.Printf("adjustment factor deriver cross-checking announcements (tolerance=%.3f strict=%v)",
						cfg.FactorAnnouncementTolerancePct, cfg.FactorRequireAnnouncement)
				}
			}
			sink = deriver
			go func() {
				if err := deriver.Run(ctx); err != nil && ctx.Err() == nil {
					errCh <- err
				}
			}()
			log.Printf("adjustment factor deriver started confirm_delay=%ds min_move=%.3f",
				cfg.FactorConfirmDelaySeconds, cfg.FactorMinMovePct)
		}
		monitors := makeInstrumentMonitors(cfg, store, sink, aliasRegistry)
		for _, monitor := range monitors {
			monitor := monitor
			go func() {
				if err := monitor.Run(ctx); err != nil && ctx.Err() == nil {
					errCh <- err
				}
			}()
		}
		pollingMonitors := makeBinancePollingMonitors(cfg, sink, aliasRegistry)
		for _, monitor := range pollingMonitors {
			monitor := monitor
			go func() {
				if err := monitor.RunPolling(ctx); err != nil && ctx.Err() == nil {
					errCh <- err
				}
			}()
		}
		if len(monitors)+len(pollingMonitors) > 0 {
			log.Printf("instrument monitor started ws=%d polling=%d", len(monitors), len(pollingMonitors))
		}
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
			SymbolMaxAge:  time.Duration(cfg.KlineGuardianSymbolMaxAgeSeconds) * time.Second,
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

// makeInstrumentMonitors builds real-time metadata monitors for OKX instruments
// and Binance USD-M contractInfo. Binance COIN-M is excluded because the
// equity/TRADIFI perpetual products live on USD-M.
func makeInstrumentMonitors(cfg config.Config, store storage.HistoricalStore, sink instrument.EventSink, aliasRegistry *corpaction.AliasRegistry) []*instrument.Monitor {
	pingInterval := time.Duration(cfg.InstrumentMonitorPingSeconds) * time.Second
	monitors := make([]*instrument.Monitor, 0)
	for _, exchangeConfig := range cfg.Exchanges {
		if !exchangeConfig.Enabled {
			continue
		}
		var source instrument.Source
		switch exchangeConfig.Name {
		case "okx":
			source = exchange.NewOKXAdapter(exchangeConfig.MarketType, exchangeConfig.RestURL, cfg.OKXInstrumentsWSURL)
		case "binance":
			if strings.Contains(strings.ToLower(exchangeConfig.MarketType), "coin") {
				continue
			}
			source = exchange.NewBinanceContractInfoSource(exchangeConfig.MarketType, exchangeConfig.RestURL, cfg.BinanceContractInfoWSURL)
		}
		if source == nil {
			continue
		}
		monitor := instrument.New(source, sink, store, instrument.Config{
			ReconnectBaseDelay:        time.Duration(cfg.ReconnectBaseDelaySeconds) * time.Second,
			ReconnectMaxDelay:         time.Duration(cfg.ReconnectMaxDelaySeconds) * time.Second,
			PingInterval:              pingInterval,
			EmitInitialCorporateState: exchangeConfig.Name == "binance",
		})
		if aliasRegistry != nil {
			monitor.SetAliasLinker(aliasRegistry)
		}
		monitors = append(monitors, monitor)
	}
	return monitors
}

// makeBinancePollingMonitors builds a REST-polling instrument monitor per
// enabled Binance USD-M market. This remains as exchangeInfo reconciliation if
// the contractInfo stream misses a transition during a disconnect.
func makeBinancePollingMonitors(cfg config.Config, sink instrument.EventSink, aliasRegistry *corpaction.AliasRegistry) []*instrument.Monitor {
	interval := time.Duration(cfg.InstrumentMonitorPollSeconds) * time.Second
	monitors := make([]*instrument.Monitor, 0)
	for _, exchangeConfig := range cfg.Exchanges {
		if !exchangeConfig.Enabled || exchangeConfig.Name != "binance" {
			continue
		}
		if strings.Contains(strings.ToLower(exchangeConfig.MarketType), "coin") {
			continue // equity perps are USDⓈ-M only
		}
		fetcher := exchange.NewBinanceFuturesAdapter(exchangeConfig.MarketType, exchangeConfig.RestURL, exchangeConfig.WSURL)
		monitor := instrument.NewPolling(fetcher, sink, nil, instrument.Config{
			PollInterval: interval,
		})
		if aliasRegistry != nil {
			monitor.SetAliasLinker(aliasRegistry)
		}
		monitors = append(monitors, monitor)
	}
	return monitors
}

// officialKlineSource adapts the exchange REST kline fetchers to the deriver's
// OfficialKlineSource interface, keyed by source market (exchange:marketType).
type officialKlineSource struct {
	fetchers map[string]exchange.RESTKlineFetcher
	client   *http.Client
}

func (s *officialKlineSource) FetchOfficialKlines(ctx context.Context, _ string, sourceMarket string, symbol string, timeframe string, startMS int64, endMS int64) ([]market.Bar, error) {
	fetcher, ok := s.fetchers[sourceMarket]
	if !ok {
		return nil, nil
	}
	return fetcher.FetchKlines(ctx, s.client, exchange.KlineRequest{
		Symbol:    symbol,
		Timeframe: timeframe,
		StartMS:   startMS,
		EndMS:     endMS,
	})
}

// makeOfficialKlineSource builds the authoritative REST kline source used to
// derive factors from trusted exchange data and flag local store divergence.
func makeOfficialKlineSource(cfg config.Config) *officialKlineSource {
	fetchers := make(map[string]exchange.RESTKlineFetcher)
	for _, exchangeConfig := range cfg.Exchanges {
		if !exchangeConfig.Enabled {
			continue
		}
		var fetcher exchange.RESTKlineFetcher
		switch exchangeConfig.Name {
		case "binance":
			fetcher = exchange.NewBinanceFuturesAdapter(exchangeConfig.MarketType, exchangeConfig.RestURL, exchangeConfig.WSURL)
		case "okx":
			fetcher = exchange.NewOKXAdapter(exchangeConfig.MarketType, exchangeConfig.RestURL, exchangeConfig.WSURL)
		}
		if fetcher == nil {
			continue
		}
		fetchers[market.SourceMarket(fetcher.Name(), fetcher.MarketType())] = fetcher
	}
	if len(fetchers) == 0 {
		return nil
	}
	return &officialKlineSource{fetchers: fetchers, client: &http.Client{Timeout: 20 * time.Second}}
}

// multiAnnouncementVerifier dispatches announcement cross-checks to the
// per-exchange verifier. Exchanges without a verifier degrade to "not found"
// (lenient), so they never block a factor.
type multiAnnouncementVerifier struct {
	okx     *exchange.OKXAnnouncementVerifier
	binance *exchange.BinanceAnnouncementVerifier
}

func (m *multiAnnouncementVerifier) VerifyCorporateAction(ctx context.Context, exchangeName string, sourceMarket string, symbol string, boundaryMS int64) (bool, float64, bool) {
	switch strings.ToLower(exchangeName) {
	case "binance":
		if m.binance != nil {
			return m.binance.VerifyCorporateAction(ctx, exchangeName, sourceMarket, symbol, boundaryMS)
		}
	case "okx":
		if m.okx != nil {
			return m.okx.VerifyCorporateAction(ctx, exchangeName, sourceMarket, symbol, boundaryMS)
		}
	}
	return false, 0, false
}

// makeAnnouncementVerifier builds the announcement cross-check dispatcher.
func makeAnnouncementVerifier(cfg config.Config) *multiAnnouncementVerifier {
	client := &http.Client{Timeout: 20 * time.Second}
	verifier := &multiAnnouncementVerifier{}
	for _, exchangeConfig := range cfg.Exchanges {
		if !exchangeConfig.Enabled {
			continue
		}
		if exchangeConfig.Name == "okx" && verifier.okx == nil {
			verifier.okx = exchange.NewOKXAnnouncementVerifier(exchangeConfig.RestURL, client)
		}
		if exchangeConfig.Name == "binance" && !strings.Contains(strings.ToLower(exchangeConfig.MarketType), "coin") && verifier.binance == nil {
			verifier.binance = exchange.NewBinanceAnnouncementVerifier(cfg.BinanceCMSURL, client)
		}
	}
	if verifier.okx == nil && verifier.binance == nil {
		return nil
	}
	return verifier
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
