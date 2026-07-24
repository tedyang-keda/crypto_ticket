package guardian

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	"crypto-ticket/internal/aggregator"
	"crypto-ticket/internal/exchange"
	"crypto-ticket/internal/market"
	"crypto-ticket/internal/timeframe"
)

const (
	defaultAuditInterval  = time.Minute
	defaultAuditWindow    = 30 * time.Minute
	defaultAuditDelay     = 2 * time.Minute
	defaultHTTPTimeout    = 20 * time.Second
	defaultQueueSize      = 4096
	defaultSymbolsPerRun  = 50
	defaultSymbolMaxAge   = 10 * time.Minute
	defaultFloatTolerance = 1e-8
)

type Config struct {
	Enabled        bool
	AuditInterval  time.Duration
	AuditWindow    time.Duration
	AuditDelay     time.Duration
	RequestDelay   time.Duration
	HTTPTimeout    time.Duration
	QueueSize      int
	SymbolsPerRun  int
	SymbolMaxAge   time.Duration
	FloatTolerance float64
}

type Store interface {
	BarsInRange(ctx context.Context, exchange string, symbol string, timeframe string, startMS int64, endMS int64) ([]market.Bar, error)
	ListSymbols(ctx context.Context, exchange string, activeOnly *bool) ([]market.SymbolInfo, error)
	LoadKlineGuardianState(ctx context.Context, exchange string, symbol string, timeframe string) (*market.KlineGuardianState, error)
	UpsertKlineGuardianState(ctx context.Context, state market.KlineGuardianState) error
	InsertKlineGuardianEvents(ctx context.Context, events []market.KlineGuardianEvent) error
}

type Repairer interface {
	RepairFinalBars(ctx context.Context, bars []market.Bar) error
}

type Fetcher interface {
	Name() string
	MarketType() string
	FetchKlines(ctx context.Context, client *http.Client, request exchange.KlineRequest) ([]market.Bar, error)
}

type Guardian struct {
	cfg       Config
	store     Store
	repairer  Repairer
	client    *http.Client
	finalBars chan market.Bar

	fetchersByMarket   map[string]Fetcher
	fetchersByExchange map[string][]Fetcher
	auditCursor        int
}

type auditTarget struct {
	Exchange   string
	Symbol     string
	MarketType string
	Fetcher    Fetcher
}

type repairResult struct {
	Checked  int
	Repaired int
	Events   []market.KlineGuardianEvent
}

func New(store Store, repairer Repairer, fetchers []Fetcher, cfg Config) *Guardian {
	cfg = normalizeConfig(cfg)
	g := &Guardian{
		cfg:                cfg,
		store:              store,
		repairer:           repairer,
		client:             &http.Client{Timeout: cfg.HTTPTimeout},
		finalBars:          make(chan market.Bar, cfg.QueueSize),
		fetchersByMarket:   make(map[string]Fetcher),
		fetchersByExchange: make(map[string][]Fetcher),
	}
	for _, fetcher := range fetchers {
		if fetcher == nil {
			continue
		}
		exchangeName := normalizeExchange(fetcher.Name())
		marketType := normalizeMarketType(fetcher.MarketType())
		g.fetchersByMarket[fetcherKey(exchangeName, marketType)] = fetcher
		g.fetchersByExchange[exchangeName] = append(g.fetchersByExchange[exchangeName], fetcher)
	}
	return g
}

func normalizeConfig(cfg Config) Config {
	if cfg.AuditInterval <= 0 {
		cfg.AuditInterval = defaultAuditInterval
	}
	if cfg.AuditWindow <= 0 {
		cfg.AuditWindow = defaultAuditWindow
	}
	if cfg.AuditDelay <= 0 {
		cfg.AuditDelay = defaultAuditDelay
	}
	if cfg.HTTPTimeout <= 0 {
		cfg.HTTPTimeout = defaultHTTPTimeout
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = defaultQueueSize
	}
	if cfg.SymbolsPerRun <= 0 {
		cfg.SymbolsPerRun = defaultSymbolsPerRun
	}
	if cfg.SymbolMaxAge <= 0 {
		cfg.SymbolMaxAge = defaultSymbolMaxAge
	}
	if cfg.FloatTolerance <= 0 {
		cfg.FloatTolerance = defaultFloatTolerance
	}
	return cfg
}

func (g *Guardian) ObserveFinalBar(_ context.Context, bar market.Bar) error {
	if !g.cfg.Enabled || !bar.IsFinal || bar.Timeframe != aggregator.OneMinute {
		return nil
	}
	select {
	case g.finalBars <- market.DecorateBar(bar):
	default:
		log.Printf("kline guardian final-bar queue full exchange=%s symbol=%s start_ms=%d", bar.Exchange, bar.Symbol, bar.StartMS)
	}
	return nil
}

func (g *Guardian) Run(ctx context.Context) error {
	if !g.cfg.Enabled {
		return nil
	}
	errCh := make(chan error, 2)
	go func() { errCh <- g.runObservedFinals(ctx) }()
	go func() { errCh <- g.runAudits(ctx) }()
	for i := 0; i < 2; i++ {
		err := <-errCh
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
	}
	return ctx.Err()
}

func (g *Guardian) runObservedFinals(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case bar := <-g.finalBars:
			if err := g.handleFinalBar(ctx, bar); err != nil {
				log.Printf("kline guardian final check failed exchange=%s symbol=%s start_ms=%d err=%v", bar.Exchange, bar.Symbol, bar.StartMS, err)
			}
		}
	}
}

func (g *Guardian) runAudits(ctx context.Context) error {
	ticker := time.NewTicker(g.cfg.AuditInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := g.AuditOnce(ctx); err != nil {
				log.Printf("kline guardian audit failed: %v", err)
			}
		}
	}
}

func (g *Guardian) handleFinalBar(ctx context.Context, bar market.Bar) error {
	bar.Exchange = normalizeExchange(bar.Exchange)
	bar.Symbol = normalizeSymbol(bar.Symbol)
	bar.Timeframe = timeframe.MustNormalize(bar.Timeframe)
	state, err := g.store.LoadKlineGuardianState(ctx, bar.Exchange, bar.Symbol, bar.Timeframe)
	if err != nil {
		return err
	}
	previousStart := int64(0)
	if state != nil {
		previousStart = state.LastFinalStartMS
	}
	if previousStart == 0 {
		baseline, err := g.localBaselineStart(ctx, bar)
		if err != nil {
			return err
		}
		previousStart = baseline
	}

	now := market.NowMS()
	nextExpected := int64(0)
	var events []market.KlineGuardianEvent
	reloadState := false
	if previousStart > 0 {
		nextExpected = timeframe.NextStartMS(previousStart, aggregator.OneMinute)
	}
	status := "ok"
	if nextExpected > 0 && bar.StartMS > nextExpected {
		gapStart := nextExpected
		gapEnd := bar.StartMS - timeframe.MinuteMS
		status = "gap_detected"
		events = append(events, market.KlineGuardianEvent{
			Exchange: bar.Exchange, Symbol: bar.Symbol, Timeframe: bar.Timeframe,
			StartMS: gapStart, EndMS: gapEnd, EventType: "gap_detected", CreatedAtMS: now,
		})
		result, err := g.repairRange(ctx, bar.Exchange, bar.Symbol, gapStart, gapEnd, "watermark_gap")
		reloadState = true
		if err != nil {
			status = "repair_error"
			events = append(events, market.KlineGuardianEvent{
				Exchange: bar.Exchange, Symbol: bar.Symbol, Timeframe: bar.Timeframe,
				StartMS: gapStart, EndMS: gapEnd, EventType: "repair_error", NewValueJSON: quoteError(err), CreatedAtMS: now,
			})
		} else if result.Repaired > 0 {
			status = "gap_repaired"
		} else {
			status = "gap_checked"
		}
	}
	if len(events) > 0 {
		if err := g.store.InsertKlineGuardianEvents(ctx, events); err != nil {
			return err
		}
	}
	if reloadState {
		refreshed, err := g.store.LoadKlineGuardianState(ctx, bar.Exchange, bar.Symbol, bar.Timeframe)
		if err != nil {
			return err
		}
		if refreshed != nil {
			state = refreshed
		}
	}
	if state == nil {
		state = &market.KlineGuardianState{Exchange: bar.Exchange, Symbol: bar.Symbol, Timeframe: bar.Timeframe}
	}
	if bar.StartMS > state.LastFinalStartMS {
		state.LastFinalStartMS = bar.StartMS
	}
	if bar.UpdatedAtMS > 0 {
		state.LastFinalRecvMS = bar.UpdatedAtMS
	} else {
		state.LastFinalRecvMS = now
	}
	if nextExpected > 0 && bar.StartMS > nextExpected {
		state.LastGapStartMS = nextExpected
		state.LastGapEndMS = bar.StartMS - timeframe.MinuteMS
	}
	state.Status = status
	state.UpdatedAtMS = now
	return g.store.UpsertKlineGuardianState(ctx, *state)
}

func (g *Guardian) localBaselineStart(ctx context.Context, bar market.Bar) (int64, error) {
	start := bar.StartMS - int64(g.cfg.AuditWindow/time.Millisecond)
	if start < 0 {
		start = 0
	}
	bars, err := g.store.BarsInRange(ctx, bar.Exchange, bar.Symbol, aggregator.OneMinute, start, bar.StartMS-1)
	if err != nil || len(bars) == 0 {
		return 0, err
	}
	return bars[len(bars)-1].StartMS, nil
}

func (g *Guardian) AuditOnce(ctx context.Context) error {
	endStart := g.auditEndStart()
	if endStart <= 0 {
		return nil
	}
	windowMinutes := int64(g.cfg.AuditWindow / time.Minute)
	if windowMinutes <= 0 {
		windowMinutes = int64(defaultAuditWindow / time.Minute)
	}
	start := endStart - (windowMinutes-1)*timeframe.MinuteMS
	if start < 0 {
		start = 0
	}
	targets, err := g.auditTargets(ctx)
	if err != nil {
		return err
	}
	if len(targets) == 0 {
		return nil
	}
	count := minInt(g.cfg.SymbolsPerRun, len(targets))
	for i := 0; i < count; i++ {
		index := (g.auditCursor + i) % len(targets)
		target := targets[index]
		if _, err := g.repairRangeWithFetcher(ctx, target.Fetcher, target.Exchange, target.Symbol, start, endStart, "window_audit"); err != nil {
			log.Printf("kline guardian target audit failed exchange=%s symbol=%s err=%v", target.Exchange, target.Symbol, err)
		}
		if g.cfg.RequestDelay > 0 && i < count-1 {
			timer := time.NewTimer(g.cfg.RequestDelay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
	g.auditCursor = (g.auditCursor + count) % len(targets)
	return nil
}

func (g *Guardian) auditEndStart() int64 {
	eligibleEnd := market.NowMS() - int64(g.cfg.AuditDelay/time.Millisecond)
	return timeframe.FloorStartMS(eligibleEnd, aggregator.OneMinute) - timeframe.MinuteMS
}

func (g *Guardian) auditTargets(ctx context.Context) ([]auditTarget, error) {
	activeOnly := true
	now := market.NowMS()
	var targets []auditTarget
	for exchangeName := range g.fetchersByExchange {
		symbols, err := g.store.ListSymbols(ctx, exchangeName, &activeOnly)
		if err != nil {
			return nil, err
		}
		for _, symbol := range symbols {
			if !symbol.IsActive {
				continue
			}
			if symbol.LastSeenAtMS > 0 && now-symbol.LastSeenAtMS > int64(g.cfg.SymbolMaxAge/time.Millisecond) {
				continue
			}
			fetcher := g.fetcherFor(symbol.Exchange, symbol.MarketType)
			if fetcher == nil {
				continue
			}
			targets = append(targets, auditTarget{
				Exchange:   normalizeExchange(symbol.Exchange),
				Symbol:     normalizeSymbol(symbol.Symbol),
				MarketType: normalizeMarketType(symbol.MarketType),
				Fetcher:    fetcher,
			})
		}
	}
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].Exchange != targets[j].Exchange {
			return targets[i].Exchange < targets[j].Exchange
		}
		if targets[i].MarketType != targets[j].MarketType {
			return targets[i].MarketType < targets[j].MarketType
		}
		return targets[i].Symbol < targets[j].Symbol
	})
	return targets, nil
}

func (g *Guardian) repairRange(ctx context.Context, exchangeName string, symbol string, startMS int64, endMS int64, reason string) (repairResult, error) {
	fetcher, err := g.fetcherForSymbol(ctx, exchangeName, symbol)
	if err != nil {
		return repairResult{}, err
	}
	if fetcher == nil {
		return repairResult{}, fmt.Errorf("missing REST kline fetcher for %s %s", exchangeName, symbol)
	}
	return g.repairRangeWithFetcher(ctx, fetcher, exchangeName, symbol, startMS, endMS, reason)
}

func (g *Guardian) repairRangeWithFetcher(ctx context.Context, fetcher Fetcher, exchangeName string, symbol string, startMS int64, endMS int64, reason string) (repairResult, error) {
	exchangeName = normalizeExchange(exchangeName)
	symbol = normalizeSymbol(symbol)
	startMS = timeframe.FloorStartMS(startMS, aggregator.OneMinute)
	endMS = timeframe.FloorStartMS(endMS, aggregator.OneMinute)
	if endMS < startMS {
		return repairResult{}, nil
	}
	limit := int((endMS-startMS)/timeframe.MinuteMS) + 1
	official, err := fetcher.FetchKlines(ctx, g.client, exchange.KlineRequest{
		Symbol: symbol, Timeframe: aggregator.OneMinute, StartMS: startMS, EndMS: endMS, Limit: limit,
		ForwardAdjusted: true,
	})
	if err != nil {
		event := market.KlineGuardianEvent{
			Exchange: exchangeName, Symbol: symbol, Timeframe: aggregator.OneMinute,
			StartMS: startMS, EndMS: endMS, EventType: "rest_error", NewValueJSON: quoteError(err), CreatedAtMS: market.NowMS(),
		}
		_ = g.store.InsertKlineGuardianEvents(ctx, []market.KlineGuardianEvent{event})
		g.upsertAuditState(ctx, exchangeName, symbol, startMS, endMS, "rest_error")
		return repairResult{}, err
	}
	local, err := g.store.BarsInRange(ctx, exchangeName, symbol, aggregator.OneMinute, startMS, endMS)
	if err != nil {
		return repairResult{}, err
	}
	result := g.diffOfficialBars(exchangeName, symbol, official, local, reason)
	if result.Repaired > 0 {
		toRepair := make([]market.Bar, 0, result.Repaired)
		for _, event := range result.Events {
			if event.EventType != "missing_repair" && event.EventType != "mismatch_repair" {
				continue
			}
			for _, bar := range official {
				if bar.StartMS == event.StartMS {
					bar.Source = "rest"
					bar.Reason = "guardian_" + reason
					bar.IsFinal = true
					bar.UpdatedAtMS = market.NowMS()
					toRepair = append(toRepair, market.DecorateBar(bar))
					break
				}
			}
		}
		if err := g.repairer.RepairFinalBars(ctx, toRepair); err != nil {
			_ = g.store.InsertKlineGuardianEvents(ctx, []market.KlineGuardianEvent{{
				Exchange: exchangeName, Symbol: symbol, Timeframe: aggregator.OneMinute,
				StartMS: startMS, EndMS: endMS, EventType: "repair_error", NewValueJSON: quoteError(err), CreatedAtMS: market.NowMS(),
			}})
			return result, err
		}
	}
	if len(result.Events) > 0 {
		if err := g.store.InsertKlineGuardianEvents(ctx, result.Events); err != nil {
			return result, err
		}
	}
	status := "ok"
	if result.Repaired > 0 {
		status = "repaired"
	}
	if err := g.upsertAuditState(ctx, exchangeName, symbol, startMS, endMS, status); err != nil {
		return result, err
	}
	return result, nil
}

func (g *Guardian) diffOfficialBars(exchangeName string, symbol string, official []market.Bar, local []market.Bar, reason string) repairResult {
	now := market.NowMS()
	localByStart := make(map[int64]market.Bar, len(local))
	for _, bar := range local {
		localByStart[bar.StartMS] = bar
	}
	var result repairResult
	for _, officialBar := range official {
		if !officialBar.IsFinal || officialBar.Timeframe != aggregator.OneMinute {
			continue
		}
		result.Checked++
		localBar, ok := localByStart[officialBar.StartMS]
		if !ok {
			result.Repaired++
			result.Events = append(result.Events, market.KlineGuardianEvent{
				Exchange: exchangeName, Symbol: symbol, Timeframe: aggregator.OneMinute,
				StartMS: officialBar.StartMS, EndMS: officialBar.EndMS, EventType: "missing_repair",
				NewValueJSON: barJSON(officialBar), CreatedAtMS: now,
			})
			continue
		}
		if barsDiffer(localBar, officialBar, g.cfg.FloatTolerance) {
			result.Repaired++
			result.Events = append(result.Events, market.KlineGuardianEvent{
				Exchange: exchangeName, Symbol: symbol, Timeframe: aggregator.OneMinute,
				StartMS: officialBar.StartMS, EndMS: officialBar.EndMS, EventType: "mismatch_repair",
				OldValueJSON: barJSON(localBar), NewValueJSON: barJSON(officialBar), CreatedAtMS: now,
			})
		}
	}
	return result
}

func (g *Guardian) upsertAuditState(ctx context.Context, exchangeName string, symbol string, startMS int64, endMS int64, status string) error {
	now := market.NowMS()
	state, err := g.store.LoadKlineGuardianState(ctx, exchangeName, symbol, aggregator.OneMinute)
	if err != nil {
		return err
	}
	if state == nil {
		state = &market.KlineGuardianState{Exchange: exchangeName, Symbol: symbol, Timeframe: aggregator.OneMinute}
	}
	state.LastCheckedStartMS = startMS
	state.LastCheckedEndMS = endMS
	state.LastCheckedAtMS = now
	state.Status = status
	state.UpdatedAtMS = now
	return g.store.UpsertKlineGuardianState(ctx, *state)
}

func (g *Guardian) fetcherForSymbol(ctx context.Context, exchangeName string, symbol string) (Fetcher, error) {
	exchangeName = normalizeExchange(exchangeName)
	symbol = normalizeSymbol(symbol)
	activeOnly := true
	symbols, err := g.store.ListSymbols(ctx, exchangeName, &activeOnly)
	if err != nil {
		return nil, err
	}
	for _, info := range symbols {
		if normalizeSymbol(info.Symbol) == symbol {
			return g.fetcherFor(info.Exchange, info.MarketType), nil
		}
	}
	return g.singleExchangeFetcher(exchangeName), nil
}

func (g *Guardian) fetcherFor(exchangeName string, marketType string) Fetcher {
	exchangeName = normalizeExchange(exchangeName)
	marketType = normalizeMarketType(marketType)
	if fetcher := g.fetchersByMarket[fetcherKey(exchangeName, marketType)]; fetcher != nil {
		return fetcher
	}
	return g.singleExchangeFetcher(exchangeName)
}

func (g *Guardian) singleExchangeFetcher(exchangeName string) Fetcher {
	fetchers := g.fetchersByExchange[normalizeExchange(exchangeName)]
	if len(fetchers) == 1 {
		return fetchers[0]
	}
	return nil
}

func barsDiffer(local market.Bar, official market.Bar, tolerance float64) bool {
	if local.EndMS != official.EndMS || !local.IsFinal {
		return true
	}
	if floatDiffers(local.OpenPrice, official.OpenPrice, tolerance) ||
		floatDiffers(local.HighPrice, official.HighPrice, tolerance) ||
		floatDiffers(local.LowPrice, official.LowPrice, tolerance) ||
		floatDiffers(local.ClosePrice, official.ClosePrice, tolerance) ||
		floatDiffers(local.Volume, official.Volume, tolerance) ||
		floatDiffers(local.QuoteVolume, official.QuoteVolume, tolerance) ||
		floatDiffers(local.ContractVolume, official.ContractVolume, tolerance) {
		return true
	}
	if official.TradeCount > 0 && local.TradeCount != official.TradeCount {
		return true
	}
	return false
}

func floatDiffers(left float64, right float64, tolerance float64) bool {
	limit := math.Max(tolerance, math.Abs(right)*tolerance)
	return math.Abs(left-right) > limit
}

func barJSON(bar market.Bar) string {
	payload, err := json.Marshal(market.DecorateBar(bar))
	if err != nil {
		return ""
	}
	return string(payload)
}

func quoteError(err error) string {
	payload, _ := json.Marshal(map[string]string{"error": err.Error()})
	return string(payload)
}

func fetcherKey(exchangeName string, marketType string) string {
	return normalizeExchange(exchangeName) + ":" + normalizeMarketType(marketType)
}

func normalizeExchange(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func normalizeSymbol(value string) string {
	return strings.ToUpper(strings.TrimSpace(value))
}

func normalizeMarketType(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func minInt(left int, right int) int {
	if left < right {
		return left
	}
	return right
}
