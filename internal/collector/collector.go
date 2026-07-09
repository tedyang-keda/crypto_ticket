package collector

import (
	"context"
	"errors"
	"log"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"crypto-ticket/internal/exchange"
	"crypto-ticket/internal/market"
	"crypto-ticket/internal/storage"
)

type Config struct {
	SymbolRefreshInterval time.Duration
	ReconnectBaseDelay    time.Duration
	ReconnectMaxDelay     time.Duration
	SubscriptionChunkSize int
}

type KlinePublisher interface {
	IngestKline(ctx context.Context, bar market.Bar) error
}

type Runtime struct {
	Adapter exchange.Adapter
	Config  Config
}

type Runner struct {
	runtimes  []Runtime
	store     storage.HistoricalStore
	publisher KlinePublisher
	client    *http.Client
}

type activeSymbols struct {
	List []string
	Meta map[string]market.SymbolInfo
}

func NewRunner(runtimes []Runtime, store storage.HistoricalStore, publisher KlinePublisher) *Runner {
	return &Runner{
		runtimes:  runtimes,
		store:     store,
		publisher: publisher,
		client: &http.Client{
			Timeout: 20 * time.Second,
		},
	}
}

func (r *Runner) Run(ctx context.Context) error {
	errCh := make(chan error, len(r.runtimes))
	for _, runtime := range r.runtimes {
		runtime := runtime
		go func() {
			errCh <- r.runExchange(ctx, runtime)
		}()
	}
	for range r.runtimes {
		err := <-errCh
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
	}
	return nil
}

func (r *Runner) runExchange(ctx context.Context, runtime Runtime) error {
	cfg := runtime.Config
	if cfg.ReconnectBaseDelay <= 0 {
		cfg.ReconnectBaseDelay = time.Second
	}
	if cfg.ReconnectMaxDelay <= 0 {
		cfg.ReconnectMaxDelay = 60 * time.Second
	}
	backoff := cfg.ReconnectBaseDelay
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err := r.connectOnce(ctx, runtime.Adapter, cfg)
		if err == nil {
			backoff = cfg.ReconnectBaseDelay
			continue
		}
		if ctx.Err() == nil {
			log.Printf("%s %s collector reconnect after error: %v", runtime.Adapter.Name(), runtime.Adapter.MarketType(), err)
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		backoff = minDuration(backoff*2, cfg.ReconnectMaxDelay)
	}
}

func (r *Runner) connectOnce(ctx context.Context, adapter exchange.Adapter, cfg Config) error {
	symbols, err := r.refreshSymbols(ctx, adapter)
	if err != nil {
		return err
	}
	if len(symbols.List) == 0 {
		return errors.New("no active symbols")
	}
	if staticAdapter, ok := adapter.(exchange.StaticStreamAdapter); ok {
		return r.connectStaticStreams(ctx, adapter, staticAdapter, symbols, cfg)
	}

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, adapter.WSURL(), nil)
	if err != nil {
		return err
	}
	defer conn.Close()

	requestID := time.Now().UnixNano() % math.MaxInt32
	requestID, err = sendSubscriptions(conn, adapter, symbols.List, requestID, cfg.SubscriptionChunkSize)
	if err != nil {
		return err
	}
	log.Printf("%s %s kline collector connected symbols=%d", adapter.Name(), adapter.MarketType(), len(symbols.List))

	refreshAt := time.Now().Add(cfg.SymbolRefreshInterval)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if cfg.SymbolRefreshInterval > 0 && !time.Now().Before(refreshAt) {
			symbols, requestID, err = r.refreshConnectionSubscriptions(ctx, conn, adapter, symbols, requestID, cfg)
			if err != nil {
				return err
			}
			refreshAt = time.Now().Add(cfg.SymbolRefreshInterval)
		}
		_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		_, payload, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		bars, err := adapter.ParseKlineMessage(payload)
		if err != nil {
			log.Printf("%s %s parse kline failed: %v", adapter.Name(), adapter.MarketType(), err)
			continue
		}
		for _, bar := range bars {
			bar = enrichBarFromSymbols(bar, symbols.Meta)
			if err := r.publisher.IngestKline(ctx, bar); err != nil {
				return err
			}
		}
	}
}

func (r *Runner) connectStaticStreams(ctx context.Context, adapter exchange.Adapter, staticAdapter exchange.StaticStreamAdapter, symbols activeSymbols, cfg Config) error {
	chunks := chunkSymbols(symbols.List, cfg.SubscriptionChunkSize)
	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, len(chunks))
	for index, chunk := range chunks {
		index := index
		chunk := append([]string(nil), chunk...)
		go func() {
			errCh <- r.readStaticStream(childCtx, adapter, staticAdapter.StaticStreamURL(chunk), index, len(chunk), symbols.Meta)
		}()
	}
	err := <-errCh
	cancel()
	return err
}

func (r *Runner) readStaticStream(ctx context.Context, adapter exchange.Adapter, wsURL string, index int, symbolCount int, meta map[string]market.SymbolInfo) error {
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return err
	}
	defer conn.Close()
	log.Printf("%s %s static kline stream connected chunk=%d symbols=%d", adapter.Name(), adapter.MarketType(), index, symbolCount)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		_, payload, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		bars, err := adapter.ParseKlineMessage(payload)
		if err != nil {
			log.Printf("%s %s parse kline failed: %v", adapter.Name(), adapter.MarketType(), err)
			continue
		}
		for _, bar := range bars {
			bar = enrichBarFromSymbols(bar, meta)
			if err := r.publisher.IngestKline(ctx, bar); err != nil {
				return err
			}
		}
	}
}

func (r *Runner) refreshConnectionSubscriptions(
	ctx context.Context,
	conn *websocket.Conn,
	adapter exchange.Adapter,
	current activeSymbols,
	requestID int64,
	cfg Config,
) (activeSymbols, int64, error) {
	next, err := r.refreshSymbols(ctx, adapter)
	if err != nil {
		return current, requestID, err
	}
	if len(next.List) == 0 {
		return current, requestID, errors.New("no active symbols")
	}
	requestID, subscribed, unsubscribed, err := syncSubscriptions(conn, adapter, current.List, next.List, requestID, cfg.SubscriptionChunkSize)
	if err != nil {
		return current, requestID, err
	}
	if subscribed > 0 || unsubscribed > 0 {
		log.Printf("%s %s subscriptions refreshed active=%d subscribe=%d unsubscribe=%d", adapter.Name(), adapter.MarketType(), len(next.List), subscribed, unsubscribed)
	}
	return next, requestID, nil
}

func (r *Runner) refreshSymbols(ctx context.Context, adapter exchange.Adapter) (activeSymbols, error) {
	symbols, err := adapter.FetchSymbols(ctx, r.client)
	if err != nil {
		return activeSymbols{}, err
	}
	if r.store != nil {
		if err := r.store.UpsertSymbols(ctx, symbols); err != nil {
			return activeSymbols{}, err
		}
	}
	active := make([]string, 0, len(symbols))
	meta := make(map[string]market.SymbolInfo, len(symbols))
	for _, symbol := range symbols {
		if symbol.SourceMarket == "" {
			symbol.SourceMarket = market.SourceMarket(symbol.Exchange, symbol.MarketType)
		}
		meta[strings.ToUpper(symbol.Symbol)] = symbol
		if symbol.IsActive {
			active = append(active, symbol.Symbol)
		}
	}
	return activeSymbols{List: active, Meta: meta}, nil
}

func enrichBarFromSymbols(bar market.Bar, symbols map[string]market.SymbolInfo) market.Bar {
	if len(symbols) == 0 {
		return market.DecorateBar(bar)
	}
	if symbol, ok := symbols[strings.ToUpper(bar.Symbol)]; ok {
		return market.ApplyClassificationToBar(bar, symbol)
	}
	return market.DecorateBar(bar)
}

func sendSubscriptions(conn *websocket.Conn, adapter exchange.Adapter, symbols []string, requestID int64, chunkSize int) (int64, error) {
	chunks := chunkSymbols(symbols, chunkSize)
	for index, chunk := range chunks {
		payload, err := adapter.BuildSubscribePayload(chunk, requestID)
		if err != nil {
			return requestID, err
		}
		if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
			return requestID, err
		}
		requestID++
		if index < len(chunks)-1 {
			time.Sleep(250 * time.Millisecond)
		}
	}
	return requestID, nil
}

func syncSubscriptions(conn *websocket.Conn, adapter exchange.Adapter, current []string, next []string, requestID int64, chunkSize int) (int64, int, int, error) {
	subscribe, unsubscribe := diffSymbols(current, next)
	for _, chunk := range chunkSymbols(unsubscribe, chunkSize) {
		payload, err := adapter.BuildUnsubscribePayload(chunk, requestID)
		if err != nil {
			return requestID, len(subscribe), len(unsubscribe), err
		}
		if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
			return requestID, len(subscribe), len(unsubscribe), err
		}
		requestID++
	}
	for _, chunk := range chunkSymbols(subscribe, chunkSize) {
		payload, err := adapter.BuildSubscribePayload(chunk, requestID)
		if err != nil {
			return requestID, len(subscribe), len(unsubscribe), err
		}
		if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
			return requestID, len(subscribe), len(unsubscribe), err
		}
		requestID++
	}
	return requestID, len(subscribe), len(unsubscribe), nil
}

func diffSymbols(current []string, next []string) ([]string, []string) {
	currentSet := make(map[string]bool, len(current))
	nextSet := make(map[string]bool, len(next))
	for _, symbol := range current {
		symbol = strings.ToUpper(strings.TrimSpace(symbol))
		if symbol != "" {
			currentSet[symbol] = true
		}
	}
	for _, symbol := range next {
		symbol = strings.ToUpper(strings.TrimSpace(symbol))
		if symbol != "" {
			nextSet[symbol] = true
		}
	}
	var subscribe []string
	var unsubscribe []string
	for symbol := range nextSet {
		if !currentSet[symbol] {
			subscribe = append(subscribe, symbol)
		}
	}
	for symbol := range currentSet {
		if !nextSet[symbol] {
			unsubscribe = append(unsubscribe, symbol)
		}
	}
	sort.Strings(subscribe)
	sort.Strings(unsubscribe)
	return subscribe, unsubscribe
}

func chunkSymbols(symbols []string, chunkSize int) [][]string {
	if chunkSize <= 0 {
		chunkSize = 100
	}
	var chunks [][]string
	for start := 0; start < len(symbols); start += chunkSize {
		end := start + chunkSize
		if end > len(symbols) {
			end = len(symbols)
		}
		chunks = append(chunks, symbols[start:end])
	}
	return chunks
}

func minDuration(a time.Duration, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
