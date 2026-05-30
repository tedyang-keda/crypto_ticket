package collector

import (
	"context"
	"errors"
	"log"
	"math"
	"net"
	"net/http"
	"sort"
	"time"

	"github.com/gorilla/websocket"

	"crypto-ticket/internal/exchange"
	"crypto-ticket/internal/market"
	"crypto-ticket/internal/storage"
	"crypto-ticket/internal/stream"
)

type Config struct {
	SymbolRefreshInterval time.Duration
	ReconnectBaseDelay    time.Duration
	ReconnectMaxDelay     time.Duration
	StreamMaxLen          int64
	SubscriptionChunkSize int
	WriteBatchSize        int
	WriteFlushInterval    time.Duration
	Shard                 int
}

type TickPublisher interface {
	PublishTick(ctx context.Context, tick market.Tick) (market.Tick, error)
}

type Runner struct {
	adapters  []exchange.Adapter
	writer    stream.TickStream
	store     storage.HistoricalStore
	publisher TickPublisher
	configs   map[string]Config
	client    *http.Client
}

func NewRunner(adapters []exchange.Adapter, writer stream.TickStream, store storage.HistoricalStore, publisher TickPublisher, configs map[string]Config) *Runner {
	return &Runner{
		adapters:  adapters,
		writer:    writer,
		store:     store,
		publisher: publisher,
		configs:   configs,
		client: &http.Client{
			Timeout: 20 * time.Second,
		},
	}
}

func (r *Runner) Run(ctx context.Context) error {
	errCh := make(chan error, len(r.adapters))
	for _, adapter := range r.adapters {
		adapter := adapter
		go func() {
			errCh <- r.runExchange(ctx, adapter)
		}()
	}
	for range r.adapters {
		err := <-errCh
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
	}
	return nil
}

func (r *Runner) runExchange(ctx context.Context, adapter exchange.Adapter) error {
	cfg := r.configFor(adapter.Name())
	backoff := cfg.ReconnectBaseDelay
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err := r.connectOnce(ctx, adapter, cfg)
		if err == nil {
			backoff = cfg.ReconnectBaseDelay
			continue
		}
		if ctx.Err() == nil {
			log.Printf("%s collector reconnect after error: %v", adapter.Name(), err)
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
	if len(symbols) == 0 {
		return errors.New("no active symbols")
	}

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, adapter.WSURL(), nil)
	if err != nil {
		return err
	}
	defer conn.Close()

	requestID := time.Now().UnixNano() % math.MaxInt32
	requestID, err = sendSubscriptions(conn, adapter, symbols, requestID, cfg.SubscriptionChunkSize)
	if err != nil {
		return err
	}
	log.Printf("%s collector connected symbols=%d stream=%s", adapter.Name(), len(symbols), stream.NameForExchange(adapter.Name(), cfg.Shard))

	writerCtx, cancelWriter := context.WithCancel(ctx)
	defer cancelWriter()
	tickCh := make(chan market.Tick, 50_000)
	writeErrCh := make(chan error, 1)
	go r.writeTicks(writerCtx, stream.NameForExchange(adapter.Name(), cfg.Shard), cfg, tickCh, writeErrCh)

	refreshAt := time.Now().Add(cfg.SymbolRefreshInterval)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		select {
		case err := <-writeErrCh:
			return err
		default:
		}
		if !time.Now().Before(refreshAt) {
			symbols, requestID, err = r.refreshConnectionSubscriptions(ctx, conn, adapter, symbols, requestID, cfg)
			if err != nil {
				return err
			}
			refreshAt = time.Now().Add(cfg.SymbolRefreshInterval)
		}
		deadline := time.Now().Add(30 * time.Second)
		if untilRefresh := time.Until(refreshAt); untilRefresh > 0 && untilRefresh < 30*time.Second {
			deadline = time.Now().Add(untilRefresh)
		}
		_ = conn.SetReadDeadline(deadline)
		_, payload, err := conn.ReadMessage()
		if err != nil {
			if isTimeout(err) && time.Now().Before(refreshAt) {
				_ = conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second))
				continue
			}
			if isTimeout(err) && !time.Now().Before(refreshAt) {
				continue
			}
			return err
		}
		ticks, err := adapter.ParseMessage(payload)
		if err != nil {
			log.Printf("%s parse message failed: %v", adapter.Name(), err)
			continue
		}
		for _, tick := range ticks {
			if tick.RecvMS == 0 {
				tick.RecvMS = market.NowMS()
			}
			if r.publisher != nil {
				tick, err = r.publisher.PublishTick(ctx, tick)
				if err != nil {
					return err
				}
			}
			if err := enqueueTick(ctx, tickCh, writeErrCh, tick); err != nil {
				return err
			}
		}
	}
}

func (r *Runner) refreshConnectionSubscriptions(
	ctx context.Context,
	conn *websocket.Conn,
	adapter exchange.Adapter,
	current []string,
	requestID int64,
	cfg Config,
) ([]string, int64, error) {
	next, err := r.refreshSymbols(ctx, adapter)
	if err != nil {
		return current, requestID, err
	}
	if len(next) == 0 {
		return current, requestID, errors.New("no active symbols")
	}
	requestID, subscribed, unsubscribed, err := syncSubscriptions(conn, adapter, current, next, requestID, cfg.SubscriptionChunkSize)
	if err != nil {
		return current, requestID, err
	}
	if subscribed > 0 || unsubscribed > 0 {
		log.Printf("%s subscriptions refreshed active=%d subscribe=%d unsubscribe=%d", adapter.Name(), len(next), subscribed, unsubscribed)
	}
	return next, requestID, nil
}

func (r *Runner) writeTicks(ctx context.Context, streamName string, cfg Config, ticks <-chan market.Tick, errCh chan<- error) {
	batchSize := cfg.WriteBatchSize
	if batchSize <= 0 {
		batchSize = 500
	}
	flushInterval := cfg.WriteFlushInterval
	if flushInterval <= 0 {
		flushInterval = 50 * time.Millisecond
	}
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()
	batch := make([]market.Tick, 0, batchSize)

	flush := func() bool {
		if len(batch) == 0 {
			return true
		}
		if _, err := r.writer.AddTicks(ctx, streamName, batch, cfg.StreamMaxLen); err != nil {
			select {
			case errCh <- err:
			default:
			}
			return false
		}
		batch = batch[:0]
		return true
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case tick := <-ticks:
			batch = append(batch, tick)
			if len(batch) >= batchSize && !flush() {
				return
			}
		case <-ticker.C:
			if !flush() {
				return
			}
		}
	}
}

func enqueueTick(ctx context.Context, tickCh chan<- market.Tick, errCh <-chan error, tick market.Tick) error {
	select {
	case tickCh <- tick:
		return nil
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Runner) refreshSymbols(ctx context.Context, adapter exchange.Adapter) ([]string, error) {
	symbols, err := adapter.FetchSymbols(ctx, r.client)
	if err != nil {
		return nil, err
	}
	if r.store != nil {
		if err := r.store.UpsertSymbols(ctx, symbols); err != nil {
			return nil, err
		}
	}
	active := make([]string, 0, len(symbols))
	for _, symbol := range symbols {
		if symbol.IsActive {
			active = append(active, symbol.Symbol)
		}
	}
	sort.Strings(active)
	log.Printf("%s symbol refresh active=%d total=%d", adapter.Name(), len(active), len(symbols))
	return active, nil
}

func (r *Runner) configFor(exchangeName string) Config {
	cfg := r.configs[exchangeName]
	if cfg.SymbolRefreshInterval <= 0 {
		cfg.SymbolRefreshInterval = 2 * time.Minute
	}
	if cfg.ReconnectBaseDelay <= 0 {
		cfg.ReconnectBaseDelay = time.Second
	}
	if cfg.ReconnectMaxDelay <= 0 {
		cfg.ReconnectMaxDelay = time.Minute
	}
	if cfg.SubscriptionChunkSize <= 0 {
		cfg.SubscriptionChunkSize = 100
	}
	if cfg.WriteBatchSize <= 0 {
		cfg.WriteBatchSize = 500
	}
	if cfg.WriteFlushInterval <= 0 {
		cfg.WriteFlushInterval = 50 * time.Millisecond
	}
	return cfg
}

func syncSubscriptions(
	conn *websocket.Conn,
	adapter exchange.Adapter,
	current []string,
	next []string,
	requestID int64,
	chunkSize int,
) (int64, int, int, error) {
	subscribe, unsubscribe := diffSymbols(current, next)
	var err error
	if len(unsubscribe) > 0 {
		requestID, err = sendUnsubscriptions(conn, adapter, unsubscribe, requestID, chunkSize)
		if err != nil {
			return requestID, 0, 0, err
		}
	}
	if len(subscribe) > 0 {
		requestID, err = sendSubscriptions(conn, adapter, subscribe, requestID, chunkSize)
		if err != nil {
			return requestID, 0, 0, err
		}
	}
	return requestID, len(subscribe), len(unsubscribe), nil
}

func diffSymbols(current []string, next []string) ([]string, []string) {
	currentSet := make(map[string]struct{}, len(current))
	for _, symbol := range current {
		currentSet[symbol] = struct{}{}
	}
	nextSet := make(map[string]struct{}, len(next))
	for _, symbol := range next {
		nextSet[symbol] = struct{}{}
	}
	subscribe := make([]string, 0)
	for _, symbol := range next {
		if _, ok := currentSet[symbol]; !ok {
			subscribe = append(subscribe, symbol)
		}
	}
	unsubscribe := make([]string, 0)
	for _, symbol := range current {
		if _, ok := nextSet[symbol]; !ok {
			unsubscribe = append(unsubscribe, symbol)
		}
	}
	return subscribe, unsubscribe
}

func sendSubscriptions(conn *websocket.Conn, adapter exchange.Adapter, symbols []string, requestID int64, chunkSize int) (int64, error) {
	return sendSubscriptionPayloads(conn, symbols, requestID, chunkSize, adapter.BuildSubscribePayload)
}

func sendUnsubscriptions(conn *websocket.Conn, adapter exchange.Adapter, symbols []string, requestID int64, chunkSize int) (int64, error) {
	return sendSubscriptionPayloads(conn, symbols, requestID, chunkSize, adapter.BuildUnsubscribePayload)
}

type subscriptionPayloadBuilder func(symbols []string, requestID int64) ([]byte, error)

func sendSubscriptionPayloads(conn *websocket.Conn, symbols []string, requestID int64, chunkSize int, build subscriptionPayloadBuilder) (int64, error) {
	if chunkSize <= 0 {
		chunkSize = 100
	}
	for index := 0; index < len(symbols); index += chunkSize {
		end := index + chunkSize
		if end > len(symbols) {
			end = len(symbols)
		}
		payload, err := build(symbols[index:end], requestID)
		if err != nil {
			return requestID, err
		}
		if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
			return requestID, err
		}
		requestID++
	}
	return requestID, nil
}

func isTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func minDuration(a time.Duration, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
