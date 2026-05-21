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
	Shard                 int
}

type Runner struct {
	adapters []exchange.Adapter
	writer   stream.TickStream
	store    storage.HistoricalStore
	configs  map[string]Config
	client   *http.Client
}

func NewRunner(adapters []exchange.Adapter, writer stream.TickStream, store storage.HistoricalStore, configs map[string]Config) *Runner {
	return &Runner{
		adapters: adapters,
		writer:   writer,
		store:    store,
		configs:  configs,
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
	if err := sendSubscriptions(conn, adapter, symbols, requestID, cfg.SubscriptionChunkSize); err != nil {
		return err
	}
	log.Printf("%s collector connected symbols=%d stream=%s", adapter.Name(), len(symbols), stream.NameForExchange(adapter.Name(), cfg.Shard))

	refreshAt := time.Now().Add(cfg.SymbolRefreshInterval)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
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
				return nil
			}
			return err
		}
		ticks, err := adapter.ParseMessage(payload)
		if err != nil {
			log.Printf("%s parse message failed: %v", adapter.Name(), err)
			continue
		}
		streamName := stream.NameForExchange(adapter.Name(), cfg.Shard)
		for _, tick := range ticks {
			if tick.RecvMS == 0 {
				tick.RecvMS = market.NowMS()
			}
			if _, err := r.writer.AddTick(ctx, streamName, tick, cfg.StreamMaxLen); err != nil {
				return err
			}
		}
		if !time.Now().Before(refreshAt) {
			return nil
		}
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
	return cfg
}

func sendSubscriptions(conn *websocket.Conn, adapter exchange.Adapter, symbols []string, requestID int64, chunkSize int) error {
	if chunkSize <= 0 {
		chunkSize = 100
	}
	for index := 0; index < len(symbols); index += chunkSize {
		end := index + chunkSize
		if end > len(symbols) {
			end = len(symbols)
		}
		payload, err := adapter.BuildSubscribePayload(symbols[index:end], requestID)
		if err != nil {
			return err
		}
		if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
			return err
		}
		requestID++
	}
	return nil
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
