package collector

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"crypto-ticket/internal/exchange"
	"crypto-ticket/internal/market"
)

func TestDiffSymbols(t *testing.T) {
	subscribe, unsubscribe := diffSymbols(
		[]string{"ADAUSDT", "BTCUSDT", "ETHUSDT"},
		[]string{"BTCUSDT", "ETHUSDT", "SOLUSDT"},
	)
	if !reflect.DeepEqual(subscribe, []string{"SOLUSDT"}) {
		t.Fatalf("unexpected subscribe diff: %+v", subscribe)
	}
	if !reflect.DeepEqual(unsubscribe, []string{"ADAUSDT"}) {
		t.Fatalf("unexpected unsubscribe diff: %+v", unsubscribe)
	}
}

func TestDiffSymbolsNoChanges(t *testing.T) {
	subscribe, unsubscribe := diffSymbols(
		[]string{"BTCUSDT", "ETHUSDT"},
		[]string{"BTCUSDT", "ETHUSDT"},
	)
	if len(subscribe) != 0 || len(unsubscribe) != 0 {
		t.Fatalf("expected no diff, subscribe=%+v unsubscribe=%+v", subscribe, unsubscribe)
	}
}

func TestSameSymbolsIgnoresOrderAndDuplicates(t *testing.T) {
	if !sameSymbols(
		[]string{"BTCUSDT", "ETHUSDT", "BTCUSDT"},
		[]string{"ethusdt", "btcusdt"},
	) {
		t.Fatal("expected equivalent symbol sets")
	}
	if sameSymbols([]string{"BTCUSDT"}, []string{"BTCUSDT", "SOLUSDT"}) {
		t.Fatal("expected added symbol to require reconnect")
	}
}

func TestShardSymbolsBalancesAndInterleaves(t *testing.T) {
	symbols := []string{"A", "B", "C", "D", "E", "F", "G"}
	shards := shardSymbols(symbols, 3)
	want := [][]string{{"A", "D", "G"}, {"B", "E"}, {"C", "F"}}
	if !reflect.DeepEqual(shards, want) {
		t.Fatalf("unexpected shards: got=%v want=%v", shards, want)
	}
	for _, shard := range shards {
		if len(shard) > 3 {
			t.Fatalf("shard exceeds maximum: %v", shard)
		}
	}
}

func TestShardSymbolsAvoidsTinyFinalShard(t *testing.T) {
	symbols := make([]string, 757)
	for index := range symbols {
		symbols[index] = "symbol"
	}
	shards := shardSymbols(symbols, 50)
	if len(shards) != 16 {
		t.Fatalf("unexpected shard count: %d", len(shards))
	}
	for index, shard := range shards {
		if len(shard) < 47 || len(shard) > 48 {
			t.Fatalf("shard %d is unbalanced: %d", index, len(shard))
		}
	}
}

func TestWebsocketHeartbeatDurationsKeepPingBeforeTimeout(t *testing.T) {
	ping, wait := websocketHeartbeatDurations(Config{WSPingInterval: time.Minute, WSPongWait: 30 * time.Second})
	if ping != 15*time.Second || wait != 30*time.Second {
		t.Fatalf("unexpected heartbeat durations: ping=%s wait=%s", ping, wait)
	}
}

func TestStaticStreamHeartbeatKeepsIdleConnectionAlive(t *testing.T) {
	pingReceived := make(chan struct{}, 1)
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		conn, err := upgrader.Upgrade(w, request, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.SetPingHandler(func(data string) error {
			select {
			case pingReceived <- struct{}{}:
			default:
			}
			return conn.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(time.Second))
		})
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	adapter := exchange.NewBinanceFuturesAdapter("um_futures", "", wsURL)
	runner := NewRunner(nil, nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
	defer cancel()
	stable, err := runner.readStaticStream(ctx, adapter, wsURL, []string{"BTCUSDT"}, 0, Config{
		ReconnectResetAfter: 50 * time.Millisecond,
		WSPingInterval:      20 * time.Millisecond,
		WSPongWait:          200 * time.Millisecond,
	})
	if !stable || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("idle connection should stay healthy until context cancellation: stable=%t err=%v", stable, err)
	}
	select {
	case <-pingReceived:
	default:
		t.Fatal("expected client heartbeat ping")
	}
}

func TestReconnectBackoffResetsOnlyAfterStableSession(t *testing.T) {
	for _, static := range []bool{false, true} {
		name := "subscription_connection"
		if static {
			name = "static_shard"
		}
		t.Run(name, func(t *testing.T) {
			type session struct {
				connectedAt time.Time
				closedAt    time.Time
			}
			// Short successful handshakes still back off. The third and fifth
			// sessions carry healthy data past the stability window and reset it.
			healthySessions := []bool{false, false, true, false, true, false, false}
			sessions := make(chan session, len(healthySessions))
			var attempt atomic.Int32
			upgrader := websocket.Upgrader{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				conn, err := upgrader.Upgrade(w, request, nil)
				if err != nil {
					return
				}
				index := int(attempt.Add(1)) - 1
				connectedAt := time.Now()
				if index < len(healthySessions) && healthySessions[index] {
					_ = conn.WriteMessage(websocket.TextMessage, []byte("{}"))
					time.Sleep(80 * time.Millisecond)
					_ = conn.WriteMessage(websocket.TextMessage, []byte("{}"))
				}
				_ = conn.Close()
				if index < len(healthySessions) {
					sessions <- session{connectedAt: connectedAt, closedAt: time.Now()}
				}
			}))
			defer server.Close()

			adapter := reconnectTestAdapter{url: "ws" + strings.TrimPrefix(server.URL, "http")}
			runner := NewRunner(nil, nil, nil)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cfg := Config{
				ReconnectBaseDelay:  50 * time.Millisecond,
				ReconnectMaxDelay:   400 * time.Millisecond,
				ReconnectResetAfter: 50 * time.Millisecond,
			}
			done := make(chan error, 1)
			go func() {
				if static {
					runner.runStaticShard(ctx, adapter, reconnectTestStaticAdapter{adapter}, []string{"BTCUSDT"}, 0, cfg)
					done <- ctx.Err()
					return
				}
				done <- runner.runExchange(ctx, Runtime{Adapter: adapter, Config: cfg})
			}()
			wantDelays := []time.Duration{50, 100, 50, 100, 50, 100}
			var previous session
			for index := range healthySessions {
				var current session
				select {
				case current = <-sessions:
				case <-ctx.Done():
					t.Fatalf("timed out waiting for connection %d: %v", index+1, ctx.Err())
				}
				if index > 0 {
					got := current.connectedAt.Sub(previous.closedAt)
					want := wantDelays[index-1] * time.Millisecond
					if got < want*4/5 || got >= want+100*time.Millisecond {
						t.Fatalf("connection %d retry delay: got=%s want approximately %s", index+1, got, want)
					}
				}
				previous = current
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("runner should stop on cancellation: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("runner did not stop after cancellation")
			}
		})
	}
}

type reconnectTestAdapter struct {
	exchange.Adapter
	url string
}

func (a reconnectTestAdapter) Name() string       { return "test" }
func (a reconnectTestAdapter) MarketType() string { return "futures" }
func (a reconnectTestAdapter) WSURL() string      { return a.url }
func (a reconnectTestAdapter) FetchSymbols(context.Context, *http.Client) ([]market.SymbolInfo, error) {
	return []market.SymbolInfo{{Symbol: "BTCUSDT", IsActive: true}}, nil
}
func (a reconnectTestAdapter) BuildSubscribePayload([]string, int64) ([]byte, error) {
	return []byte("{}"), nil
}
func (a reconnectTestAdapter) ParseKlineMessage([]byte) ([]market.Bar, error) {
	return nil, nil
}

type reconnectTestStaticAdapter struct{ reconnectTestAdapter }

func (a reconnectTestStaticAdapter) StaticStreamURL([]string) string { return a.url }
