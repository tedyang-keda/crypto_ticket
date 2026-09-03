package collector

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"crypto-ticket/internal/exchange"
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
	connected, err := runner.readStaticStream(ctx, adapter, wsURL, []string{"BTCUSDT"}, 0, Config{
		WSPingInterval: 20 * time.Millisecond,
		WSPongWait:     200 * time.Millisecond,
	})
	if !connected || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("idle connection should survive until context cancellation: connected=%t err=%v", connected, err)
	}
	select {
	case <-pingReceived:
	default:
		t.Fatal("expected client heartbeat ping")
	}
}
