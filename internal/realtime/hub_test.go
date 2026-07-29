package realtime

import (
	"testing"

	"crypto-ticket/internal/market"
)

type hubObserver struct {
	connections int
	drops       int
}

func (o *hubObserver) WSConnection(delta int) { o.connections += delta }
func (o *hubObserver) WSEventDropped()        { o.drops++ }

func TestHubReportsSlowSubscriberDrops(t *testing.T) {
	observer := &hubObserver{}
	hub := NewHub(observer)
	subscriber := hub.Subscribe()
	subscriber.Add(KlineChannel("okx", "BTC-USDT-SWAP", "1m"))
	for i := 0; i < 257; i++ {
		hub.Publish(market.Event{Type: "kline", Exchange: "okx", Symbol: "BTC-USDT-SWAP", Timeframe: "1m"})
	}
	if observer.connections != 1 || observer.drops != 1 {
		t.Fatalf("unexpected observer state connections=%d drops=%d", observer.connections, observer.drops)
	}
	subscriber.Close()
	if observer.connections != 0 {
		t.Fatalf("expected connection decrement, got %d", observer.connections)
	}
}
