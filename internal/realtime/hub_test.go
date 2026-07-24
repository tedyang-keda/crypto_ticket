package realtime

import "testing"

func TestKlineSubscriptionsDeduplicatesChannels(t *testing.T) {
	hub := NewHub()
	first := hub.Subscribe()
	defer first.Close()
	second := hub.Subscribe()
	defer second.Close()
	channel := KlineChannel("okx", "KORU-USDT-SWAP", "1W")
	first.Add(channel)
	second.Add(channel)
	second.Add(TickerChannel("okx", "KORU-USDT-SWAP"))

	subscriptions := hub.KlineSubscriptions()
	if len(subscriptions) != 1 {
		t.Fatalf("unexpected subscriptions: %+v", subscriptions)
	}
	got := subscriptions[0]
	if got.Exchange != "okx" || got.Symbol != "KORU-USDT-SWAP" || got.Timeframe != "1W" {
		t.Fatalf("unexpected subscription: %+v", got)
	}
}
