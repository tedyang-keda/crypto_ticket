package stream

import "testing"

func TestNameForExchange(t *testing.T) {
	if got := NameForExchange("Binance", 0); got != "ticks:binance:00" {
		t.Fatalf("unexpected stream name: %s", got)
	}
	if got := NameForExchange("okx", 12); got != "ticks:okx:12" {
		t.Fatalf("unexpected stream name: %s", got)
	}
}

func TestTickFromFields(t *testing.T) {
	tick, err := TickFromFields(map[string]any{
		"exchange":   "binance",
		"symbol":     "btcusdt",
		"ts_ms":      "1779340001000",
		"price":      "100000.12",
		"size":       "0.01",
		"side":       "buy",
		"trade_id":   "t1",
		"event_type": "trade",
		"source":     "ws",
		"recv_ms":    "1779340001005",
		"raw":        `{"ok":true}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	if tick.Exchange != "binance" || tick.Symbol != "BTCUSDT" || tick.Price != 100000.12 || tick.Size != 0.01 {
		t.Fatalf("unexpected tick: %+v", tick)
	}
	if string(tick.Raw) != `{"ok":true}` {
		t.Fatalf("unexpected raw: %s", string(tick.Raw))
	}
}
