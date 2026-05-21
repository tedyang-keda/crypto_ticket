package exchange

import "testing"

func TestBinanceParseTradeMessage(t *testing.T) {
	adapter := NewBinanceFuturesAdapter("um_futures", "https://fapi.binance.com", "wss://example")
	ticks, err := adapter.ParseMessage([]byte(`{"e":"trade","E":1779340001001,"T":1779340001000,"s":"BTCUSDT","t":123,"p":"100000.12","q":"0.01","m":true}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(ticks) != 1 {
		t.Fatalf("expected one tick, got %d", len(ticks))
	}
	tick := ticks[0]
	if tick.Exchange != "binance" || tick.Symbol != "BTCUSDT" || tick.Price != 100000.12 || tick.Size != 0.01 {
		t.Fatalf("unexpected tick: %+v", tick)
	}
	if tick.Side != "sell" || tick.TradeID != "123" {
		t.Fatalf("unexpected side/trade id: %+v", tick)
	}
}

func TestOKXParseTradesMessage(t *testing.T) {
	adapter := NewOKXAdapter("SWAP", "https://www.okx.com", "wss://example")
	ticks, err := adapter.ParseMessage([]byte(`{"arg":{"channel":"trades","instId":"BTC-USDT-SWAP"},"data":[{"instId":"BTC-USDT-SWAP","tradeId":"9","px":"70000.5","sz":"2","side":"buy","ts":"1779340001000"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(ticks) != 1 {
		t.Fatalf("expected one tick, got %d", len(ticks))
	}
	tick := ticks[0]
	if tick.Exchange != "okx" || tick.Symbol != "BTC-USDT-SWAP" || tick.Price != 70000.5 || tick.Size != 2 {
		t.Fatalf("unexpected tick: %+v", tick)
	}
	if tick.Side != "buy" || tick.TradeID != "9" {
		t.Fatalf("unexpected side/trade id: %+v", tick)
	}
}
