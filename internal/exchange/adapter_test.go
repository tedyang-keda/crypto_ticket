package exchange

import (
	"math"
	"testing"
)

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

func TestBinanceParseUMarginKlineMessage(t *testing.T) {
	adapter := NewBinanceFuturesAdapter("um_futures", "https://fapi.binance.com", "wss://example")
	bars, err := adapter.ParseKlineMessage([]byte(`{"e":"kline","E":1779340060000,"s":"BTCUSDT","k":{"t":1779340000000,"T":1779340059999,"s":"BTCUSDT","i":"1m","o":"100","c":"105","h":"110","l":"95","v":"2.5","n":12,"x":true,"q":"260","V":"1","Q":"100"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(bars) != 1 {
		t.Fatalf("expected one bar, got %d", len(bars))
	}
	bar := bars[0]
	if bar.MarginType != "umargin" || bar.Volume != 2.5 || bar.VolumeUnit != "BTC" || bar.QuoteVolume != 260 || bar.QuoteUnit != "USDT" || !bar.IsFinal {
		t.Fatalf("unexpected bar: %+v", bar)
	}
}

func TestBinanceParseCoinMarginKlineMessage(t *testing.T) {
	adapter := NewBinanceFuturesAdapter("coin_futures", "https://dapi.binance.com", "wss://example")
	bars, err := adapter.ParseKlineMessage([]byte(`{"e":"kline","E":1779340060000,"s":"BTCUSD_PERP","k":{"t":1779340000000,"T":1779340059999,"s":"BTCUSD_PERP","i":"1m","o":"100","c":"105","h":"110","l":"95","v":"25","n":12,"x":true,"q":"0.5"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(bars) != 1 {
		t.Fatalf("expected one bar, got %d", len(bars))
	}
	bar := bars[0]
	if bar.MarginType != "coinmargin" || bar.Volume != 25 || bar.VolumeUnit != "contract" || bar.ContractVolume != 25 || bar.QuoteVolume != 0.5 || bar.QuoteUnit != "BTC" {
		t.Fatalf("unexpected bar: %+v", bar)
	}
}

func TestBinanceUMarginStaticStreamURLUsesMarketNamespace(t *testing.T) {
	adapter := NewBinanceFuturesAdapter("um_futures", "https://fapi.binance.com", "wss://fstream.binance.com/ws")
	got := adapter.StaticStreamURL([]string{"BTCUSDT", "ETHUSDT"})
	want := "wss://fstream.binance.com/market/stream?streams=btcusdt@kline_1m/ethusdt@kline_1m"
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

func TestBinanceCoinMarginStaticStreamURLKeepsDStreamNamespace(t *testing.T) {
	adapter := NewBinanceFuturesAdapter("coin_futures", "https://dapi.binance.com", "wss://dstream.binance.com/ws")
	got := adapter.StaticStreamURL([]string{"BTCUSD_PERP"})
	want := "wss://dstream.binance.com/stream?streams=btcusd_perp@kline_1m"
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
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

func TestOKXParseUMarginCandleMessage(t *testing.T) {
	adapter := NewOKXAdapter("SWAP", "https://www.okx.com", "wss://example")
	adapter.replaceInstrumentSpecs(map[string]okxInstrumentSpec{
		"BTC-USDT-SWAP": {baseCcy: "BTC", quoteCcy: "USDT", settleCcy: "USDT"},
	})
	bars, err := adapter.ParseKlineMessage([]byte(`{"arg":{"channel":"candle1m","instId":"BTC-USDT-SWAP"},"data":[["1779340000000","100","110","95","105","25","0.25","26000","1"]]}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(bars) != 1 {
		t.Fatalf("expected one bar, got %d", len(bars))
	}
	bar := bars[0]
	if bar.MarginType != "umargin" || bar.Volume != 0.25 || bar.VolumeUnit != "BTC" || bar.QuoteVolume != 26000 || bar.QuoteUnit != "USDT" || bar.ContractVolume != 25 {
		t.Fatalf("unexpected bar: %+v", bar)
	}
}

func TestOKXParseCoinMarginCandleMessage(t *testing.T) {
	adapter := NewOKXAdapter("SWAP", "https://www.okx.com", "wss://example")
	adapter.replaceInstrumentSpecs(map[string]okxInstrumentSpec{
		"BTC-USD-SWAP": {baseCcy: "BTC", quoteCcy: "USD", settleCcy: "BTC"},
	})
	bars, err := adapter.ParseKlineMessage([]byte(`{"arg":{"channel":"candle1m","instId":"BTC-USD-SWAP"},"data":[["1779340000000","100","110","95","105","25","0.5","25000","1"]]}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(bars) != 1 {
		t.Fatalf("expected one bar, got %d", len(bars))
	}
	bar := bars[0]
	if bar.MarginType != "coinmargin" || bar.Volume != 25 || bar.VolumeUnit != "contract" || bar.QuoteVolume != 0.5 || bar.QuoteUnit != "BTC" || bar.ContractVolume != 25 {
		t.Fatalf("unexpected bar: %+v", bar)
	}
}

func TestOKXParseSwapTradeUsesBaseContractValue(t *testing.T) {
	adapter := NewOKXAdapter("SWAP", "https://www.okx.com", "wss://example")
	adapter.replaceInstrumentSpecs(map[string]okxInstrumentSpec{
		"BTC-USDT-SWAP": {
			baseCcy:   "BTC",
			quoteCcy:  "USDT",
			settleCcy: "USDT",
			ctVal:     0.01,
			ctValCcy:  "BTC",
		},
	})
	ticks, err := adapter.ParseMessage([]byte(`{"arg":{"channel":"trades","instId":"BTC-USDT-SWAP"},"data":[{"instId":"BTC-USDT-SWAP","tradeId":"9","px":"70000.5","sz":"2","side":"buy","ts":"1779340001000"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(ticks) != 1 {
		t.Fatalf("expected one tick, got %d", len(ticks))
	}
	assertFloatEqual(t, ticks[0].Size, 0.02)
}

func TestOKXParseSwapTradeConvertsQuoteContractValueToBase(t *testing.T) {
	adapter := NewOKXAdapter("SWAP", "https://www.okx.com", "wss://example")
	adapter.replaceInstrumentSpecs(map[string]okxInstrumentSpec{
		"BTC-USD-SWAP": {
			baseCcy:   "BTC",
			quoteCcy:  "USD",
			settleCcy: "BTC",
			ctVal:     100,
			ctValCcy:  "USD",
		},
	})
	ticks, err := adapter.ParseMessage([]byte(`{"arg":{"channel":"trades","instId":"BTC-USD-SWAP"},"data":[{"instId":"BTC-USD-SWAP","tradeId":"10","px":"50000","sz":"3","side":"sell","ts":"1779340001000"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(ticks) != 1 {
		t.Fatalf("expected one tick, got %d", len(ticks))
	}
	assertFloatEqual(t, ticks[0].Size, 0.006)
}

func assertFloatEqual(t *testing.T, got float64, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-12 {
		t.Fatalf("expected %v, got %v", want, got)
	}
}
