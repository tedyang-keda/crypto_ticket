package aggregator

import (
	"testing"

	"crypto-ticket/internal/market"
)

func TestEngineUpdatesLiveAndFinalBars(t *testing.T) {
	engine := NewEngine([]string{"1m", "5m"})
	base := int64(1779262200000)

	result := engine.OnTick(market.Tick{Exchange: "binance", Symbol: "BTCUSDT", TsMS: base + 1_000, Price: 100, Size: 1})
	if len(result.LiveBars) != 2 {
		t.Fatalf("expected 2 live bars, got %d", len(result.LiveBars))
	}
	if len(result.FinalBars) != 0 {
		t.Fatalf("expected no final bars, got %d", len(result.FinalBars))
	}

	result = engine.OnTick(market.Tick{Exchange: "binance", Symbol: "BTCUSDT", TsMS: base + 61_000, Price: 101, Size: 2})
	if len(result.FinalBars) != 1 {
		t.Fatalf("expected one finalized 1m bar, got %d", len(result.FinalBars))
	}
	final := result.FinalBars[0]
	if final.Timeframe != "1m" || !final.IsFinal || final.ClosePrice != 100 {
		t.Fatalf("unexpected final bar: %+v", final)
	}
}

func TestEngineFillsGapBars(t *testing.T) {
	engine := NewEngine([]string{"1m"})
	base := int64(1779262200000)
	engine.OnTick(market.Tick{Exchange: "okx", Symbol: "BTC-USDT-SWAP", TsMS: base + 10_000, Price: 100, Size: 1})

	result := engine.OnTick(market.Tick{Exchange: "okx", Symbol: "BTC-USDT-SWAP", TsMS: base + 3*60_000 + 1_000, Price: 101, Size: 1})
	gaps := 0
	for _, bar := range result.FinalBars {
		if bar.Reason == "gap" {
			gaps++
		}
	}
	if gaps != 2 {
		t.Fatalf("expected 2 gap bars, got %d", gaps)
	}
}
