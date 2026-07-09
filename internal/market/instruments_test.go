package market

import "testing"

func TestClassifyBinanceSymbolCoversTradFiAndPreMarket(t *testing.T) {
	equity := ClassifyBinanceSymbol("um_futures", map[string]any{
		"symbol":            "TSLAUSDT",
		"status":            "TRADING",
		"contractType":      "TRADIFI_PERPETUAL",
		"underlyingType":    "EQUITY",
		"underlyingSubType": []string{"TradFi"},
	})
	if equity.SourceMarket != "binance:um_futures" ||
		equity.InstrumentType != "TRADIFI_PERPETUAL" ||
		equity.AssetClass != AssetClassEquity ||
		equity.RuleType != RuleNormal ||
		equity.LifecyclePhase != PhaseContinuous {
		t.Fatalf("unexpected equity classification: %+v", equity)
	}

	preMarket := ClassifyBinanceSymbol("um_futures", map[string]any{
		"symbol":            "OPENAIUSDT",
		"status":            "TRADING",
		"contractType":      "TRADIFI_PERPETUAL",
		"underlyingType":    "PREMARKET",
		"underlyingSubType": []string{"Pre-IPO", "TradFi"},
	})
	if preMarket.AssetClass != AssetClassPreMarket || preMarket.RuleType != RulePreMarket {
		t.Fatalf("unexpected pre-market classification: %+v", preMarket)
	}
}

func TestClassifyOKXSymbolUsesCategoryAndRuleType(t *testing.T) {
	stock := ClassifyOKXSymbol("SWAP", map[string]any{
		"instId":       "AAPL-USDT-SWAP",
		"instType":     "SWAP",
		"instCategory": "3",
		"ruleType":     "normal",
		"state":        "live",
	})
	if stock.SourceMarket != "okx:SWAP" ||
		stock.AssetClass != AssetClassEquity ||
		stock.LifecyclePhase != PhaseContinuous {
		t.Fatalf("unexpected stock classification: %+v", stock)
	}

	preMarket := ClassifyOKXSymbol("SWAP", map[string]any{
		"instId":       "OPENAI-USDT-SWAP",
		"instType":     "SWAP",
		"instCategory": "3",
		"ruleType":     "pre_market",
		"state":        "live",
	})
	if preMarket.AssetClass != AssetClassPreMarket || preMarket.LifecyclePhase != PhasePreMarket {
		t.Fatalf("unexpected pre-market classification: %+v", preMarket)
	}
}

func TestApplyFactorToBarUpdatesPriceAndVolume(t *testing.T) {
	bar := Bar{
		Exchange:    "binance",
		Symbol:      "TSLAUSDT",
		Timeframe:   "1m",
		StartMS:     1,
		EndMS:       59_999,
		OpenPrice:   100,
		HighPrice:   110,
		LowPrice:    90,
		ClosePrice:  104,
		Volume:      10,
		QuoteVolume: 1000,
	}
	adjusted := ApplyFactorToBar(bar, AdjustmentFactor{
		Provider:         "vendor",
		ProviderVersion:  "v1",
		AdjMode:          PriceModeBackwardAdjusted,
		PriceMultiplier:  0.5,
		VolumeMultiplier: 2,
		EventType:        "split",
	})
	if adjusted.OpenPrice != 50 || adjusted.HighPrice != 55 || adjusted.ClosePrice != 52 {
		t.Fatalf("unexpected adjusted prices: %+v", adjusted)
	}
	if adjusted.Volume != 20 || adjusted.QuoteVolume != 1000 {
		t.Fatalf("unexpected adjusted volumes: %+v", adjusted)
	}
	if adjusted.RawOpenPrice != 100 || adjusted.RawVolume != 10 || adjusted.AdjustmentStatus != AdjustmentStatusAdjusted {
		t.Fatalf("unexpected adjustment metadata: %+v", adjusted)
	}
}
