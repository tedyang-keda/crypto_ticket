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
