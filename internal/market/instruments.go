package market

import (
	"fmt"
	"strings"
)

func ClassifyBinanceSymbol(marketType string, payload map[string]any) InstrumentClassification {
	source := SourceMarket("binance", marketType)
	contractType := strings.ToUpper(stringField(payload, "contractType"))
	if contractType == "" && !strings.EqualFold(strings.TrimSpace(marketType), "um_futures") {
		contractType = "SPOT"
	}
	underlyingType := strings.ToUpper(stringField(payload, "underlyingType"))
	status := strings.ToUpper(stringField(payload, "status"))
	if status == "" {
		status = strings.ToUpper(stringField(payload, "contractStatus"))
	}

	assetClass := AssetClassUnknown
	switch underlyingType {
	case "COIN":
		assetClass = AssetClassCrypto
	case "EQUITY":
		assetClass = AssetClassEquity
	case "KR_EQUITY":
		assetClass = AssetClassKREquity
	case "COMMODITY":
		assetClass = AssetClassCommodity
	case "INDEX":
		assetClass = AssetClassIndex
	case "PREMARKET":
		assetClass = AssetClassPreMarket
	default:
		if !strings.EqualFold(strings.TrimSpace(marketType), "um_futures") {
			assetClass = AssetClassCrypto
		}
	}

	ruleType := RuleNormal
	if assetClass == AssetClassPreMarket {
		ruleType = RulePreMarket
	}
	lifecyclePhase := PhaseUnknown
	switch status {
	case "TRADING":
		lifecyclePhase = PhaseContinuous
	case "PENDING_TRADING", "PRE_TRADING":
		lifecyclePhase = PhasePreopen
	case "SETTLING", "DELIVERING", "PRE_DELIVERING":
		lifecyclePhase = PhaseSuspend
	case "EXPIRED":
		lifecyclePhase = PhaseExpired
	case "":
		lifecyclePhase = PhaseUnknown
	default:
		lifecyclePhase = PhaseSuspend
	}
	return InstrumentClassification{
		SourceMarket:   source,
		InstrumentType: firstNonEmptyString(contractType, strings.ToUpper(strings.TrimSpace(marketType))),
		AssetClass:     assetClass,
		RuleType:       ruleType,
		LifecyclePhase: lifecyclePhase,
	}
}

func ClassifyOKXSymbol(instType string, payload map[string]any) InstrumentClassification {
	instType = strings.ToUpper(strings.TrimSpace(firstNonEmptyString(stringField(payload, "instType"), instType)))
	source := SourceMarket("okx", instType)
	instCategory := strings.TrimSpace(stringField(payload, "instCategory"))
	ruleType := strings.ToLower(strings.TrimSpace(firstNonEmptyString(stringField(payload, "ruleType"), RuleNormal)))
	state := strings.ToLower(strings.TrimSpace(stringField(payload, "state")))

	assetClass := AssetClassUnknown
	switch instCategory {
	case "1":
		assetClass = AssetClassCrypto
	case "3":
		assetClass = AssetClassEquity
	case "4":
		assetClass = AssetClassCommodity
	case "5":
		assetClass = AssetClassForex
	case "6":
		assetClass = AssetClassBonds
	}
	if ruleType == RulePreMarket {
		assetClass = AssetClassPreMarket
	}

	lifecyclePhase := PhaseUnknown
	switch state {
	case "live":
		if ruleType == RulePreMarket {
			lifecyclePhase = PhasePreMarket
		} else {
			lifecyclePhase = PhaseContinuous
		}
	case "preopen":
		lifecyclePhase = PhasePreopen
	case "rebase":
		lifecyclePhase = PhaseRebase
	case "suspend", "post_only", "test", "settling":
		lifecyclePhase = PhaseSuspend
	case "":
		lifecyclePhase = PhaseUnknown
	default:
		lifecyclePhase = PhaseUnknown
	}

	return InstrumentClassification{
		SourceMarket:   source,
		InstrumentType: instType,
		AssetClass:     assetClass,
		RuleType:       ruleType,
		LifecyclePhase: lifecyclePhase,
	}
}

func ApplyClassificationToBar(bar Bar, symbol SymbolInfo) Bar {
	if symbol.SourceMarket != "" {
		bar.SourceMarket = symbol.SourceMarket
	}
	if symbol.InstrumentType != "" {
		bar.InstrumentType = symbol.InstrumentType
	}
	if symbol.AssetClass != "" {
		bar.AssetClass = symbol.AssetClass
	}
	if symbol.RuleType != "" {
		bar.RuleType = symbol.RuleType
	}
	if symbol.LifecyclePhase != "" {
		bar.LifecyclePhase = symbol.LifecyclePhase
	}
	return DecorateBar(bar)
}

func ApplyClassificationToTick(tick Tick, symbol SymbolInfo) Tick {
	if symbol.SourceMarket != "" {
		tick.SourceMarket = symbol.SourceMarket
	}
	if symbol.InstrumentType != "" {
		tick.InstrumentType = symbol.InstrumentType
	}
	if symbol.AssetClass != "" {
		tick.AssetClass = symbol.AssetClass
	}
	if symbol.RuleType != "" {
		tick.RuleType = symbol.RuleType
	}
	if symbol.LifecyclePhase != "" {
		tick.LifecyclePhase = symbol.LifecyclePhase
	}
	return tick
}

func ApplyClassificationFieldsToBar(bar Bar, classification InstrumentClassification) Bar {
	bar.SourceMarket = classification.SourceMarket
	bar.InstrumentType = classification.InstrumentType
	bar.AssetClass = classification.AssetClass
	bar.RuleType = classification.RuleType
	bar.LifecyclePhase = classification.LifecyclePhase
	return DecorateBar(bar)
}

func ApplyClassificationFieldsToSymbol(symbol SymbolInfo, classification InstrumentClassification) SymbolInfo {
	symbol.SourceMarket = classification.SourceMarket
	symbol.InstrumentType = classification.InstrumentType
	symbol.AssetClass = classification.AssetClass
	symbol.RuleType = classification.RuleType
	symbol.LifecyclePhase = classification.LifecyclePhase
	return symbol
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func stringField(payload map[string]any, key string) string {
	if payload == nil {
		return ""
	}
	value, ok := payload[key]
	if !ok || value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return typed
	default:
		return strings.TrimSpace(fmt.Sprint(typed))
	}
}
