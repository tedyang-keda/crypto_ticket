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
		if contractType == "TRADIFI_PERPETUAL" {
			assetClass = AssetClassEquity
		} else if !strings.EqualFold(strings.TrimSpace(marketType), "um_futures") {
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
	case "TRADING_HALT":
		lifecyclePhase = PhaseHalt
	case "TRADING_CANCEL_ONLY":
		lifecyclePhase = PhaseCancelOnly
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

// IsEquityLikeAssetClass reports whether an asset class is subject to
// corporate actions (splits, ex-dividend, pre-IPO share rebase) and therefore
// eligible for corporate-action handling. Crypto is deliberately excluded.
func IsEquityLikeAssetClass(assetClass string) bool {
	switch strings.ToLower(strings.TrimSpace(assetClass)) {
	case AssetClassEquity, AssetClassKREquity, AssetClassPreMarket, AssetClassIndex, AssetClassCommodity:
		return true
	default:
		return false
	}
}

// CorporateActionEventType classifies a transition between two snapshots of the
// same instrument as a corporate-action candidate, returning the event type and
// true when the change warrants corporate-action repair attention. It only fires for
// equity-like instruments so ordinary crypto churn is ignored.
func CorporateActionEventType(previous SymbolInfo, current SymbolInfo) (string, bool) {
	if !IsEquityLikeAssetClass(current.AssetClass) && !IsEquityLikeAssetClass(previous.AssetClass) {
		return "", false
	}
	// Entered the rebase state: the split/rebase is executing now.
	if current.LifecyclePhase == PhaseRebase && previous.LifecyclePhase != PhaseRebase {
		return InstrumentEventRebase, true
	}
	// Contract flagged as rebase-eligible ahead of the event.
	if strings.EqualFold(current.RuleType, RuleRebaseContract) &&
		!strings.EqualFold(previous.RuleType, RuleRebaseContract) {
		return InstrumentEventRebaseArmed, true
	}
	// Trading halted from a live state — often the pre-adjustment suspension
	// window; worth surfacing so downstream can freeze the affected bars.
	if (current.LifecyclePhase == PhaseSuspend || current.LifecyclePhase == PhaseHalt) && previous.LifecyclePhase == PhaseContinuous {
		return InstrumentEventSuspended, true
	}
	if current.LifecyclePhase == PhaseCancelOnly &&
		(previous.LifecyclePhase == PhaseHalt || previous.LifecyclePhase == PhaseSuspend) {
		return InstrumentEventCancelOnly, true
	}
	if current.LifecyclePhase == PhaseContinuous &&
		(previous.LifecyclePhase == PhaseHalt || previous.LifecyclePhase == PhaseCancelOnly ||
			previous.LifecyclePhase == PhaseSuspend || previous.LifecyclePhase == PhaseRebase) {
		return InstrumentEventResumed, true
	}
	// Instrument expired from a live state: candidate delisting or rename leg.
	if current.LifecyclePhase == PhaseExpired && previous.LifecyclePhase != PhaseExpired &&
		previous.LifecyclePhase != PhaseUnknown {
		return InstrumentEventDelisted, true
	}
	return "", false
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
