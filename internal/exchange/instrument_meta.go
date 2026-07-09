package exchange

import (
	"encoding/json"
	"strings"

	"crypto-ticket/internal/market"
)

func rawJSON(value any) json.RawMessage {
	body, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	return body
}

func binanceDefaultClassification(marketType string) market.InstrumentClassification {
	return market.InstrumentClassification{
		SourceMarket:   market.SourceMarket("binance", marketType),
		InstrumentType: strings.ToUpper(strings.TrimSpace(marketType)),
		AssetClass:     market.AssetClassCrypto,
		RuleType:       market.RuleNormal,
		LifecyclePhase: market.PhaseUnknown,
	}
}

func okxDefaultClassification(instType string) market.InstrumentClassification {
	instType = strings.ToUpper(strings.TrimSpace(instType))
	return market.InstrumentClassification{
		SourceMarket:   market.SourceMarket("okx", instType),
		InstrumentType: instType,
		AssetClass:     market.AssetClassUnknown,
		RuleType:       market.RuleNormal,
		LifecyclePhase: market.PhaseUnknown,
	}
}
