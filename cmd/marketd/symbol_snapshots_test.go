package main

import (
	"crypto-ticket/internal/config"
	"testing"
)

func TestSymbolSnapshotSourcesIncludeDisabledCollectionMarkets(t *testing.T) {
	configs := []config.ExchangeConfig{{Name: "binance", MarketType: "um_futures", Enabled: true}, {Name: "binance", MarketType: "coin_futures", Enabled: false}, {Name: "okx", MarketType: "SWAP", Enabled: true}}
	sources := makeSymbolSnapshotSources(configs, true)
	if len(sources) != 3 || sources[0].Fetcher.MarketType() != "um_futures" || sources[1].SubscriptionEnabled || !sources[0].SubscriptionEnabled || !sources[2].SubscriptionEnabled {
		t.Fatalf("bad inventory scope: %+v", sources)
	}
	for _, source := range makeSymbolSnapshotSources(configs, false) {
		if source.SubscriptionEnabled {
			t.Fatal("collector off must not mark local subscription on")
		}
	}
}
