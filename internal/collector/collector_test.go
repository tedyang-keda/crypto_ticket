package collector

import (
	"reflect"
	"testing"
)

func TestDiffSymbols(t *testing.T) {
	subscribe, unsubscribe := diffSymbols(
		[]string{"ADAUSDT", "BTCUSDT", "ETHUSDT"},
		[]string{"BTCUSDT", "ETHUSDT", "SOLUSDT"},
	)
	if !reflect.DeepEqual(subscribe, []string{"SOLUSDT"}) {
		t.Fatalf("unexpected subscribe diff: %+v", subscribe)
	}
	if !reflect.DeepEqual(unsubscribe, []string{"ADAUSDT"}) {
		t.Fatalf("unexpected unsubscribe diff: %+v", unsubscribe)
	}
}

func TestDiffSymbolsNoChanges(t *testing.T) {
	subscribe, unsubscribe := diffSymbols(
		[]string{"BTCUSDT", "ETHUSDT"},
		[]string{"BTCUSDT", "ETHUSDT"},
	)
	if len(subscribe) != 0 || len(unsubscribe) != 0 {
		t.Fatalf("expected no diff, subscribe=%+v unsubscribe=%+v", subscribe, unsubscribe)
	}
}
