package timeframe

import (
	"errors"
	"testing"
)

func TestExchangeCapabilitiesDisableThreeDay(t *testing.T) {
	for _, exchange := range []string{"binance", "okx", " Binance ", "BINANCE", ""} {
		if err := ValidateForExchange(exchange, "3D"); !errors.Is(err, ErrDisabledTimeframe) {
			t.Fatalf("expected 3D disabled, exchange=%q err=%v", exchange, err)
		}
	}
	for _, exchange := range []string{"binance", "okx"} {
		for _, tf := range Order {
			if tf == "3D" {
				continue
			}
			if err := ValidateForExchange(exchange, tf); err != nil {
				t.Fatalf("unexpected disabled interval %s/%s: %v", exchange, tf, err)
			}
		}
	}
	if err := ValidateForExchange("okx", "3d"); err == nil {
		t.Fatal("invalid interval spelling accepted")
	}
}

func TestDisabledTimeframeKeepsHistoricalArithmeticAndIndependentLists(t *testing.T) {
	start := FloorStartMS(1_710_000_000_000, "3D")
	if NextStartMS(start, "3D") != start+3*DayMS || EndMS(start, "3D") != start+3*DayMS-1 {
		t.Fatal("historical 3D math was removed")
	}
	binance, okx := SupportedForExchange("binance"), SupportedForExchange("okx")
	if len(binance) != len(Order)-1 || len(okx) != len(Order)-1 {
		t.Fatalf("unexpected capabilities: Binance=%v OKX=%v", binance, okx)
	}
	for _, frames := range [][]string{binance, okx} {
		for _, tf := range frames {
			if tf == "3D" {
				t.Fatal("exchange still advertises 3D")
			}
		}
	}
	binance[0], okx[0] = "changed", "changed"
	if Order[0] != "1m" {
		t.Fatal("capability lists mutated shared Order")
	}
}
