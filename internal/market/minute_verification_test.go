package market

import "testing"

func TestOfficialZeroTradeCountIsComparedForBinance(t *testing.T) {
	for _, exchange := range []string{"binance", "okx"} {
		t.Run(exchange, func(t *testing.T) {
			official := Bar{Exchange: exchange, EndMS: 59_999, IsFinal: true, OpenPrice: 100,
				HighPrice: 100, LowPrice: 100, ClosePrice: 100, TradeCount: 0}
			local := official
			local.TradeCount = 7
			if got, want := SameOfficialMinuteValues(local, official), exchange == "okx"; got != want {
				t.Fatalf("zero-count comparison: got equal=%t want %t", got, want)
			}
		})
	}
}
