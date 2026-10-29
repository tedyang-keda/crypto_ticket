package market

import (
	"math"
	"strings"
)

// OfficialTradeCountAvailable distinguishes Binance's valid zero-trade bars
// from feeds such as OKX, whose K-line responses do not carry a trade count.
func OfficialTradeCountAvailable(bar Bar) bool {
	return bar.TradeCount > 0 || strings.EqualFold(strings.TrimSpace(bar.Exchange), "binance")
}

// SameOfficialMinuteValues compares stored values with an official completed
// minute. Exchange event timestamps and derived display fields are not candle
// revisions; an unavailable official trade count also carries no comparison.
func SameOfficialMinuteValues(local, official Bar) bool {
	if !local.IsFinal || !official.IsFinal || local.EndMS != official.EndMS {
		return false
	}
	for _, pair := range [][2]float64{
		{local.OpenPrice, official.OpenPrice}, {local.HighPrice, official.HighPrice},
		{local.LowPrice, official.LowPrice}, {local.ClosePrice, official.ClosePrice},
		{local.Volume, official.Volume}, {local.QuoteVolume, official.QuoteVolume},
		{local.ContractVolume, official.ContractVolume},
	} {
		if math.Abs(pair[0]-pair[1]) > math.Max(1e-8, math.Abs(pair[1])*1e-8) {
			return false
		}
	}
	return !OfficialTradeCountAvailable(official) || local.TradeCount == official.TradeCount
}
