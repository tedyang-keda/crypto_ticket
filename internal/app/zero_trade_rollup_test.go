package app

import (
	"context"
	"reflect"
	"testing"
	"time"

	"crypto-ticket/internal/market"
	"crypto-ticket/internal/realtime"
	"crypto-ticket/internal/storage"
	"crypto-ticket/internal/timeframe"
)

func TestMarketServiceZeroTradeRollupsUseTradedPricesForLiveAndFinalDay(t *testing.T) {
	for _, exchange := range []string{"binance", "okx"} {
		t.Run(exchange, func(t *testing.T) {
			ctx := context.Background()
			service := newTestMarketServiceWithFrames([]string{"1m", "1D"})
			minutes := zeroTradeRollupDay(exchange)
			for _, bar := range minutes[:len(minutes)-1] {
				if err := service.IngestKline(ctx, bar); err != nil {
					t.Fatalf("ingest final source minute: %v", err)
				}
			}

			sub := service.hub.Subscribe()
			defer sub.Close()
			sub.Add(realtime.KlineChannel(exchange, minutes[0].Symbol, "1D"))
			live := minutes[len(minutes)-1]
			live.IsFinal = false
			if err := service.IngestKline(ctx, live); err != nil {
				t.Fatalf("ingest live no-trade minute: %v", err)
			}
			event := nextTestEvent(t, sub)
			if event.Bar == nil || event.Timeframe != "1D" || event.Bar.IsFinal {
				t.Fatalf("expected live daily rollup event, got %+v", event)
			}
			assertZeroTradeDayPrices(t, *event.Bar)

			if err := service.IngestKline(ctx, minutes[len(minutes)-1]); err != nil {
				t.Fatalf("ingest final no-trade minute: %v", err)
			}
			assertZeroTradeRollupChain(t, service, minutes)
		})
	}
}

func TestRepairRollupsRebuildsPollutedZeroTradeChainFromVerifiedMinutes(t *testing.T) {
	for _, exchange := range []string{"binance", "okx"} {
		t.Run(exchange, func(t *testing.T) {
			ctx := context.Background()
			service := newTestMarketServiceWithFrames([]string{"1m", "1D"})
			minutes := zeroTradeRollupDay(exchange)
			first := minutes[0]
			if err := service.store.UpsertBars(ctx, minutes); err != nil {
				t.Fatalf("seed retained official minutes: %v", err)
			}
			if err := service.RepairRollups(ctx, minutes); err != nil {
				t.Fatalf("seed complete aggregate chain: %v", err)
			}

			// Represent aggregate OHLC saved by the old algorithm, which included
			// the first three zero-trade minutes' carry-forward price of 50.26.
			for _, tf := range []string{"15m", "30m", "1H", "1D"} {
				rows, err := service.store.BarsInRange(ctx, exchange, first.Symbol, tf, first.StartMS, first.StartMS)
				if err != nil || len(rows) != 1 {
					t.Fatalf("read %s seed: rows=%+v err=%v", tf, rows, err)
				}
				stale := rows[0]
				stale.OpenPrice, stale.LowPrice = 50.26, 50.26
				if err := service.store.UpsertBars(ctx, []market.Bar{stale}); err != nil {
					t.Fatalf("seed polluted %s: %v", tf, err)
				}
			}
			// Unchanged official values only receive verification marks. That
			// alone cannot retrospectively repair existing aggregate prices.
			if err := service.VerifyFinalBars(ctx, minutes, 1000); err != nil {
				t.Fatalf("verify unchanged retained minutes: %v", err)
			}
			verified, err := service.store.(storage.MinuteVerificationStore).LoadMinuteVerifications(ctx, exchange, first.Symbol, first.StartMS, minutes[len(minutes)-1].StartMS)
			if err != nil || len(verified) != len(minutes) {
				t.Fatalf("minute verification coverage incomplete: count=%d err=%v", len(verified), err)
			}
			for _, row := range verified {
				if row.VerifiedAtMS != 1000 {
					t.Fatalf("source minute was not verified before replay: %+v", row)
				}
			}
			staleDays, err := service.store.BarsInRange(ctx, exchange, first.Symbol, "1D", first.StartMS, first.StartMS)
			if err != nil || len(staleDays) != 1 || staleDays[0].OpenPrice != 50.26 {
				t.Fatalf("fixture must retain pre-existing polluted daily bar until replay: rows=%+v err=%v", staleDays, err)
			}
			before, err := service.store.BarsInRange(ctx, exchange, first.Symbol, "1m", first.StartMS, minutes[len(minutes)-1].StartMS)
			if err != nil {
				t.Fatal(err)
			}

			if err := service.RepairRollups(ctx, minutes); err != nil {
				t.Fatalf("replay verified minute chain: %v", err)
			}
			assertZeroTradeRollupChain(t, service, minutes)
			after, err := service.store.BarsInRange(ctx, exchange, first.Symbol, "1m", first.StartMS, minutes[len(minutes)-1].StartMS)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("aggregate replay changed verified minute sources: count=%d err=%v", len(after), err)
			}
		})
	}
}

func zeroTradeRollupDay(exchange string) []market.Bar {
	base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	minutes := make([]market.Bar, 24*60)
	for i := range minutes {
		carry := 50.48
		if i < 3 {
			carry = 50.26
		}
		start := base + int64(i)*timeframe.MinuteMS
		minutes[i] = market.Bar{
			Exchange: exchange, Symbol: "BACUSDT", MarginType: "umargin", Timeframe: "1m",
			StartMS: start, EndMS: timeframe.EndMS(start, "1m"), LastTickMS: timeframe.EndMS(start, "1m"),
			OpenPrice: carry, HighPrice: carry, LowPrice: carry, ClosePrice: carry, IsFinal: true,
		}
	}
	minutes[3].OpenPrice, minutes[3].HighPrice = 50.33, 50.88
	minutes[3].LowPrice, minutes[3].ClosePrice = 50.27, 50.48
	minutes[3].Volume, minutes[3].QuoteVolume = 1, 50.48
	if exchange == "binance" {
		minutes[3].TradeCount = 1
	}
	return minutes
}

func assertZeroTradeDayPrices(t *testing.T, bar market.Bar) {
	t.Helper()
	if bar.OpenPrice != 50.33 || bar.HighPrice != 50.88 || bar.LowPrice != 50.27 || bar.ClosePrice != 50.48 ||
		bar.Volume != 1 || bar.QuoteVolume != 50.48 {
		t.Fatalf("zero-trade carry prices contaminated aggregate OHLC or activity: %+v", bar)
	}
	expectedTrades := int64(0)
	if bar.Exchange == "binance" {
		expectedTrades = 1
	}
	if bar.TradeCount != expectedTrades {
		t.Fatalf("aggregate lost exchange trade-count semantics: %+v", bar)
	}
}

func assertZeroTradeRollupChain(t *testing.T, service *MarketService, minutes []market.Bar) {
	t.Helper()
	first, last := minutes[0], minutes[len(minutes)-1]
	for _, item := range []struct {
		frame string
		count int
	}{{"15m", 96}, {"30m", 48}, {"1H", 24}, {"1D", 1}} {
		bars, err := service.store.BarsInRange(context.Background(), first.Exchange, first.Symbol, item.frame, first.StartMS, last.StartMS)
		if err != nil || len(bars) != item.count {
			t.Fatalf("incomplete %s aggregate coverage: count=%d expected=%d err=%v", item.frame, len(bars), item.count, err)
		}
		assertZeroTradeDayPrices(t, bars[0])
		for i, bar := range bars {
			if !bar.IsFinal || bar.StartMS != first.StartMS+int64(i)*timeframe.DurationMS(item.frame) ||
				bar.EndMS != timeframe.EndMS(bar.StartMS, item.frame) {
				t.Fatalf("aggregate lost coverage/finality: %+v", bar)
			}
			if i > 0 && (bar.OpenPrice != 50.48 || bar.HighPrice != 50.48 || bar.LowPrice != 50.48 || bar.ClosePrice != 50.48 ||
				bar.Volume != 0 || bar.QuoteVolume != 0 || bar.TradeCount != 0) {
				t.Fatalf("wholly untraded bucket lost carry-forward prices or activity: %+v", bar)
			}
		}
	}
}
