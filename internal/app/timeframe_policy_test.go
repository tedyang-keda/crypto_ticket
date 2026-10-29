package app

import (
	"context"
	"errors"
	"testing"

	"crypto-ticket/internal/market"
	"crypto-ticket/internal/realtime"
	"crypto-ticket/internal/storage"
	"crypto-ticket/internal/timeframe"
)

func TestDisabledThreeDayRequestsNeverReachStorage(t *testing.T) {
	ctx := context.Background()
	// A nil store makes any accidental read/write fail immediately.
	service := NewMarketService(nil, realtime.NewHub(), []string{"1m", "3D"}, 300)
	for _, exchange := range []string{"binance", "okx"} {
		t.Run(exchange, func(t *testing.T) {
			bar := market.Bar{Exchange: exchange, Symbol: "TEST", Timeframe: "3D", IsFinal: true}
			for name, run := range map[string]func() error{
				"ingest":        func() error { return service.IngestKline(ctx, bar) },
				"repair":        func() error { return service.RepairFinalBars(ctx, []market.Bar{bar}) },
				"source-repair": func() error { return service.RepairRollups(ctx, []market.Bar{bar}) },
				"query": func() error {
					_, err := service.Klines(ctx, market.KlineQuery{Exchange: exchange, Symbol: "TEST", Timeframe: "3D"})
					return err
				},
				"live": func() error {
					_, err := service.buildLiveRollup(ctx, "3D", bar, 0)
					return err
				},
			} {
				if err := run(); !errors.Is(err, timeframe.ErrDisabledTimeframe) {
					t.Fatalf("%s expected disabled error, got %v", name, err)
				}
			}
		})
	}
}

func TestNormalizeFramesDoesNotEnableRetiredIntervalOrItsDependencies(t *testing.T) {
	if frames := normalizeFrames([]string{"3D"}); len(frames) != 0 {
		t.Fatalf("retired-only selection enabled dependencies: %v", frames)
	}
	for _, tf := range normalizeFrames(nil) {
		if tf == "3D" {
			t.Fatal("default MarketService frames include retired 3D")
		}
	}
}

func TestThreeDayDisabledWithoutBreakingOtherDailyRollups(t *testing.T) {
	ctx := context.Background()
	for _, exchange := range []string{"binance", "okx"} {
		t.Run(exchange, func(t *testing.T) {
			store := storage.NewMemoryHistoricalStore()
			hub := realtime.NewHub()
			service := NewMarketService(store, hub, []string{"1m", "2D", "3D", "5D"}, 300)
			start := timeframe.FloorStartMS(1_710_000_000_000, "5D")
			old := market.Bar{Exchange: exchange, Symbol: "TEST", Timeframe: "3D", StartMS: start, EndMS: timeframe.EndMS(start, "3D"), OpenPrice: 17, HighPrice: 17, LowPrice: 17, ClosePrice: 17, IsFinal: true}
			if err := store.UpsertBars(ctx, []market.Bar{old}); err != nil {
				t.Fatal(err)
			}
			var days []market.Bar
			for index := int64(0); index < 10; index++ {
				day := start + index*timeframe.DayMS
				bar := market.Bar{Exchange: exchange, Symbol: "TEST", Timeframe: "1D", StartMS: day, EndMS: timeframe.EndMS(day, "1D"), OpenPrice: 100, HighPrice: 110, LowPrice: 90, ClosePrice: 105, Volume: 1, IsFinal: true}
				days = append(days, bar)
				if err := service.IngestKline(ctx, bar); err != nil {
					t.Fatal(err)
				}
			}
			for _, tf := range []string{"2D", "5D"} {
				bars, err := store.RecentBars(ctx, market.KlineQuery{Exchange: exchange, Symbol: "TEST", Timeframe: tf})
				if err != nil || len(bars) == 0 {
					t.Fatalf("unrelated %s rollup stopped: bars=%v err=%v", tf, bars, err)
				}
				if _, err := store.DeleteBarsInRange(ctx, exchange, "TEST", tf, start-10*timeframe.DayMS, start+10*timeframe.DayMS); err != nil {
					t.Fatal(err)
				}
			}
			if err := service.RepairRollups(ctx, days); err != nil {
				t.Fatal(err)
			}
			for _, tf := range []string{"2D", "5D"} {
				bars, err := store.RecentBars(ctx, market.KlineQuery{Exchange: exchange, Symbol: "TEST", Timeframe: tf})
				if err != nil || len(bars) == 0 {
					t.Fatalf("unrelated %s repair stopped: bars=%v err=%v", tf, bars, err)
				}
			}
			bars, err := store.RecentBars(ctx, market.KlineQuery{Exchange: exchange, Symbol: "TEST", Timeframe: "3D"})
			if err != nil || len(bars) != 1 || bars[0].ClosePrice != 17 {
				t.Fatalf("disabled history was changed or additional 3D was generated: %+v err=%v", bars, err)
			}
		})
	}
}

func TestDisabledThreeDayLiveSubscriptionNeverBuildsOrPublishes(t *testing.T) {
	ctx := context.Background()
	for _, exchange := range []string{"binance", "okx"} {
		t.Run(exchange, func(t *testing.T) {
			hub := realtime.NewHub()
			sub := hub.Subscribe()
			defer sub.Close()
			sub.Add(realtime.KlineChannel(exchange, "TEST", "3D"))
			service := NewMarketService(nil, hub, []string{"1m", "3D"}, 300)
			if err := service.publishLiveRollups(ctx, market.Bar{Exchange: exchange, Symbol: "TEST", Timeframe: "1m"}); err != nil {
				t.Fatal(err)
			}
			select {
			case event := <-sub.Events():
				t.Fatalf("disabled live update published: %+v", event)
			default:
			}
		})
	}
}
