package app

import (
	"context"
	"testing"

	"crypto-ticket/internal/cache"
	"crypto-ticket/internal/market"
	"crypto-ticket/internal/realtime"
	"crypto-ticket/internal/storage"
)

func TestPublishTickUpdatesLatestWithoutAggregating(t *testing.T) {
	ctx := context.Background()
	service := newTestMarketService()

	tick, err := service.PublishTick(ctx, market.Tick{
		Exchange: " Binance ",
		Symbol:   "btcusdt",
		TsMS:     1_710_000_000_123,
		Price:    100,
		Size:     2,
	})
	if err != nil {
		t.Fatalf("publish tick: %v", err)
	}
	if tick.Exchange != "binance" || tick.Symbol != "BTCUSDT" || tick.Source != "ws" || tick.EventType != "trade" {
		t.Fatalf("unexpected normalized tick: %+v", tick)
	}

	latest, err := service.LatestTick(ctx, "binance", "BTCUSDT")
	if err != nil {
		t.Fatalf("latest tick: %v", err)
	}
	if latest == nil || latest.Price != 100 {
		t.Fatalf("unexpected latest tick: %+v", latest)
	}

	bars, err := service.Klines(ctx, market.KlineQuery{
		Exchange:    "binance",
		Symbol:      "BTCUSDT",
		Timeframe:   "1m",
		Limit:       10,
		IncludeLive: true,
	})
	if err != nil {
		t.Fatalf("klines: %v", err)
	}
	if len(bars) != 0 {
		t.Fatalf("publish tick should not aggregate bars: %+v", bars)
	}
}

func TestAggregateTickDoesNotUpdateLatest(t *testing.T) {
	ctx := context.Background()
	service := newTestMarketService()

	if err := service.AggregateTick(ctx, market.Tick{
		Exchange: "binance",
		Symbol:   "BTCUSDT",
		TsMS:     1_710_000_000_123,
		Price:    101,
		Size:     3,
	}); err != nil {
		t.Fatalf("aggregate tick: %v", err)
	}

	latest, err := service.LatestTick(ctx, "binance", "BTCUSDT")
	if err != nil {
		t.Fatalf("latest tick: %v", err)
	}
	if latest != nil {
		t.Fatalf("aggregate tick should not update latest tick: %+v", latest)
	}

	bars, err := service.Klines(ctx, market.KlineQuery{
		Exchange:    "binance",
		Symbol:      "BTCUSDT",
		Timeframe:   "1m",
		Limit:       10,
		IncludeLive: true,
	})
	if err != nil {
		t.Fatalf("klines: %v", err)
	}
	if len(bars) != 1 || bars[0].ClosePrice != 101 {
		t.Fatalf("aggregate tick should update live bar: %+v", bars)
	}
}

func TestKlinesBuildsHigherTimeframeFromFinalOneMinuteAndLiveOneMinute(t *testing.T) {
	ctx := context.Background()
	service := newTestMarketServiceWithFrames([]string{"1m", "1H"})
	base := int64(1_710_000_000_000)

	if err := service.AggregateTick(ctx, market.Tick{
		Exchange: "binance",
		Symbol:   "BTCUSDT",
		TsMS:     base + 1_000,
		Price:    100,
		Size:     1,
	}); err != nil {
		t.Fatalf("aggregate first tick: %v", err)
	}
	if err := service.AggregateTick(ctx, market.Tick{
		Exchange: "binance",
		Symbol:   "BTCUSDT",
		TsMS:     base + 61_000,
		Price:    110,
		Size:     2,
	}); err != nil {
		t.Fatalf("aggregate second tick: %v", err)
	}

	bars, err := service.Klines(ctx, market.KlineQuery{
		Exchange:    "binance",
		Symbol:      "BTCUSDT",
		Timeframe:   "1H",
		Limit:       10,
		IncludeLive: true,
	})
	if err != nil {
		t.Fatalf("klines: %v", err)
	}
	if len(bars) != 1 {
		t.Fatalf("expected one partial 1H bar, got %+v", bars)
	}
	bar := bars[0]
	if bar.Timeframe != "1H" || bar.IsFinal {
		t.Fatalf("expected live 1H rollup, got %+v", bar)
	}
	if bar.OpenPrice != 100 || bar.ClosePrice != 110 || bar.Volume != 3 || bar.TradeCount != 2 {
		t.Fatalf("unexpected rollup values: %+v", bar)
	}
}

func newTestMarketService() *MarketService {
	return newTestMarketServiceWithFrames([]string{"1m"})
}

func newTestMarketServiceWithFrames(frames []string) *MarketService {
	return NewMarketService(
		cache.NewMemoryMarketCache(),
		storage.NewMemoryHistoricalStore(),
		realtime.NewHub(),
		frames,
		300,
	)
}
