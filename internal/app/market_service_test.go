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

func newTestMarketService() *MarketService {
	return NewMarketService(
		cache.NewMemoryMarketCache(),
		storage.NewMemoryHistoricalStore(),
		realtime.NewHub(),
		[]string{"1m"},
		300,
	)
}
