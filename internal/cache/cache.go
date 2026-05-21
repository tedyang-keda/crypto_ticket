package cache

import (
	"context"

	"crypto-ticket/internal/market"
)

type MarketCache interface {
	SetLatestTick(ctx context.Context, tick market.Tick) error
	GetLatestTick(ctx context.Context, exchange string, symbol string) (*market.Tick, error)
	SetLiveBar(ctx context.Context, bar market.Bar) error
	GetLiveBar(ctx context.Context, exchange string, symbol string, timeframe string) (*market.Bar, error)
	PutFinalBars(ctx context.Context, bars []market.Bar, maxKeep int) error
	RecentBars(ctx context.Context, query market.KlineQuery) ([]market.Bar, error)
}
