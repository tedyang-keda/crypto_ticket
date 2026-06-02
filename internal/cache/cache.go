package cache

import (
	"context"
	"time"

	"crypto-ticket/internal/market"
)

type MarketCache interface {
	SetLatestTick(ctx context.Context, tick market.Tick) error
	GetLatestTick(ctx context.Context, exchange string, symbol string) (*market.Tick, error)
	SetLiveBar(ctx context.Context, bar market.Bar) error
	GetLiveBar(ctx context.Context, exchange string, symbol string, timeframe string) (*market.Bar, error)
	PutFinalBars(ctx context.Context, bars []market.Bar, maxKeep int) error
	RecentBars(ctx context.Context, query market.KlineQuery) ([]market.Bar, error)
	MarkTickSeen(ctx context.Context, tick market.Tick, windowStartMS int64, ttl time.Duration) (bool, error)
	GetRollingBar(ctx context.Context, exchange string, symbol string, startMS int64) (*market.Bar, error)
	PutRollingBar(ctx context.Context, bar market.Bar, actionAtMS int64, ttl time.Duration) error
	DeleteRollingBar(ctx context.Context, exchange string, symbol string, startMS int64) error
	DueRollingBars(ctx context.Context, cutoffMS int64, limit int) ([]market.Bar, error)
	SetLive1mBar(ctx context.Context, bar market.Bar) error
	GetLive1mBar(ctx context.Context, exchange string, symbol string) (*market.Bar, error)
}
