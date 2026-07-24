package storage

import (
	"context"

	"crypto-ticket/internal/market"
)

type HistoricalStore interface {
	EnsureSchema(ctx context.Context) error
	UpsertBars(ctx context.Context, bars []market.Bar) error
	ReplaceBarsInRange(ctx context.Context, exchange string, symbol string, timeframe string, startMS int64, endMS int64, bars []market.Bar) error
	RecentBars(ctx context.Context, query market.KlineQuery) ([]market.Bar, error)
	BarsInRange(ctx context.Context, exchange string, symbol string, timeframe string, startMS int64, endMS int64) ([]market.Bar, error)
	UpsertSymbols(ctx context.Context, symbols []market.SymbolInfo) error
	ListSymbols(ctx context.Context, exchange string, activeOnly *bool) ([]market.SymbolInfo, error)
	UpsertCorporateActionEvent(ctx context.Context, event market.CorporateActionEvent) error
}
