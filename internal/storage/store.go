package storage

import (
	"context"

	"crypto-ticket/internal/market"
)

type HistoricalStore interface {
	EnsureSchema(ctx context.Context) error
	UpsertBars(ctx context.Context, bars []market.Bar) error
	RecentBars(ctx context.Context, query market.KlineQuery) ([]market.Bar, error)
	BarsInRange(ctx context.Context, exchange string, symbol string, timeframe string, startMS int64, endMS int64) ([]market.Bar, error)
	UpsertSymbols(ctx context.Context, symbols []market.SymbolInfo) error
	ListSymbols(ctx context.Context, exchange string, activeOnly *bool) ([]market.SymbolInfo, error)
	AdjustmentFactorAt(ctx context.Context, exchange string, sourceMarket string, symbol string, priceMode string, tsMS int64) (*market.AdjustmentFactor, error)
	ListAdjustmentFactors(ctx context.Context, exchange string, sourceMarket string, symbol string, priceMode string) ([]market.AdjustmentFactor, error)
	UpsertAdjustmentFactors(ctx context.Context, factors []market.AdjustmentFactor) error
	ReplaceAdjustmentFactors(ctx context.Context, exchange string, sourceMarket string, symbol string, priceMode string, factors []market.AdjustmentFactor) error
	UpsertAdjustedBars(ctx context.Context, bars []market.Bar) error
	UpsertCorporateActionEvent(ctx context.Context, event market.CorporateActionEvent) error
	ListOpenCorporateActionEvents(ctx context.Context) ([]market.CorporateActionEvent, error)
	HasAdjustmentCoverage(ctx context.Context, exchange string, sourceMarket string, symbol string, startMS int64, endMS int64) (bool, error)
}
