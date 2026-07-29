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
}

type CorporateActionStore interface {
	LoadCorporateActionJob(ctx context.Context, exchange string, symbol string, effectiveMS int64) (*market.CorporateActionJob, error)
	InsertCorporateActionJob(ctx context.Context, job market.CorporateActionJob) error
	ListDueCorporateActionJobs(ctx context.Context, nowMS int64, limit int) ([]market.CorporateActionJob, error)
	UpdateCorporateActionJob(ctx context.Context, job market.CorporateActionJob) error
	ListCorporateActionFactors(ctx context.Context, exchange string, symbol string) ([]market.CorporateActionJob, error)
}
