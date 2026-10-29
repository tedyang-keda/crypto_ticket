package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"crypto-ticket/internal/timeaudit"
	"crypto-ticket/internal/timeframe"
)

// TimeAuditReader has one connection and exposes only SELECT queries. It never
// initializes schema and never holds a transaction across an entire audit.
type TimeAuditReader struct{ db *sql.DB }

func NewTimeAuditReader(dsn string) (*TimeAuditReader, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return &TimeAuditReader{db: db}, nil
}
func (r *TimeAuditReader) Close() error                   { return r.db.Close() }
func (r *TimeAuditReader) Ping(ctx context.Context) error { return r.db.PingContext(ctx) }

// Registry uses actual optional schema columns where available. first_seen is
// observational metadata, never an authoritative listing-date boundary.
func (r *TimeAuditReader) Registry(ctx context.Context, exchange string) ([]timeaudit.Metadata, error) {
	columns := map[string]bool{}
	rows, err := r.db.QueryContext(ctx, `SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='symbol_registry'`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			rows.Close()
			return nil, err
		}
		columns[name] = true
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	optional := []string{"source_market", "instrument_type", "asset_class", "rule_type", "lifecycle_phase"}
	selects := []string{"exchange", "symbol", "market_type", "is_active", "first_seen_at_ms", "last_seen_at_ms", "last_status"}
	for _, name := range optional {
		if columns[name] {
			selects = append(selects, name)
		} else {
			selects = append(selects, "''")
		}
	}
	query := "SELECT " + strings.Join(selects, ",") + " FROM symbol_registry WHERE exchange=? ORDER BY symbol"
	rows, err = r.db.QueryContext(ctx, query, exchange)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []timeaudit.Metadata{}
	for rows.Next() {
		var m timeaudit.Metadata
		m.InRegistry = true
		if err = rows.Scan(&m.Exchange, &m.Symbol, &m.MarketType, &m.Active, &m.FirstSeenMS, &m.LastSeenMS, &m.LastStatus, &m.SourceMarket, &m.InstrumentType, &m.AssetClass, &m.RuleType, &m.LifecyclePhase); err != nil {
			return nil, err
		}
		result = append(result, m)
	}
	return result, rows.Err()
}

func timeAuditSQL(symbols []string) string {
	q := `SELECT /*+ MAX_EXECUTION_TIME(600000) */ symbol,start_ms,end_ms FROM bar_history FORCE INDEX (PRIMARY) WHERE exchange=? AND timeframe=? AND is_final=1 AND start_ms<?`
	if len(symbols) > 0 {
		q += " AND symbol IN (" + strings.TrimSuffix(strings.Repeat("?,", len(symbols)), ",") + ")"
	}
	return q + " ORDER BY symbol,start_ms"
}

// StreamTimes reads the partition-pruned PRIMARY order without SQL aggregation,
// window functions, temporary tables, or OHLCV columns. Callback back-pressure
// keeps application memory bounded independently of the number of history rows.
func (r *TimeAuditReader) StreamTimes(ctx context.Context, exchange, tf string, latest int64, symbols []string, consume func(timeaudit.Row) error) error {
	if exchange != "binance" && exchange != "okx" {
		return fmt.Errorf("unsupported audit exchange")
	}
	if _, err := timeframe.Normalize(tf); err != nil {
		return err
	}
	if consume == nil {
		return fmt.Errorf("nil timestamp consumer")
	}
	args := []any{exchange, tf, timeframe.NextStartMS(latest, tf)}
	for _, s := range symbols {
		args = append(args, s)
	}
	rows, err := r.db.QueryContext(ctx, timeAuditSQL(symbols), args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var row timeaudit.Row
		if err = rows.Scan(&row.Symbol, &row.StartMS, &row.EndMS); err != nil {
			return err
		}
		if err = consume(row); err != nil {
			return err
		}
	}
	return rows.Err()
}
