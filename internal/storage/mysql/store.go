package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"crypto-ticket/internal/market"
)

type Store struct {
	db *sql.DB
}

func New(dsn string) (*Store, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(30 * time.Minute)
	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) EnsureSchema(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS symbol_registry (
			exchange VARCHAR(16) NOT NULL,
			symbol VARCHAR(64) NOT NULL,
			market_type VARCHAR(16) NOT NULL,
			is_active TINYINT(1) NOT NULL DEFAULT 1,
			first_seen_at_ms BIGINT NOT NULL DEFAULT 0,
			last_seen_at_ms BIGINT NOT NULL DEFAULT 0,
			last_status VARCHAR(32) NOT NULL DEFAULT '',
			raw_json JSON NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			PRIMARY KEY (exchange, symbol),
			KEY idx_registry_active (exchange, is_active, last_seen_at_ms)
		)`,
		`CREATE TABLE IF NOT EXISTS bar_checkpoint (
			exchange VARCHAR(16) NOT NULL,
			symbol VARCHAR(64) NOT NULL,
			timeframe VARCHAR(8) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
			start_ms BIGINT NOT NULL,
			end_ms BIGINT NOT NULL,
			open_price DECIMAL(28, 12) NOT NULL,
			high_price DECIMAL(28, 12) NOT NULL,
			low_price DECIMAL(28, 12) NOT NULL,
			close_price DECIMAL(28, 12) NOT NULL,
			volume DECIMAL(30, 12) NOT NULL DEFAULT 0,
			quote_volume DECIMAL(30, 12) NOT NULL DEFAULT 0,
			trade_count BIGINT NOT NULL DEFAULT 0,
			last_tick_ms BIGINT NOT NULL,
			is_final TINYINT(1) NOT NULL DEFAULT 0,
			raw_json JSON NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			PRIMARY KEY (exchange, symbol, timeframe),
			KEY idx_bar_checkpoint_time (exchange, timeframe, start_ms)
		)`,
		`CREATE TABLE IF NOT EXISTS bar_history (
			exchange VARCHAR(16) NOT NULL,
			symbol VARCHAR(64) NOT NULL,
			timeframe VARCHAR(8) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
			start_ms BIGINT NOT NULL,
			end_ms BIGINT NOT NULL,
			open_price DECIMAL(28, 12) NOT NULL,
			high_price DECIMAL(28, 12) NOT NULL,
			low_price DECIMAL(28, 12) NOT NULL,
			close_price DECIMAL(28, 12) NOT NULL,
			volume DECIMAL(30, 12) NOT NULL DEFAULT 0,
			quote_volume DECIMAL(30, 12) NOT NULL DEFAULT 0,
			trade_count BIGINT NOT NULL DEFAULT 0,
			last_tick_ms BIGINT NOT NULL,
			is_final TINYINT(1) NOT NULL DEFAULT 0,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			PRIMARY KEY (exchange, symbol, timeframe, start_ms),
			KEY idx_bar_lookup (exchange, symbol, timeframe, start_ms)
		)`,
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) UpsertBars(ctx context.Context, bars []market.Bar) error {
	if len(bars) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	historySQL := `INSERT INTO bar_history
		(exchange, symbol, timeframe, start_ms, end_ms, open_price, high_price, low_price, close_price,
		 volume, quote_volume, trade_count, last_tick_ms, is_final)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
		 end_ms=VALUES(end_ms), open_price=VALUES(open_price), high_price=VALUES(high_price),
		 low_price=VALUES(low_price), close_price=VALUES(close_price), volume=VALUES(volume),
		 quote_volume=VALUES(quote_volume), trade_count=VALUES(trade_count), last_tick_ms=VALUES(last_tick_ms),
		 is_final=VALUES(is_final)`
	checkpointSQL := `INSERT INTO bar_checkpoint
		(exchange, symbol, timeframe, start_ms, end_ms, open_price, high_price, low_price, close_price,
		 volume, quote_volume, trade_count, last_tick_ms, is_final, raw_json)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
		 start_ms=VALUES(start_ms), end_ms=VALUES(end_ms), open_price=VALUES(open_price),
		 high_price=VALUES(high_price), low_price=VALUES(low_price), close_price=VALUES(close_price),
		 volume=VALUES(volume), quote_volume=VALUES(quote_volume), trade_count=VALUES(trade_count),
		 last_tick_ms=VALUES(last_tick_ms), is_final=VALUES(is_final), raw_json=VALUES(raw_json)`

	for _, bar := range bars {
		args := barArgs(bar)
		if _, err := tx.ExecContext(ctx, historySQL, args...); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, checkpointSQL, checkpointArgs(bar)...); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) ClearBars(ctx context.Context) (int64, error) {
	var deleted int64
	for _, statement := range []string{`TRUNCATE TABLE bar_checkpoint`, `TRUNCATE TABLE bar_history`} {
		result, err := s.db.ExecContext(ctx, statement)
		if err != nil {
			return deleted, err
		}
		count, err := result.RowsAffected()
		if err == nil {
			deleted += count
		}
	}
	return deleted, nil
}

func (s *Store) RecentBars(ctx context.Context, query market.KlineQuery) ([]market.Bar, error) {
	limit := query.Limit
	if limit <= 0 {
		limit = 300
	}
	if limit > 1000 {
		limit = 1000
	}
	rows, err := s.db.QueryContext(ctx, `SELECT exchange, symbol, timeframe, start_ms, end_ms,
		open_price, high_price, low_price, close_price, volume, quote_volume, trade_count, last_tick_ms, is_final
		FROM bar_history
		WHERE exchange = ? AND symbol = ? AND timeframe = ?
		ORDER BY start_ms DESC
		LIMIT ?`, strings.ToLower(query.Exchange), strings.ToUpper(query.Symbol), query.Timeframe, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var bars []market.Bar
	for rows.Next() {
		var bar market.Bar
		var isFinal bool
		if err := rows.Scan(
			&bar.Exchange, &bar.Symbol, &bar.Timeframe, &bar.StartMS, &bar.EndMS,
			&bar.OpenPrice, &bar.HighPrice, &bar.LowPrice, &bar.ClosePrice, &bar.Volume,
			&bar.QuoteVolume, &bar.TradeCount, &bar.LastTickMS, &isFinal,
		); err != nil {
			return nil, err
		}
		bar.IsFinal = isFinal
		bar.Source = "mysql"
		bars = append(bars, bar)
	}
	for i, j := 0, len(bars)-1; i < j; i, j = i+1, j-1 {
		bars[i], bars[j] = bars[j], bars[i]
	}
	return bars, rows.Err()
}

func (s *Store) UpsertSymbols(ctx context.Context, symbols []market.SymbolInfo) error {
	if len(symbols) == 0 {
		return nil
	}
	stmt := `INSERT INTO symbol_registry
		(exchange, symbol, market_type, is_active, first_seen_at_ms, last_seen_at_ms, last_status, raw_json)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
		 market_type=VALUES(market_type), is_active=VALUES(is_active),
		 first_seen_at_ms=IF(first_seen_at_ms = 0, VALUES(first_seen_at_ms), LEAST(first_seen_at_ms, VALUES(first_seen_at_ms))),
		 last_seen_at_ms=GREATEST(last_seen_at_ms, VALUES(last_seen_at_ms)),
		 last_status=VALUES(last_status), raw_json=VALUES(raw_json)`
	for _, symbol := range symbols {
		rawJSON, _ := json.Marshal(symbol)
		if _, err := s.db.ExecContext(ctx, stmt,
			strings.ToLower(symbol.Exchange),
			strings.ToUpper(symbol.Symbol),
			symbol.MarketType,
			symbol.IsActive,
			symbol.FirstSeenAtMS,
			symbol.LastSeenAtMS,
			symbol.Status,
			string(rawJSON),
		); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) ListSymbols(ctx context.Context, exchange string, activeOnly *bool) ([]market.SymbolInfo, error) {
	filter := "WHERE exchange = ?"
	args := []any{strings.ToLower(exchange)}
	if activeOnly != nil {
		filter += " AND is_active = ?"
		args = append(args, *activeOnly)
	}
	query := fmt.Sprintf(`SELECT exchange, symbol, market_type, is_active, first_seen_at_ms,
		last_seen_at_ms, last_status FROM symbol_registry %s ORDER BY is_active DESC, symbol`, filter)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var symbols []market.SymbolInfo
	for rows.Next() {
		var symbol market.SymbolInfo
		if err := rows.Scan(&symbol.Exchange, &symbol.Symbol, &symbol.MarketType, &symbol.IsActive, &symbol.FirstSeenAtMS, &symbol.LastSeenAtMS, &symbol.Status); err != nil {
			return nil, err
		}
		symbols = append(symbols, symbol)
	}
	return symbols, rows.Err()
}

func barArgs(bar market.Bar) []any {
	return []any{
		strings.ToLower(bar.Exchange),
		strings.ToUpper(bar.Symbol),
		bar.Timeframe,
		bar.StartMS,
		bar.EndMS,
		bar.OpenPrice,
		bar.HighPrice,
		bar.LowPrice,
		bar.ClosePrice,
		bar.Volume,
		bar.QuoteVolume,
		bar.TradeCount,
		bar.LastTickMS,
		bar.IsFinal,
	}
}

func checkpointArgs(bar market.Bar) []any {
	args := barArgs(bar)
	rawJSON, _ := json.Marshal(bar)
	return append(args, string(rawJSON))
}
