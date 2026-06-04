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
		createBarHistoryTableStatement(time.Now().UTC()),
		`CREATE TABLE IF NOT EXISTS kline_guardian_state (
				exchange VARCHAR(16) NOT NULL,
				symbol VARCHAR(64) NOT NULL,
				timeframe VARCHAR(8) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
				last_final_start_ms BIGINT NOT NULL DEFAULT 0,
				last_final_recv_ms BIGINT NOT NULL DEFAULT 0,
				last_checked_start_ms BIGINT NOT NULL DEFAULT 0,
				last_checked_end_ms BIGINT NOT NULL DEFAULT 0,
				last_checked_at_ms BIGINT NOT NULL DEFAULT 0,
				last_gap_start_ms BIGINT NOT NULL DEFAULT 0,
				last_gap_end_ms BIGINT NOT NULL DEFAULT 0,
				status VARCHAR(32) NOT NULL DEFAULT '',
				updated_at_ms BIGINT NOT NULL DEFAULT 0,
				created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
				updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
				PRIMARY KEY (exchange, symbol, timeframe),
				KEY idx_guardian_state_status (exchange, status, updated_at)
			)`,
		`CREATE TABLE IF NOT EXISTS kline_guardian_event (
				id BIGINT NOT NULL AUTO_INCREMENT,
				exchange VARCHAR(16) NOT NULL,
				symbol VARCHAR(64) NOT NULL,
				timeframe VARCHAR(8) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
				start_ms BIGINT NOT NULL,
				end_ms BIGINT NOT NULL,
				event_type VARCHAR(32) NOT NULL,
				old_value_json JSON NULL,
				new_value_json JSON NULL,
				created_at_ms BIGINT NOT NULL,
				created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
				PRIMARY KEY (id),
				KEY idx_guardian_event_market_time (exchange, symbol, timeframe, start_ms),
				KEY idx_guardian_event_type (event_type, created_at)
			)`,
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	if err := s.ensureBarHistoryColumns(ctx); err != nil {
		return err
	}
	return nil
}

func createBarHistoryTableStatement(now time.Time) string {
	partitionStart := time.Date(now.Year(), 1, 1, 0, 0, 0, 0, time.UTC)
	return "CREATE TABLE IF NOT EXISTS bar_history (\n" +
		barHistoryColumnsDDL() +
		"\n)\n" +
		BuildTimeframePartitionClause(TimeframePartitionOptions{
			StartMonth: partitionStart,
			Months:     12,
		})
}

func (s *Store) ensureBarHistoryColumns(ctx context.Context) error {
	columns := []string{
		`ALTER TABLE bar_history ADD COLUMN margin_type VARCHAR(16) NOT NULL DEFAULT '' AFTER symbol`,
		`ALTER TABLE bar_history ADD COLUMN volume_unit VARCHAR(16) NOT NULL DEFAULT '' AFTER volume`,
		`ALTER TABLE bar_history ADD COLUMN quote_unit VARCHAR(16) NOT NULL DEFAULT '' AFTER quote_volume`,
		`ALTER TABLE bar_history ADD COLUMN contract_volume DECIMAL(30, 12) NOT NULL DEFAULT 0 AFTER quote_unit`,
		`ALTER TABLE bar_history ADD COLUMN prev_close DECIMAL(28, 12) NOT NULL DEFAULT 0 AFTER trade_count`,
		`ALTER TABLE bar_history ADD COLUMN chg DECIMAL(18, 8) NOT NULL DEFAULT 0 AFTER prev_close`,
		`ALTER TABLE bar_history ADD COLUMN amp DECIMAL(18, 8) NOT NULL DEFAULT 0 AFTER chg`,
	}
	for _, statement := range columns {
		if _, err := s.db.ExecContext(ctx, statement); err != nil && !strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
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
		(exchange, symbol, margin_type, timeframe, start_ms, end_ms, open_price, high_price, low_price, close_price,
		 volume, volume_unit, quote_volume, quote_unit, contract_volume, trade_count, prev_close, chg, amp, last_tick_ms, is_final)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
		 margin_type=VALUES(margin_type), end_ms=VALUES(end_ms), open_price=VALUES(open_price), high_price=VALUES(high_price),
		 low_price=VALUES(low_price), close_price=VALUES(close_price), volume=VALUES(volume),
		 volume_unit=VALUES(volume_unit), quote_volume=VALUES(quote_volume), quote_unit=VALUES(quote_unit),
		 contract_volume=VALUES(contract_volume), trade_count=VALUES(trade_count),
		 prev_close=VALUES(prev_close), chg=VALUES(chg), amp=VALUES(amp), last_tick_ms=VALUES(last_tick_ms),
		 is_final=VALUES(is_final)`
	for _, bar := range bars {
		args := barArgs(bar)
		if _, err := tx.ExecContext(ctx, historySQL, args...); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) ClearBars(ctx context.Context) (int64, error) {
	var deleted int64
	result, err := s.db.ExecContext(ctx, `TRUNCATE TABLE bar_history`)
	if err != nil {
		return deleted, err
	}
	count, err := result.RowsAffected()
	if err == nil {
		deleted += count
	}
	return deleted, nil
}

func (s *Store) DeleteBarsBefore(ctx context.Context, timeframe string, cutoffMS int64, limit int) (int64, error) {
	if limit <= 0 {
		limit = 10_000
	}
	result, err := s.db.ExecContext(ctx, `DELETE FROM bar_history
		WHERE timeframe = ? AND start_ms < ?
		ORDER BY start_ms ASC
		LIMIT ?`, timeframe, cutoffMS, limit)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (s *Store) CountBarsBefore(ctx context.Context, timeframe string, cutoffMS int64) (int64, error) {
	var count int64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM bar_history
		WHERE timeframe = ? AND start_ms < ?`, timeframe, cutoffMS).Scan(&count)
	return count, err
}

func (s *Store) RecentBars(ctx context.Context, query market.KlineQuery) ([]market.Bar, error) {
	limit := query.Limit
	if limit <= 0 {
		limit = 300
	}
	if limit > 1000 {
		limit = 1000
	}
	rows, err := s.db.QueryContext(ctx, `SELECT exchange, symbol, margin_type, timeframe, start_ms, end_ms,
		open_price, high_price, low_price, close_price, volume, volume_unit, quote_volume, quote_unit,
		contract_volume, trade_count, prev_close, chg, amp, last_tick_ms, is_final
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
			&bar.Exchange, &bar.Symbol, &bar.MarginType, &bar.Timeframe, &bar.StartMS, &bar.EndMS,
			&bar.OpenPrice, &bar.HighPrice, &bar.LowPrice, &bar.ClosePrice, &bar.Volume, &bar.VolumeUnit,
			&bar.QuoteVolume, &bar.QuoteUnit, &bar.ContractVolume, &bar.TradeCount, &bar.PrevClose,
			&bar.Chg, &bar.Amp, &bar.LastTickMS, &isFinal,
		); err != nil {
			return nil, err
		}
		bar.IsFinal = isFinal
		bar.Source = "mysql"
		bars = append(bars, market.DecorateBar(bar))
	}
	for i, j := 0, len(bars)-1; i < j; i, j = i+1, j-1 {
		bars[i], bars[j] = bars[j], bars[i]
	}
	return bars, rows.Err()
}

func (s *Store) BarsInRange(ctx context.Context, exchange string, symbol string, tf string, startMS int64, endMS int64) ([]market.Bar, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT exchange, symbol, margin_type, timeframe, start_ms, end_ms,
		open_price, high_price, low_price, close_price, volume, volume_unit, quote_volume, quote_unit,
		contract_volume, trade_count, prev_close, chg, amp, last_tick_ms, is_final
		FROM bar_history
		WHERE exchange = ? AND symbol = ? AND timeframe = ? AND start_ms >= ? AND start_ms <= ? AND is_final = 1
		ORDER BY start_ms ASC`, strings.ToLower(exchange), strings.ToUpper(symbol), tf, startMS, endMS)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var bars []market.Bar
	for rows.Next() {
		var bar market.Bar
		var isFinal bool
		if err := rows.Scan(
			&bar.Exchange, &bar.Symbol, &bar.MarginType, &bar.Timeframe, &bar.StartMS, &bar.EndMS,
			&bar.OpenPrice, &bar.HighPrice, &bar.LowPrice, &bar.ClosePrice, &bar.Volume, &bar.VolumeUnit,
			&bar.QuoteVolume, &bar.QuoteUnit, &bar.ContractVolume, &bar.TradeCount, &bar.PrevClose,
			&bar.Chg, &bar.Amp, &bar.LastTickMS, &isFinal,
		); err != nil {
			return nil, err
		}
		bar.IsFinal = isFinal
		bar.Source = "mysql"
		bars = append(bars, market.DecorateBar(bar))
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

func (s *Store) LoadKlineGuardianState(ctx context.Context, exchange string, symbol string, tf string) (*market.KlineGuardianState, error) {
	var state market.KlineGuardianState
	err := s.db.QueryRowContext(ctx, `SELECT exchange, symbol, timeframe, last_final_start_ms, last_final_recv_ms,
		last_checked_start_ms, last_checked_end_ms, last_checked_at_ms, last_gap_start_ms, last_gap_end_ms, status, updated_at_ms
		FROM kline_guardian_state WHERE exchange = ? AND symbol = ? AND timeframe = ?`,
		strings.ToLower(exchange), strings.ToUpper(symbol), tf,
	).Scan(
		&state.Exchange, &state.Symbol, &state.Timeframe, &state.LastFinalStartMS, &state.LastFinalRecvMS,
		&state.LastCheckedStartMS, &state.LastCheckedEndMS, &state.LastCheckedAtMS, &state.LastGapStartMS,
		&state.LastGapEndMS, &state.Status, &state.UpdatedAtMS,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &state, nil
}

func (s *Store) UpsertKlineGuardianState(ctx context.Context, state market.KlineGuardianState) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO kline_guardian_state
		(exchange, symbol, timeframe, last_final_start_ms, last_final_recv_ms, last_checked_start_ms,
		 last_checked_end_ms, last_checked_at_ms, last_gap_start_ms, last_gap_end_ms, status, updated_at_ms)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
		 last_final_start_ms=GREATEST(last_final_start_ms, VALUES(last_final_start_ms)),
		 last_final_recv_ms=VALUES(last_final_recv_ms),
		 last_checked_start_ms=VALUES(last_checked_start_ms),
		 last_checked_end_ms=VALUES(last_checked_end_ms),
		 last_checked_at_ms=VALUES(last_checked_at_ms),
		 last_gap_start_ms=VALUES(last_gap_start_ms),
		 last_gap_end_ms=VALUES(last_gap_end_ms),
		 status=VALUES(status),
		 updated_at_ms=VALUES(updated_at_ms)`,
		strings.ToLower(state.Exchange),
		strings.ToUpper(state.Symbol),
		state.Timeframe,
		state.LastFinalStartMS,
		state.LastFinalRecvMS,
		state.LastCheckedStartMS,
		state.LastCheckedEndMS,
		state.LastCheckedAtMS,
		state.LastGapStartMS,
		state.LastGapEndMS,
		state.Status,
		state.UpdatedAtMS,
	)
	return err
}

func (s *Store) InsertKlineGuardianEvents(ctx context.Context, events []market.KlineGuardianEvent) error {
	if len(events) == 0 {
		return nil
	}
	stmt := `INSERT INTO kline_guardian_event
		(exchange, symbol, timeframe, start_ms, end_ms, event_type, old_value_json, new_value_json, created_at_ms)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
	for _, event := range events {
		createdAtMS := event.CreatedAtMS
		if createdAtMS == 0 {
			createdAtMS = market.NowMS()
		}
		if _, err := s.db.ExecContext(ctx, stmt,
			strings.ToLower(event.Exchange),
			strings.ToUpper(event.Symbol),
			event.Timeframe,
			event.StartMS,
			event.EndMS,
			event.EventType,
			nullableJSON(event.OldValueJSON),
			nullableJSON(event.NewValueJSON),
			createdAtMS,
		); err != nil {
			return err
		}
	}
	return nil
}

func barArgs(bar market.Bar) []any {
	return []any{
		strings.ToLower(bar.Exchange),
		strings.ToUpper(bar.Symbol),
		bar.MarginType,
		bar.Timeframe,
		bar.StartMS,
		bar.EndMS,
		bar.OpenPrice,
		bar.HighPrice,
		bar.LowPrice,
		bar.ClosePrice,
		bar.Volume,
		bar.VolumeUnit,
		bar.QuoteVolume,
		bar.QuoteUnit,
		bar.ContractVolume,
		bar.TradeCount,
		bar.PrevClose,
		bar.Chg,
		bar.Amp,
		bar.LastTickMS,
		bar.IsFinal,
	}
}

func nullableJSON(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}
