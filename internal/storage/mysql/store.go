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

type BarSeries struct {
	Exchange string
	Symbol   string
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

func (s *Store) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

func (s *Store) DBStats() sql.DBStats {
	return s.db.Stats()
}

func (s *Store) ContinuousOneMinuteSeries(ctx context.Context, startMS int64, endMS int64, minimumHours int) ([]market.MarketSeries, error) {
	if minimumHours <= 0 {
		minimumHours = 70
	}
	rows, err := s.db.QueryContext(ctx, `SELECT exchange, symbol, market_type
		FROM symbol_registry
		WHERE is_active = 1
		ORDER BY exchange, symbol`)
	if err != nil {
		return nil, err
	}
	var candidates []market.MarketSeries
	for rows.Next() {
		var item market.MarketSeries
		if err := rows.Scan(&item.Exchange, &item.Symbol, &item.MarketType); err != nil {
			rows.Close()
			return nil, err
		}
		candidates = append(candidates, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	// The primary key starts with exchange, symbol and timeframe. Querying one
	// symbol at a time keeps the 72-hour audit on that index range instead of
	// building a disk-backed temporary table for the whole market.
	statement, err := s.db.PrepareContext(ctx, `SELECT COUNT(DISTINCT FLOOR(start_ms / 3600000))
		FROM bar_history
		WHERE exchange = ? AND symbol = ? AND timeframe = '1m' AND is_final = 1
			AND start_ms BETWEEN ? AND ?`)
	if err != nil {
		return nil, err
	}
	defer statement.Close()
	var out []market.MarketSeries
	for _, item := range candidates {
		var hours int
		if err := statement.QueryRowContext(ctx, item.Exchange, item.Symbol, startMS, endMS).Scan(&hours); err != nil {
			return nil, err
		}
		if hours >= minimumHours {
			out = append(out, item)
		}
	}
	return out, nil
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
		`CREATE TABLE IF NOT EXISTS corporate_action_job (
				id BIGINT NOT NULL AUTO_INCREMENT,
				exchange VARCHAR(16) NOT NULL,
				symbol VARCHAR(64) NOT NULL,
				market_type VARCHAR(32) NOT NULL DEFAULT '',
				effective_ms BIGINT NOT NULL,
				observed_ratio DECIMAL(28, 12) NOT NULL DEFAULT 0,
				factor DECIMAL(28, 12) NOT NULL DEFAULT 0,
				detector VARCHAR(32) NOT NULL DEFAULT '',
				status VARCHAR(32) NOT NULL DEFAULT 'pending',
				attempts INT NOT NULL DEFAULT 0,
				next_retry_ms BIGINT NOT NULL DEFAULT 0,
				last_error TEXT NULL,
				rows_written BIGINT NOT NULL DEFAULT 0,
				verification_status VARCHAR(32) NOT NULL DEFAULT '',
				verification_json JSON NULL,
				created_at_ms BIGINT NOT NULL DEFAULT 0,
				updated_at_ms BIGINT NOT NULL DEFAULT 0,
				completed_at_ms BIGINT NOT NULL DEFAULT 0,
				created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
				updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
				PRIMARY KEY (id),
				UNIQUE KEY uk_corporate_action_event (exchange, symbol, effective_ms),
				KEY idx_corporate_action_due (status, next_retry_ms, id)
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

func (s *Store) DeleteBarsInRange(ctx context.Context, exchange string, symbol string, timeframe string, startMS int64, endMS int64) (int64, error) {
	result, err := s.db.ExecContext(ctx, `DELETE FROM bar_history
		WHERE exchange = ? AND symbol = ? AND timeframe = ? AND start_ms >= ? AND start_ms <= ?`,
		strings.ToLower(exchange), strings.ToUpper(symbol), timeframe, startMS, endMS)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (s *Store) DeleteBarsBefore(ctx context.Context, timeframe string, cutoffMS int64, limit int) (int64, error) {
	if limit <= 0 {
		limit = 10_000
	}
	result, err := s.db.ExecContext(ctx, `DELETE FROM bar_history
		WHERE timeframe = ? AND start_ms < ?
		LIMIT ?`, timeframe, cutoffMS, limit)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (s *Store) BarSeriesForTimeframe(ctx context.Context, timeframe string) ([]BarSeries, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT exchange, symbol FROM bar_history
		WHERE timeframe = ?`, timeframe)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var series []BarSeries
	for rows.Next() {
		var item BarSeries
		if err := rows.Scan(&item.Exchange, &item.Symbol); err != nil {
			return nil, err
		}
		series = append(series, item)
	}
	return series, rows.Err()
}

func (s *Store) DeleteSeriesBarsBefore(ctx context.Context, series BarSeries, timeframe string, cutoffMS int64, limit int) (int64, error) {
	if limit <= 0 {
		limit = 10_000
	}
	result, err := s.db.ExecContext(ctx, `DELETE FROM bar_history
		WHERE exchange = ? AND symbol = ? AND timeframe = ? AND start_ms < ?
		LIMIT ?`, series.Exchange, series.Symbol, timeframe, cutoffMS, limit)
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

func (s *Store) CountSeriesBars(ctx context.Context, series BarSeries, timeframe string) (int64, error) {
	var count int64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM bar_history
		WHERE exchange = ? AND symbol = ? AND timeframe = ?`, series.Exchange, series.Symbol, timeframe).Scan(&count)
	return count, err
}

// SeriesBarCutoff returns the start_ms of the keepBars-th newest bar. Rows
// older than this value can be removed while retaining exactly keepBars rows.
func (s *Store) SeriesBarCutoff(ctx context.Context, series BarSeries, timeframe string, keepBars int) (int64, bool, error) {
	if keepBars <= 0 {
		return 0, false, nil
	}
	var cutoff int64
	err := s.db.QueryRowContext(ctx, `SELECT start_ms FROM bar_history
		WHERE exchange = ? AND symbol = ? AND timeframe = ?
		ORDER BY start_ms DESC LIMIT 1 OFFSET ?`, series.Exchange, series.Symbol, timeframe, keepBars-1).Scan(&cutoff)
	if err == sql.ErrNoRows {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return cutoff, true, nil
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

func (s *Store) LoadCorporateActionJob(ctx context.Context, exchange string, symbol string, effectiveMS int64) (*market.CorporateActionJob, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id, exchange, symbol, market_type, effective_ms, observed_ratio, factor,
		detector, status, attempts, next_retry_ms, COALESCE(last_error, ''), rows_written,
		verification_status, COALESCE(CAST(verification_json AS CHAR), ''), created_at_ms, updated_at_ms, completed_at_ms
		FROM corporate_action_job WHERE exchange = ? AND symbol = ? AND effective_ms = ?`,
		strings.ToLower(exchange), strings.ToUpper(symbol), effectiveMS)
	job, err := scanCorporateActionJob(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &job, nil
}

func (s *Store) InsertCorporateActionJob(ctx context.Context, job market.CorporateActionJob) error {
	nowMS := market.NowMS()
	if job.CreatedAtMS == 0 {
		job.CreatedAtMS = nowMS
	}
	if job.UpdatedAtMS == 0 {
		job.UpdatedAtMS = nowMS
	}
	_, err := s.db.ExecContext(ctx, `INSERT IGNORE INTO corporate_action_job
		(exchange, symbol, market_type, effective_ms, observed_ratio, factor, detector, status, attempts,
		 next_retry_ms, last_error, rows_written, verification_status, verification_json, created_at_ms, updated_at_ms, completed_at_ms)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		strings.ToLower(job.Exchange), strings.ToUpper(job.Symbol), job.MarketType, job.EffectiveMS,
		job.ObservedRatio, job.Factor, job.Detector, job.Status, job.Attempts, job.NextRetryMS,
		nullableString(job.LastError), job.RowsWritten, job.VerificationStatus, nullableJSON(job.VerificationJSON),
		job.CreatedAtMS, job.UpdatedAtMS, job.CompletedAtMS)
	return err
}

func (s *Store) ListDueCorporateActionJobs(ctx context.Context, nowMS int64, limit int) ([]market.CorporateActionJob, error) {
	if limit <= 0 {
		limit = 10
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id, exchange, symbol, market_type, effective_ms, observed_ratio, factor,
		detector, status, attempts, next_retry_ms, COALESCE(last_error, ''), rows_written,
		verification_status, COALESCE(CAST(verification_json AS CHAR), ''), created_at_ms, updated_at_ms, completed_at_ms
		FROM corporate_action_job
		WHERE ((status IN ('pending', 'retry') AND next_retry_ms <= ?) OR
		       (status = 'running' AND updated_at_ms <= ?))
		ORDER BY id ASC LIMIT ?`, nowMS, nowMS-int64((10*time.Minute)/time.Millisecond), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var jobs []market.CorporateActionJob
	for rows.Next() {
		job, err := scanCorporateActionJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func (s *Store) UpdateCorporateActionJob(ctx context.Context, job market.CorporateActionJob) error {
	job.UpdatedAtMS = market.NowMS()
	_, err := s.db.ExecContext(ctx, `UPDATE corporate_action_job SET
		market_type = ?, observed_ratio = ?, factor = ?, detector = ?, status = ?, attempts = ?,
		next_retry_ms = ?, last_error = ?, rows_written = ?, verification_status = ?, verification_json = ?,
		updated_at_ms = ?, completed_at_ms = ?
		WHERE exchange = ? AND symbol = ? AND effective_ms = ?`,
		job.MarketType, job.ObservedRatio, job.Factor, job.Detector, job.Status, job.Attempts,
		job.NextRetryMS, nullableString(job.LastError), job.RowsWritten, job.VerificationStatus,
		nullableJSON(job.VerificationJSON), job.UpdatedAtMS, job.CompletedAtMS,
		strings.ToLower(job.Exchange), strings.ToUpper(job.Symbol), job.EffectiveMS)
	return err
}

func (s *Store) ListCorporateActionFactors(ctx context.Context, exchange string, symbol string) ([]market.CorporateActionJob, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, exchange, symbol, market_type, effective_ms, observed_ratio, factor,
		detector, status, attempts, next_retry_ms, COALESCE(last_error, ''), rows_written,
		verification_status, COALESCE(CAST(verification_json AS CHAR), ''), created_at_ms, updated_at_ms, completed_at_ms
		FROM corporate_action_job
		WHERE exchange = ? AND symbol = ? AND factor > 0 AND status IN ('confirmed', 'running', 'retry', 'completed')
		ORDER BY effective_ms ASC`, strings.ToLower(exchange), strings.ToUpper(symbol))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var jobs []market.CorporateActionJob
	for rows.Next() {
		job, err := scanCorporateActionJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanCorporateActionJob(row rowScanner) (market.CorporateActionJob, error) {
	var job market.CorporateActionJob
	err := row.Scan(&job.ID, &job.Exchange, &job.Symbol, &job.MarketType, &job.EffectiveMS,
		&job.ObservedRatio, &job.Factor, &job.Detector, &job.Status, &job.Attempts, &job.NextRetryMS,
		&job.LastError, &job.RowsWritten, &job.VerificationStatus, &job.VerificationJSON,
		&job.CreatedAtMS, &job.UpdatedAtMS, &job.CompletedAtMS)
	return job, err
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

func nullableString(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}
