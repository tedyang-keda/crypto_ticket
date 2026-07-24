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
			source_market VARCHAR(48) NOT NULL DEFAULT '',
			symbol VARCHAR(64) NOT NULL,
			market_type VARCHAR(16) NOT NULL,
			instrument_type VARCHAR(32) NOT NULL DEFAULT '',
			asset_class VARCHAR(24) NOT NULL DEFAULT '',
			rule_type VARCHAR(24) NOT NULL DEFAULT '',
			lifecycle_phase VARCHAR(24) NOT NULL DEFAULT '',
			is_active TINYINT(1) NOT NULL DEFAULT 1,
			first_seen_at_ms BIGINT NOT NULL DEFAULT 0,
			last_seen_at_ms BIGINT NOT NULL DEFAULT 0,
			last_status VARCHAR(32) NOT NULL DEFAULT '',
			raw_json JSON NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			PRIMARY KEY (exchange, symbol),
			KEY idx_registry_active (exchange, is_active, last_seen_at_ms),
			KEY idx_registry_source_market (exchange, source_market, is_active),
			KEY idx_registry_asset_class (exchange, asset_class, rule_type, lifecycle_phase)
			)`,
		createBarHistoryTableStatement(time.Now().UTC()),
		`CREATE TABLE IF NOT EXISTS instrument_change_event (
				id BIGINT NOT NULL AUTO_INCREMENT,
				exchange VARCHAR(16) NOT NULL,
				source_market VARCHAR(48) NOT NULL DEFAULT '',
				symbol VARCHAR(64) NOT NULL,
				event_ts_ms BIGINT NOT NULL,
				event_type VARCHAR(32) NOT NULL,
				previous_hash CHAR(64) NOT NULL DEFAULT '',
				current_hash CHAR(64) NOT NULL DEFAULT '',
				previous_json JSON NULL,
				current_json JSON NULL,
				created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
				PRIMARY KEY (id),
				KEY idx_instrument_event_symbol (exchange, source_market, symbol, event_ts_ms),
				KEY idx_instrument_event_type (event_type, event_ts_ms)
			)`,
		`CREATE TABLE IF NOT EXISTS corporate_action_event (
				action_id VARCHAR(160) NOT NULL,
				exchange VARCHAR(16) NOT NULL,
				source_market VARCHAR(48) NOT NULL DEFAULT '',
				symbol VARCHAR(64) NOT NULL,
				event_type VARCHAR(32) NOT NULL DEFAULT '',
				state VARCHAR(32) NOT NULL,
				first_seen_ms BIGINT NOT NULL,
				last_event_ms BIGINT NOT NULL,
				resume_ms BIGINT NOT NULL DEFAULT 0,
				boundary_ms BIGINT NOT NULL DEFAULT 0,
				announced_ratio DECIMAL(28, 12) NOT NULL DEFAULT 0,
				attempts INT NOT NULL DEFAULT 0,
				last_error VARCHAR(512) NOT NULL DEFAULT '',
				raw_json JSON NULL,
				updated_at_ms BIGINT NOT NULL,
				created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
				updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
				PRIMARY KEY (action_id),
				KEY idx_corp_action_open (state, updated_at_ms),
				KEY idx_corp_action_symbol (exchange, source_market, symbol, first_seen_ms)
			)`,
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
	if err := s.ensureSymbolRegistryColumns(ctx); err != nil {
		return err
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

func (s *Store) ensureSymbolRegistryColumns(ctx context.Context) error {
	columns := []string{
		`ALTER TABLE symbol_registry ADD COLUMN source_market VARCHAR(48) NOT NULL DEFAULT '' AFTER exchange`,
		`ALTER TABLE symbol_registry ADD COLUMN instrument_type VARCHAR(32) NOT NULL DEFAULT '' AFTER market_type`,
		`ALTER TABLE symbol_registry ADD COLUMN asset_class VARCHAR(24) NOT NULL DEFAULT '' AFTER instrument_type`,
		`ALTER TABLE symbol_registry ADD COLUMN rule_type VARCHAR(24) NOT NULL DEFAULT '' AFTER asset_class`,
		`ALTER TABLE symbol_registry ADD COLUMN lifecycle_phase VARCHAR(24) NOT NULL DEFAULT '' AFTER rule_type`,
	}
	for _, statement := range columns {
		if _, err := s.db.ExecContext(ctx, statement); err != nil && !isDuplicateColumnError(err) {
			return err
		}
	}
	return nil
}

func (s *Store) ensureBarHistoryColumns(ctx context.Context) error {
	columns := []string{
		`ALTER TABLE bar_history ADD COLUMN source_market VARCHAR(48) NOT NULL DEFAULT '' AFTER exchange`,
		`ALTER TABLE bar_history ADD COLUMN instrument_type VARCHAR(32) NOT NULL DEFAULT '' AFTER symbol`,
		`ALTER TABLE bar_history ADD COLUMN asset_class VARCHAR(24) NOT NULL DEFAULT '' AFTER instrument_type`,
		`ALTER TABLE bar_history ADD COLUMN rule_type VARCHAR(24) NOT NULL DEFAULT '' AFTER asset_class`,
		`ALTER TABLE bar_history ADD COLUMN lifecycle_phase VARCHAR(24) NOT NULL DEFAULT '' AFTER rule_type`,
		`ALTER TABLE bar_history ADD COLUMN margin_type VARCHAR(16) NOT NULL DEFAULT '' AFTER symbol`,
		`ALTER TABLE bar_history ADD COLUMN volume_unit VARCHAR(16) NOT NULL DEFAULT '' AFTER volume`,
		`ALTER TABLE bar_history ADD COLUMN quote_unit VARCHAR(16) NOT NULL DEFAULT '' AFTER quote_volume`,
		`ALTER TABLE bar_history ADD COLUMN contract_volume DECIMAL(30, 12) NOT NULL DEFAULT 0 AFTER quote_unit`,
		`ALTER TABLE bar_history ADD COLUMN prev_close DECIMAL(28, 12) NOT NULL DEFAULT 0 AFTER trade_count`,
		`ALTER TABLE bar_history ADD COLUMN chg DECIMAL(18, 8) NOT NULL DEFAULT 0 AFTER prev_close`,
		`ALTER TABLE bar_history ADD COLUMN amp DECIMAL(18, 8) NOT NULL DEFAULT 0 AFTER chg`,
	}
	for _, statement := range columns {
		if _, err := s.db.ExecContext(ctx, statement); err != nil && !isDuplicateColumnError(err) {
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

	if err := upsertBarsTx(ctx, tx, bars); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) ReplaceBarsInRange(ctx context.Context, exchange string, symbol string, tf string, startMS int64, endMS int64, bars []market.Bar) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM bar_history
		WHERE exchange = ? AND symbol = ? AND timeframe = ? AND start_ms >= ? AND start_ms <= ?`,
		strings.ToLower(exchange), strings.ToUpper(symbol), tf, startMS, endMS); err != nil {
		return err
	}
	filtered := make([]market.Bar, 0, len(bars))
	for _, bar := range bars {
		if strings.EqualFold(bar.Exchange, exchange) && strings.EqualFold(bar.Symbol, symbol) && bar.Timeframe == tf && bar.StartMS >= startMS && bar.StartMS <= endMS {
			filtered = append(filtered, bar)
		}
	}
	if err := upsertBarsTx(ctx, tx, filtered); err != nil {
		return err
	}
	return tx.Commit()
}

func upsertBarsTx(ctx context.Context, tx *sql.Tx, bars []market.Bar) error {
	historySQL := `INSERT INTO bar_history
		(exchange, source_market, symbol, instrument_type, asset_class, rule_type, lifecycle_phase, margin_type,
		 timeframe, start_ms, end_ms, open_price, high_price, low_price, close_price,
		 volume, volume_unit, quote_volume, quote_unit, contract_volume, trade_count, prev_close, chg, amp, last_tick_ms, is_final)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
		 source_market=VALUES(source_market), instrument_type=VALUES(instrument_type), asset_class=VALUES(asset_class),
		 rule_type=VALUES(rule_type), lifecycle_phase=VALUES(lifecycle_phase),
		 margin_type=VALUES(margin_type), end_ms=VALUES(end_ms), open_price=VALUES(open_price), high_price=VALUES(high_price),
		 low_price=VALUES(low_price), close_price=VALUES(close_price), volume=VALUES(volume),
		 volume_unit=VALUES(volume_unit), quote_volume=VALUES(quote_volume), quote_unit=VALUES(quote_unit),
		 contract_volume=VALUES(contract_volume), trade_count=VALUES(trade_count),
		 prev_close=VALUES(prev_close), chg=VALUES(chg), amp=VALUES(amp), last_tick_ms=VALUES(last_tick_ms),
		 is_final=VALUES(is_final)`
	for _, bar := range bars {
		if _, err := tx.ExecContext(ctx, historySQL, barArgs(bar)...); err != nil {
			return err
		}
	}
	return nil
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
	if _, err := market.NormalizePriceMode(query.PriceMode); err != nil {
		return nil, err
	}
	limit := query.Limit
	if limit <= 0 {
		limit = 300
	}
	if limit > 1000 {
		limit = 1000
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+rawBarSelectColumns()+`
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
		bar, err := scanRawBar(rows)
		if err != nil {
			return nil, err
		}
		bars = append(bars, market.MarkBarAdjustmentStatus(bar, market.PriceModeRaw, market.AdjustmentStatusRaw))
	}
	for i, j := 0, len(bars)-1; i < j; i, j = i+1, j-1 {
		bars[i], bars[j] = bars[j], bars[i]
	}
	return bars, rows.Err()
}

func (s *Store) BarsInRange(ctx context.Context, exchange string, symbol string, tf string, startMS int64, endMS int64) ([]market.Bar, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+rawBarSelectColumns()+`
		FROM bar_history
		WHERE exchange = ? AND symbol = ? AND timeframe = ? AND start_ms >= ? AND start_ms <= ? AND is_final = 1
		ORDER BY start_ms ASC`, strings.ToLower(exchange), strings.ToUpper(symbol), tf, startMS, endMS)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var bars []market.Bar
	for rows.Next() {
		bar, err := scanRawBar(rows)
		if err != nil {
			return nil, err
		}
		bars = append(bars, market.DecorateBar(bar))
	}
	return bars, rows.Err()
}

func (s *Store) UpsertSymbols(ctx context.Context, symbols []market.SymbolInfo) error {
	if len(symbols) == 0 {
		return nil
	}
	stmt := `INSERT INTO symbol_registry
		(exchange, source_market, symbol, market_type, instrument_type, asset_class, rule_type, lifecycle_phase,
		 is_active, first_seen_at_ms, last_seen_at_ms, last_status, raw_json)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
		 source_market=VALUES(source_market), market_type=VALUES(market_type),
		 instrument_type=VALUES(instrument_type), asset_class=VALUES(asset_class),
		 rule_type=VALUES(rule_type), lifecycle_phase=VALUES(lifecycle_phase), is_active=VALUES(is_active),
		 first_seen_at_ms=IF(first_seen_at_ms = 0, VALUES(first_seen_at_ms), LEAST(first_seen_at_ms, VALUES(first_seen_at_ms))),
		 last_seen_at_ms=GREATEST(last_seen_at_ms, VALUES(last_seen_at_ms)),
		 last_status=VALUES(last_status), raw_json=VALUES(raw_json)`
	for _, symbol := range symbols {
		symbol.SourceMarket = firstNonEmpty(symbol.SourceMarket, market.SourceMarket(symbol.Exchange, symbol.MarketType))
		rawJSON := []byte(symbol.Raw)
		if len(rawJSON) == 0 {
			rawJSON, _ = json.Marshal(symbol)
		}
		if _, err := s.db.ExecContext(ctx, stmt,
			strings.ToLower(symbol.Exchange),
			symbol.SourceMarket,
			strings.ToUpper(symbol.Symbol),
			symbol.MarketType,
			symbol.InstrumentType,
			symbol.AssetClass,
			symbol.RuleType,
			symbol.LifecyclePhase,
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
		last_seen_at_ms, last_status, source_market, instrument_type, asset_class, rule_type, lifecycle_phase
		FROM symbol_registry %s ORDER BY is_active DESC, symbol`, filter)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var symbols []market.SymbolInfo
	for rows.Next() {
		var symbol market.SymbolInfo
		if err := rows.Scan(
			&symbol.Exchange, &symbol.Symbol, &symbol.MarketType, &symbol.IsActive, &symbol.FirstSeenAtMS,
			&symbol.LastSeenAtMS, &symbol.Status, &symbol.SourceMarket, &symbol.InstrumentType,
			&symbol.AssetClass, &symbol.RuleType, &symbol.LifecyclePhase,
		); err != nil {
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

func (s *Store) UpsertCorporateActionEvent(ctx context.Context, event market.CorporateActionEvent) error {
	raw := any(nil)
	if len(event.Raw) > 0 {
		raw = string(event.Raw)
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO corporate_action_event
		(action_id, exchange, source_market, symbol, event_type, state, first_seen_ms, last_event_ms,
		 resume_ms, boundary_ms, announced_ratio, attempts, last_error, raw_json, updated_at_ms)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
		 event_type=VALUES(event_type), state=VALUES(state), last_event_ms=VALUES(last_event_ms),
		 resume_ms=VALUES(resume_ms), boundary_ms=VALUES(boundary_ms), announced_ratio=VALUES(announced_ratio),
		 attempts=VALUES(attempts), last_error=VALUES(last_error), raw_json=VALUES(raw_json),
		 updated_at_ms=VALUES(updated_at_ms)`,
		event.ActionID,
		strings.ToLower(strings.TrimSpace(event.Exchange)),
		event.SourceMarket,
		strings.ToUpper(strings.TrimSpace(event.Symbol)),
		event.EventType,
		event.State,
		event.FirstSeenMS,
		event.LastEventMS,
		event.ResumeMS,
		event.BoundaryMS,
		event.AnnouncedRatio,
		event.Attempts,
		event.LastError,
		raw,
		event.UpdatedAtMS,
	)
	return err
}

func barArgs(bar market.Bar) []any {
	return []any{
		strings.ToLower(bar.Exchange),
		bar.SourceMarket,
		strings.ToUpper(bar.Symbol),
		bar.InstrumentType,
		bar.AssetClass,
		bar.RuleType,
		bar.LifecyclePhase,
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

func (s *Store) recentRawBars(ctx context.Context, query market.KlineQuery, limit int) ([]market.Bar, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+rawBarSelectColumns()+`
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
		bar, err := scanRawBar(rows)
		if err != nil {
			return nil, err
		}
		bars = append(bars, bar)
	}
	for i, j := 0, len(bars)-1; i < j; i, j = i+1, j-1 {
		bars[i], bars[j] = bars[j], bars[i]
	}
	return bars, rows.Err()
}

type rowScanner interface {
	Scan(dest ...any) error
}

func rawBarSelectColumns() string {
	return `exchange, source_market, symbol, instrument_type, asset_class, rule_type, lifecycle_phase, margin_type,
		timeframe, start_ms, end_ms, open_price, high_price, low_price, close_price,
		volume, volume_unit, quote_volume, quote_unit, contract_volume, trade_count, prev_close, chg, amp, last_tick_ms, is_final`
}

func scanRawBar(row rowScanner) (market.Bar, error) {
	var bar market.Bar
	var isFinal bool
	err := row.Scan(
		&bar.Exchange, &bar.SourceMarket, &bar.Symbol, &bar.InstrumentType, &bar.AssetClass,
		&bar.RuleType, &bar.LifecyclePhase, &bar.MarginType, &bar.Timeframe, &bar.StartMS, &bar.EndMS,
		&bar.OpenPrice, &bar.HighPrice, &bar.LowPrice, &bar.ClosePrice, &bar.Volume, &bar.VolumeUnit,
		&bar.QuoteVolume, &bar.QuoteUnit, &bar.ContractVolume, &bar.TradeCount, &bar.PrevClose,
		&bar.Chg, &bar.Amp, &bar.LastTickMS, &isFinal,
	)
	if err != nil {
		return bar, err
	}
	bar.IsFinal = isFinal
	bar.Source = "mysql"
	return market.DecorateBar(bar), nil
}

func nullableJSON(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

func isDuplicateColumnError(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "duplicate column")
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
