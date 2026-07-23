package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
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
		createAdjustedBarHistoryTableStatement(time.Now().UTC()),
		`CREATE TABLE IF NOT EXISTS adjustment_factor (
				provider VARCHAR(64) NOT NULL,
				provider_version VARCHAR(64) NOT NULL,
				exchange VARCHAR(16) NOT NULL,
				source_market VARCHAR(48) NOT NULL DEFAULT '',
				symbol VARCHAR(64) NOT NULL,
				adj_mode VARCHAR(24) NOT NULL,
				effective_from_ms BIGINT NOT NULL,
				effective_to_ms BIGINT NOT NULL DEFAULT 0,
				price_multiplier DECIMAL(28, 12) NOT NULL DEFAULT 1,
				volume_multiplier DECIMAL(28, 12) NOT NULL DEFAULT 1,
				event_type VARCHAR(32) NOT NULL DEFAULT '',
				raw_json JSON NULL,
				created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
				updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
				PRIMARY KEY (provider, provider_version, exchange, source_market, symbol, adj_mode, effective_from_ms, effective_to_ms),
				KEY idx_adjustment_lookup (exchange, source_market, symbol, adj_mode, effective_from_ms, effective_to_ms),
				KEY idx_adjustment_event (event_type, updated_at)
			)`,
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

func createAdjustedBarHistoryTableStatement(now time.Time) string {
	partitionStart := time.Date(now.Year(), 1, 1, 0, 0, 0, 0, time.UTC)
	return "CREATE TABLE IF NOT EXISTS bar_history_adjusted (\n" +
		adjustedBarHistoryColumnsDDL() +
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
	priceMode, err := market.NormalizePriceMode(query.PriceMode)
	if err != nil {
		return nil, err
	}
	if priceMode != market.PriceModeRaw {
		return s.recentAdjustedBars(ctx, query, priceMode)
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
		bars = append(bars, market.MarkBarAdjustmentStatus(bar, priceMode, market.AdjustmentStatusRaw))
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

func (s *Store) AdjustmentFactorAt(ctx context.Context, exchange string, sourceMarket string, symbol string, priceMode string, tsMS int64) (*market.AdjustmentFactor, error) {
	priceMode, err := market.NormalizePriceMode(priceMode)
	if err != nil {
		return nil, err
	}
	if priceMode == market.PriceModeRaw {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT provider, provider_version, exchange, source_market, symbol, adj_mode,
		effective_from_ms, effective_to_ms, price_multiplier, volume_multiplier, event_type, raw_json
		FROM adjustment_factor
		WHERE exchange = ? AND symbol = ? AND adj_mode = ?
		  AND (source_market = ? OR source_market = '' OR ? = '')
		  AND effective_from_ms <= ? AND (effective_to_ms = 0 OR effective_to_ms >= ?)
		ORDER BY CASE WHEN source_market = ? THEN 0 WHEN source_market = '' THEN 1 ELSE 2 END,
			effective_from_ms DESC
		LIMIT 1`, strings.ToLower(exchange), strings.ToUpper(symbol), priceMode,
		sourceMarket, sourceMarket, tsMS, tsMS, sourceMarket)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, rows.Err()
	}
	factor, err := scanAdjustmentFactor(rows)
	if err != nil {
		return nil, err
	}
	return &factor, rows.Err()
}

func (s *Store) UpsertAdjustmentFactors(ctx context.Context, factors []market.AdjustmentFactor) error {
	if len(factors) == 0 {
		return nil
	}
	stmt := `INSERT INTO adjustment_factor
		(provider, provider_version, exchange, source_market, symbol, adj_mode, effective_from_ms, effective_to_ms,
		 price_multiplier, volume_multiplier, event_type, raw_json)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
		 price_multiplier=VALUES(price_multiplier), volume_multiplier=VALUES(volume_multiplier),
		 event_type=VALUES(event_type), raw_json=VALUES(raw_json)`
	for _, factor := range factors {
		mode, err := market.NormalizePriceMode(factor.AdjMode)
		if err != nil {
			return err
		}
		raw := any(nil)
		if len(factor.Raw) > 0 {
			raw = string(factor.Raw)
		}
		if _, err := s.db.ExecContext(ctx, stmt,
			factor.Provider,
			factor.ProviderVersion,
			strings.ToLower(factor.Exchange),
			factor.SourceMarket,
			strings.ToUpper(factor.Symbol),
			mode,
			factor.EffectiveFromMS,
			factor.EffectiveToMS,
			nonZeroFloat(factor.PriceMultiplier, 1),
			nonZeroFloat(factor.VolumeMultiplier, 1),
			factor.EventType,
			raw,
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

func (s *Store) ListOpenCorporateActionEvents(ctx context.Context) ([]market.CorporateActionEvent, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT action_id, exchange, source_market, symbol, event_type, state,
		first_seen_ms, last_event_ms, resume_ms, boundary_ms, announced_ratio, attempts, last_error,
		raw_json, updated_at_ms
		FROM corporate_action_event
		WHERE state NOT IN (?, ?, ?)
		ORDER BY first_seen_ms ASC`, market.CorporateActionStateFactor, market.CorporateActionStateManualReview, market.CorporateActionStateNotRequired)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]market.CorporateActionEvent, 0)
	for rows.Next() {
		var event market.CorporateActionEvent
		var raw sql.NullString
		if err := rows.Scan(
			&event.ActionID, &event.Exchange, &event.SourceMarket, &event.Symbol, &event.EventType, &event.State,
			&event.FirstSeenMS, &event.LastEventMS, &event.ResumeMS, &event.BoundaryMS, &event.AnnouncedRatio,
			&event.Attempts, &event.LastError, &raw, &event.UpdatedAtMS,
		); err != nil {
			return nil, err
		}
		if raw.Valid {
			event.Raw = []byte(raw.String)
		}
		out = append(out, event)
	}
	return out, rows.Err()
}

func (s *Store) HasAdjustmentCoverage(ctx context.Context, exchange string, sourceMarket string, symbol string, startMS int64, endMS int64) (bool, error) {
	filter := `exchange = ? AND symbol = ? AND event_type = ? AND state = ?
		AND first_seen_ms <= ? AND last_event_ms >= ?`
	args := []any{strings.ToLower(exchange), strings.ToUpper(symbol), market.CorporateActionEventHistoricalCoverage,
		market.CorporateActionStateNotRequired, startMS, endMS}
	if strings.TrimSpace(sourceMarket) != "" {
		filter += " AND (source_market = ? OR source_market = '')"
		args = append(args, sourceMarket)
	}
	var found int
	err := s.db.QueryRowContext(ctx, `SELECT 1 FROM corporate_action_event WHERE `+filter+` LIMIT 1`, args...).Scan(&found)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil && found == 1, err
}

func (s *Store) ListAdjustmentFactors(ctx context.Context, exchange string, sourceMarket string, symbol string, priceMode string) ([]market.AdjustmentFactor, error) {
	priceMode, err := market.NormalizePriceMode(priceMode)
	if err != nil {
		return nil, err
	}
	if priceMode == market.PriceModeRaw {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT provider, provider_version, exchange, source_market, symbol, adj_mode,
		effective_from_ms, effective_to_ms, price_multiplier, volume_multiplier, event_type, raw_json
		FROM adjustment_factor
		WHERE exchange = ? AND symbol = ? AND adj_mode = ?
		  AND (source_market = ? OR source_market = '')
		ORDER BY effective_from_ms ASC`, strings.ToLower(exchange), strings.ToUpper(symbol), priceMode, sourceMarket)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]market.AdjustmentFactor, 0)
	for rows.Next() {
		factor, err := scanAdjustmentFactor(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, factor)
	}
	return out, rows.Err()
}

func (s *Store) ReplaceAdjustmentFactors(ctx context.Context, exchange string, sourceMarket string, symbol string, priceMode string, factors []market.AdjustmentFactor) error {
	mode, err := market.NormalizePriceMode(priceMode)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM adjustment_factor
		WHERE exchange = ? AND symbol = ? AND adj_mode = ? AND (source_market = ? OR source_market = '')`,
		strings.ToLower(exchange), strings.ToUpper(symbol), mode, sourceMarket); err != nil {
		return err
	}
	stmt := `INSERT INTO adjustment_factor
		(provider, provider_version, exchange, source_market, symbol, adj_mode, effective_from_ms, effective_to_ms,
		 price_multiplier, volume_multiplier, event_type, raw_json)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	for _, factor := range factors {
		factorMode, err := market.NormalizePriceMode(factor.AdjMode)
		if err != nil {
			return err
		}
		raw := any(nil)
		if len(factor.Raw) > 0 {
			raw = string(factor.Raw)
		}
		if _, err := tx.ExecContext(ctx, stmt,
			factor.Provider,
			factor.ProviderVersion,
			strings.ToLower(factor.Exchange),
			factor.SourceMarket,
			strings.ToUpper(factor.Symbol),
			factorMode,
			factor.EffectiveFromMS,
			factor.EffectiveToMS,
			nonZeroFloat(factor.PriceMultiplier, 1),
			nonZeroFloat(factor.VolumeMultiplier, 1),
			factor.EventType,
			raw,
		); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) UpsertAdjustedBars(ctx context.Context, bars []market.Bar) error {
	if len(bars) == 0 {
		return nil
	}
	stmt := `INSERT INTO bar_history_adjusted
		(exchange, source_market, symbol, adj_mode, instrument_type, asset_class, rule_type, lifecycle_phase, margin_type,
		 timeframe, start_ms, end_ms, open_price, high_price, low_price, close_price,
		 volume, volume_unit, quote_volume, quote_unit, contract_volume, trade_count, prev_close, chg, amp, last_tick_ms,
		 is_final, adjustment_status, adjustment_provider, adjustment_provider_version, adjustment_event_type,
		 price_multiplier, volume_multiplier, raw_open_price, raw_high_price, raw_low_price, raw_close_price, raw_volume, raw_quote_volume)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
		 instrument_type=VALUES(instrument_type), asset_class=VALUES(asset_class), rule_type=VALUES(rule_type),
		 lifecycle_phase=VALUES(lifecycle_phase), margin_type=VALUES(margin_type), end_ms=VALUES(end_ms),
		 open_price=VALUES(open_price), high_price=VALUES(high_price), low_price=VALUES(low_price), close_price=VALUES(close_price),
		 volume=VALUES(volume), volume_unit=VALUES(volume_unit), quote_volume=VALUES(quote_volume), quote_unit=VALUES(quote_unit),
		 contract_volume=VALUES(contract_volume), trade_count=VALUES(trade_count), prev_close=VALUES(prev_close),
		 chg=VALUES(chg), amp=VALUES(amp), last_tick_ms=VALUES(last_tick_ms), is_final=VALUES(is_final),
		 adjustment_status=VALUES(adjustment_status), adjustment_provider=VALUES(adjustment_provider),
		 adjustment_provider_version=VALUES(adjustment_provider_version), adjustment_event_type=VALUES(adjustment_event_type),
		 price_multiplier=VALUES(price_multiplier), volume_multiplier=VALUES(volume_multiplier),
		 raw_open_price=VALUES(raw_open_price), raw_high_price=VALUES(raw_high_price), raw_low_price=VALUES(raw_low_price),
		 raw_close_price=VALUES(raw_close_price), raw_volume=VALUES(raw_volume), raw_quote_volume=VALUES(raw_quote_volume)`
	for _, bar := range bars {
		mode, err := market.NormalizePriceMode(bar.PriceMode)
		if err != nil {
			return err
		}
		if mode == market.PriceModeRaw {
			continue
		}
		args := adjustedBarArgs(market.DecorateBar(bar), mode)
		if _, err := s.db.ExecContext(ctx, stmt, args...); err != nil {
			return err
		}
	}
	return nil
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

func adjustedBarArgs(bar market.Bar, mode string) []any {
	if bar.AdjustmentStatus == "" {
		bar.AdjustmentStatus = market.AdjustmentStatusAdjusted
	}
	if bar.PriceMultiplier == 0 {
		bar.PriceMultiplier = 1
	}
	if bar.VolumeMultiplier == 0 {
		bar.VolumeMultiplier = 1
	}
	return []any{
		strings.ToLower(bar.Exchange),
		bar.SourceMarket,
		strings.ToUpper(bar.Symbol),
		mode,
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
		bar.AdjustmentStatus,
		bar.AdjustmentProvider,
		bar.AdjustmentProviderVersion,
		bar.AdjustmentEventType,
		bar.PriceMultiplier,
		bar.VolumeMultiplier,
		bar.RawOpenPrice,
		bar.RawHighPrice,
		bar.RawLowPrice,
		bar.RawClosePrice,
		bar.RawVolume,
		bar.RawQuoteVolume,
	}
}

func (s *Store) recentAdjustedBars(ctx context.Context, query market.KlineQuery, priceMode string) ([]market.Bar, error) {
	limit := query.Limit
	if limit <= 0 {
		limit = 300
	}
	if limit > 1000 {
		limit = 1000
	}
	rawQuery := query
	rawQuery.PriceMode = market.PriceModeRaw
	rawBars, err := s.recentRawBars(ctx, rawQuery, limit)
	if err != nil {
		return nil, err
	}
	if len(rawBars) == 0 {
		return nil, nil
	}
	materialized, err := s.materializedAdjustedBarsInRange(ctx, query, priceMode, rawBars[0].StartMS, rawBars[len(rawBars)-1].StartMS)
	if err != nil {
		return nil, err
	}
	materializedByStart := make(map[int64]market.Bar, len(materialized))
	for _, bar := range materialized {
		materializedByStart[bar.StartMS] = bar
	}
	covered, err := s.HasAdjustmentCoverage(ctx, query.Exchange, query.SourceMarket, query.Symbol,
		market.BarAdjustmentTimestamp(rawBars[0]), market.BarAdjustmentTimestamp(rawBars[len(rawBars)-1]))
	if err != nil {
		return nil, err
	}
	for i := range rawBars {
		if bar, ok := materializedByStart[rawBars[i].StartMS]; ok {
			rawBars[i] = bar
			continue
		}
		factor, err := s.AdjustmentFactorAt(ctx, rawBars[i].Exchange, firstNonEmpty(query.SourceMarket, rawBars[i].SourceMarket), rawBars[i].Symbol, priceMode, market.BarAdjustmentTimestamp(rawBars[i]))
		if err != nil {
			return nil, err
		}
		if factor == nil {
			status := market.AdjustmentStatusMissing
			if covered {
				status = market.AdjustmentStatusNotRequired
			}
			rawBars[i] = market.MarkBarAdjustmentStatus(rawBars[i], priceMode, status)
			continue
		}
		rawBars[i] = market.ApplyFactorToBar(rawBars[i], *factor)
	}
	return recalculateDerived(rawBars), nil
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

func (s *Store) materializedAdjustedBarsInRange(ctx context.Context, query market.KlineQuery, priceMode string, startMS int64, endMS int64) ([]market.Bar, error) {
	filter := "exchange = ? AND symbol = ? AND adj_mode = ? AND timeframe = ? AND start_ms >= ? AND start_ms <= ?"
	args := []any{strings.ToLower(query.Exchange), strings.ToUpper(query.Symbol), priceMode, query.Timeframe, startMS, endMS}
	if strings.TrimSpace(query.SourceMarket) != "" {
		filter += " AND source_market = ?"
		args = append(args, query.SourceMarket)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+adjustedBarSelectColumns()+`
		FROM bar_history_adjusted
		WHERE `+filter+`
		ORDER BY start_ms ASC`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var bars []market.Bar
	for rows.Next() {
		bar, err := scanAdjustedBar(rows)
		if err != nil {
			return nil, err
		}
		bars = append(bars, market.DecorateBar(bar))
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

func adjustedBarSelectColumns() string {
	return `exchange, source_market, symbol, adj_mode, instrument_type, asset_class, rule_type, lifecycle_phase, margin_type,
		timeframe, start_ms, end_ms, open_price, high_price, low_price, close_price,
		volume, volume_unit, quote_volume, quote_unit, contract_volume, trade_count, prev_close, chg, amp, last_tick_ms, is_final,
		adjustment_status, adjustment_provider, adjustment_provider_version, adjustment_event_type,
		price_multiplier, volume_multiplier, raw_open_price, raw_high_price, raw_low_price, raw_close_price, raw_volume, raw_quote_volume`
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

func scanAdjustedBar(row rowScanner) (market.Bar, error) {
	var bar market.Bar
	var isFinal bool
	err := row.Scan(
		&bar.Exchange, &bar.SourceMarket, &bar.Symbol, &bar.PriceMode, &bar.InstrumentType, &bar.AssetClass,
		&bar.RuleType, &bar.LifecyclePhase, &bar.MarginType, &bar.Timeframe, &bar.StartMS, &bar.EndMS,
		&bar.OpenPrice, &bar.HighPrice, &bar.LowPrice, &bar.ClosePrice, &bar.Volume, &bar.VolumeUnit,
		&bar.QuoteVolume, &bar.QuoteUnit, &bar.ContractVolume, &bar.TradeCount, &bar.PrevClose,
		&bar.Chg, &bar.Amp, &bar.LastTickMS, &isFinal, &bar.AdjustmentStatus,
		&bar.AdjustmentProvider, &bar.AdjustmentProviderVersion, &bar.AdjustmentEventType,
		&bar.PriceMultiplier, &bar.VolumeMultiplier, &bar.RawOpenPrice, &bar.RawHighPrice,
		&bar.RawLowPrice, &bar.RawClosePrice, &bar.RawVolume, &bar.RawQuoteVolume,
	)
	if err != nil {
		return bar, err
	}
	bar.IsFinal = isFinal
	bar.Source = "mysql_adjusted"
	return market.DecorateBar(bar), nil
}

func scanAdjustmentFactor(row rowScanner) (market.AdjustmentFactor, error) {
	var factor market.AdjustmentFactor
	var raw sql.NullString
	err := row.Scan(
		&factor.Provider, &factor.ProviderVersion, &factor.Exchange, &factor.SourceMarket, &factor.Symbol,
		&factor.AdjMode, &factor.EffectiveFromMS, &factor.EffectiveToMS, &factor.PriceMultiplier,
		&factor.VolumeMultiplier, &factor.EventType, &raw,
	)
	if err != nil {
		return factor, err
	}
	if raw.Valid {
		factor.Raw = []byte(raw.String)
	}
	return factor, nil
}

func recalculateDerived(bars []market.Bar) []market.Bar {
	out := append([]market.Bar(nil), bars...)
	var previousClose float64
	for i := range out {
		out[i].PrevClose = previousClose
		if previousClose > 0 {
			out[i].Chg = roundPercent((out[i].ClosePrice - previousClose) / previousClose * 100)
		} else {
			out[i].Chg = 0
		}
		if out[i].LowPrice > 0 {
			out[i].Amp = roundPercent((out[i].HighPrice - out[i].LowPrice) / out[i].LowPrice * 100)
		} else {
			out[i].Amp = 0
		}
		previousClose = out[i].ClosePrice
		out[i] = market.DecorateBar(out[i])
	}
	return out
}

func roundPercent(value float64) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0
	}
	return math.Round(value*1_000_000) / 1_000_000
}

func nonZeroFloat(value float64, fallback float64) float64 {
	if value == 0 {
		return fallback
	}
	return value
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
