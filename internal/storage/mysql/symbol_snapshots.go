package mysql

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"crypto-ticket/internal/market"
)

const createSymbolSnapshotTable = `CREATE TABLE IF NOT EXISTS instrument_snapshot (
	exchange VARCHAR(16) NOT NULL,
	symbol VARCHAR(64) NOT NULL,
	market_type VARCHAR(16) NOT NULL,
	official_status VARCHAR(32) NOT NULL DEFAULT '',
	official_active TINYINT(1) NOT NULL DEFAULT 0,
	presence VARCHAR(16) NOT NULL,
	subscribed TINYINT(1) NOT NULL DEFAULT 0,
	captured_at_ms BIGINT NOT NULL,
	snapshot_json JSON NOT NULL,
	updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	PRIMARY KEY (exchange,symbol),
	KEY idx_instrument_snapshot_scope (exchange,market_type,presence,subscribed)
)`

const createSymbolSnapshotSyncTable = `CREATE TABLE IF NOT EXISTS instrument_snapshot_sync (
	exchange VARCHAR(16) NOT NULL,
	market_type VARCHAR(16) NOT NULL,
	status VARCHAR(16) NOT NULL,
	last_attempt_ms BIGINT NOT NULL DEFAULT 0,
	last_success_ms BIGINT NOT NULL DEFAULT 0,
	symbol_count INT NOT NULL DEFAULT 0,
	last_error VARCHAR(512) NOT NULL DEFAULT '',
	PRIMARY KEY(exchange,market_type)
)`

func (s *Store) SaveSymbolSnapshot(ctx context.Context, batch market.SymbolSnapshotBatch) error {
	if batch.Exchange == "" || batch.MarketType == "" || batch.CapturedAtMS <= 0 || len(batch.Symbols) == 0 {
		return fmt.Errorf("invalid symbol snapshot batch")
	}
	encoded := make([][]byte, len(batch.Symbols))
	for i, item := range batch.Symbols {
		if item.Exchange != batch.Exchange || item.MarketType != batch.MarketType || item.CapturedAtMS != batch.CapturedAtMS || item.Symbol == "" {
			return fmt.Errorf("symbol snapshot outside batch scope")
		}
		var err error
		encoded[i], err = json.Marshal(item)
		if err != nil {
			return err
		}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Lock the scope before writing records, so two overlapping refreshes cannot
	// publish a mixture of old and new records under a successful watermark.
	_, err = tx.ExecContext(ctx, `INSERT INTO instrument_snapshot_sync (exchange,market_type,status) VALUES (?,?,'unavailable') ON DUPLICATE KEY UPDATE exchange=exchange`, batch.Exchange, batch.MarketType)
	if err != nil {
		return err
	}
	var previous int64
	if err := tx.QueryRowContext(ctx, `SELECT last_attempt_ms FROM instrument_snapshot_sync WHERE exchange=? AND market_type=? FOR UPDATE`, batch.Exchange, batch.MarketType).Scan(&previous); err != nil {
		return err
	}
	if previous > batch.CapturedAtMS {
		return fmt.Errorf("older symbol snapshot rejected")
	}
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO instrument_snapshot
		(exchange,symbol,market_type,official_status,official_active,presence,subscribed,captured_at_ms,snapshot_json)
		VALUES (?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE market_type=VALUES(market_type),official_status=VALUES(official_status),
		official_active=VALUES(official_active),presence=VALUES(presence),subscribed=VALUES(subscribed),captured_at_ms=VALUES(captured_at_ms),snapshot_json=VALUES(snapshot_json)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for i, item := range batch.Symbols {
		if _, err := stmt.ExecContext(ctx, item.Exchange, item.Symbol, item.MarketType, item.OfficialStatus, item.OfficialActive, item.Presence, item.Subscribed, item.CapturedAtMS, string(encoded[i])); err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(ctx, `UPDATE instrument_snapshot_sync SET status='ok',last_attempt_ms=?,last_success_ms=?,symbol_count=?,last_error='' WHERE exchange=? AND market_type=?`, batch.CapturedAtMS, batch.CapturedAtMS, len(batch.Symbols), batch.Exchange, batch.MarketType)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) RecordSymbolSnapshotFailure(ctx context.Context, state market.SymbolSnapshotSync) error {
	if chars := []rune(state.LastError); len(chars) > 512 {
		state.LastError = string(chars[:512])
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO instrument_snapshot_sync
		(exchange,market_type,status,last_attempt_ms,last_error) VALUES (?,?,'unavailable',?,?)
		ON DUPLICATE KEY UPDATE status=IF(last_attempt_ms<=VALUES(last_attempt_ms),'unavailable',status),
		last_error=IF(last_attempt_ms<=VALUES(last_attempt_ms),VALUES(last_error),last_error),last_attempt_ms=GREATEST(last_attempt_ms,VALUES(last_attempt_ms))`, strings.ToLower(state.Exchange), state.MarketType, state.LastAttemptMS, state.LastError)
	return err
}

func (s *Store) ListSymbolSnapshots(ctx context.Context, exchange string) ([]market.SymbolSnapshot, error) {
	query := `SELECT snapshot_json FROM instrument_snapshot`
	var args []any
	if exchange != "" {
		query += ` WHERE exchange=?`
		args = append(args, strings.ToLower(exchange))
	}
	query += ` ORDER BY exchange,symbol`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]market.SymbolSnapshot, 0)
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		var item market.SymbolSnapshot
		if err := json.Unmarshal(data, &item); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *Store) ListSymbolSnapshotSyncs(ctx context.Context) ([]market.SymbolSnapshotSync, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT exchange,market_type,status,last_attempt_ms,last_success_ms,symbol_count,last_error FROM instrument_snapshot_sync ORDER BY exchange,market_type`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]market.SymbolSnapshotSync, 0)
	for rows.Next() {
		var item market.SymbolSnapshotSync
		if err := rows.Scan(&item.Exchange, &item.MarketType, &item.Status, &item.LastAttemptMS, &item.LastSuccessMS, &item.SymbolCount, &item.LastError); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}
