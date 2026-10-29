package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"crypto-ticket/internal/market"
	"crypto-ticket/internal/storage"
)

var _ storage.MinuteAuditTaskStore = (*Store)(nil)

const createMinuteAuditTaskTable = `CREATE TABLE IF NOT EXISTS kline_minute_audit_task (
	id BIGINT NOT NULL AUTO_INCREMENT,
	exchange VARCHAR(16) NOT NULL,
	symbol VARCHAR(64) NOT NULL,
	market_type VARCHAR(32) NOT NULL DEFAULT '',
	kind VARCHAR(16) NOT NULL,
	start_ms BIGINT NOT NULL,
	end_ms BIGINT NOT NULL,
	expected_start_ms BIGINT NOT NULL DEFAULT 0,
	expect_data TINYINT(1) NOT NULL DEFAULT 0,
	status VARCHAR(16) NOT NULL DEFAULT 'pending',
	attempts INT NOT NULL DEFAULT 0,
	next_attempt_ms BIGINT NOT NULL DEFAULT 0,
	lease_until_ms BIGINT NOT NULL DEFAULT 0,
	lease_token VARCHAR(64) NOT NULL DEFAULT '',
	force_replay TINYINT(1) NOT NULL DEFAULT 0,
	last_error VARCHAR(1024) NOT NULL DEFAULT '',
	created_at_ms BIGINT NOT NULL,
	updated_at_ms BIGINT NOT NULL,
	PRIMARY KEY(id),
	UNIQUE KEY uk_minute_audit_range(exchange,symbol,kind,start_ms,end_ms),
	KEY idx_minute_audit_due(exchange,status,next_attempt_ms,id),
	KEY idx_minute_audit_kind_due(exchange,kind,status,next_attempt_ms,id),
	KEY idx_minute_audit_lease(exchange,status,lease_until_ms,id),
	KEY idx_minute_audit_gc(status,updated_at_ms,id)
)`

const minuteAuditTaskColumns = `id,exchange,symbol,market_type,kind,start_ms,end_ms,expected_start_ms,expect_data,
	status,attempts,next_attempt_ms,lease_until_ms,lease_token,force_replay,last_error,created_at_ms,updated_at_ms`

const createMinuteAuditCursorTable = `CREATE TABLE IF NOT EXISTS kline_minute_audit_cursor (
	exchange VARCHAR(16) NOT NULL,
	symbol VARCHAR(64) NOT NULL,
	kind VARCHAR(16) NOT NULL,
	through_ms BIGINT NOT NULL DEFAULT 0,
	updated_at_ms BIGINT NOT NULL,
	PRIMARY KEY(exchange,symbol,kind)
)`

func (s *Store) EnqueueMinuteAuditTask(ctx context.Context, task market.MinuteAuditTask) (int64, error) {
	task, err := storage.NormalizeMinuteAuditTask(task)
	if err != nil {
		return 0, err
	}
	// A repeated scheduler pass must never reset a lease or backoff. LAST_INSERT_ID
	// returns the existing identity without a read-before-insert race.
	result, err := s.db.ExecContext(ctx, `INSERT INTO kline_minute_audit_task
		(exchange,symbol,market_type,kind,start_ms,end_ms,expected_start_ms,expect_data,status,
		next_attempt_ms,force_replay,last_error,created_at_ms,updated_at_ms)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id)`,
		task.Exchange, task.Symbol, task.MarketType, task.Kind, task.StartMS, task.EndMS,
		task.ExpectedStartMS, task.ExpectData, task.Status, task.NextAttemptMS, task.ForceReplay,
		task.LastError, task.CreatedAtMS, task.UpdatedAtMS)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (s *Store) LeaseMinuteAuditTask(ctx context.Context, id int64, token string, nowMS, leaseUntilMS int64) (bool, error) {
	if token == "" || len(token) > 64 || leaseUntilMS <= nowMS {
		return false, fmt.Errorf("invalid minute audit lease")
	}
	result, err := s.db.ExecContext(ctx, `UPDATE kline_minute_audit_task
		SET status='running',lease_token=?,lease_until_ms=?,attempts=attempts+1,updated_at_ms=?
		WHERE id=? AND ((status IN ('pending','retry','blocked') AND next_attempt_ms<=?)
		OR (status='running' AND lease_until_ms<=?))`, token, leaseUntilMS, nowMS, id, nowMS, nowMS)
	if err != nil {
		return false, err
	}
	count, err := result.RowsAffected()
	return count == 1, err
}

func (s *Store) LoadMinuteAuditTask(ctx context.Context, id int64) (*market.MinuteAuditTask, error) {
	task, err := scanMinuteAuditTask(s.db.QueryRowContext(ctx, `SELECT `+minuteAuditTaskColumns+` FROM kline_minute_audit_task WHERE id=?`, id))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return task, err
}

func (s *Store) ListDueMinuteAuditTasks(ctx context.Context, exchange string, nowMS int64, limit int) ([]market.MinuteAuditTask, error) {
	return s.listDueMinuteAuditTasks(ctx, exchange, "", nowMS, limit)
}

func (s *Store) ListDueMinuteAuditTasksByKind(ctx context.Context, exchange, kind string, nowMS int64, limit int) ([]market.MinuteAuditTask, error) {
	if kind != "daily" && kind != "non_daily" {
		return nil, fmt.Errorf("invalid minute audit work group %q", kind)
	}
	return s.listDueMinuteAuditTasks(ctx, exchange, kind, nowMS, limit)
}

func (s *Store) listDueMinuteAuditTasks(ctx context.Context, exchange, kind string, nowMS int64, limit int) ([]market.MinuteAuditTask, error) {
	if limit <= 0 {
		limit = 100
	}
	query := `SELECT ` + minuteAuditTaskColumns + ` FROM kline_minute_audit_task WHERE `
	var args []any
	if exchange != "" {
		query += `exchange=? AND `
		args = append(args, strings.ToLower(exchange))
	}
	if kind == "daily" {
		query += `kind='daily' AND `
	} else if kind == "non_daily" {
		query += `kind IN ('window','conflict','catchup') AND `
	}
	query += `((status IN ('pending','retry','blocked') AND next_attempt_ms<=?) OR (status='running' AND lease_until_ms<=?))
		ORDER BY next_attempt_ms,id LIMIT ?`
	args = append(args, nowMS, nowMS, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tasks []market.MinuteAuditTask
	for rows.Next() {
		task, err := scanMinuteAuditTask(rows)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, *task)
	}
	return tasks, rows.Err()
}

func scanMinuteAuditTask(row interface{ Scan(...any) error }) (*market.MinuteAuditTask, error) {
	var task market.MinuteAuditTask
	err := row.Scan(&task.ID, &task.Exchange, &task.Symbol, &task.MarketType, &task.Kind,
		&task.StartMS, &task.EndMS, &task.ExpectedStartMS, &task.ExpectData, &task.Status,
		&task.Attempts, &task.NextAttemptMS, &task.LeaseUntilMS, &task.LeaseToken,
		&task.ForceReplay, &task.LastError, &task.CreatedAtMS, &task.UpdatedAtMS)
	return &task, err
}

func (s *Store) FinishMinuteAuditTask(ctx context.Context, id int64, token, outcome string, nextAttemptMS int64, forceReplay bool, lastError string, nowMS int64) error {
	outcome, err := storage.NormalizeMinuteAuditOutcome(outcome)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var status, kind, previousToken string
	var leaseUntilMS int64
	err = tx.QueryRowContext(ctx, `SELECT status,kind,lease_token,lease_until_ms FROM kline_minute_audit_task WHERE id=? FOR UPDATE`, id).
		Scan(&status, &kind, &previousToken, &leaseUntilMS)
	if err == sql.ErrNoRows {
		return tx.Commit()
	}
	if err != nil {
		return err
	}
	if status == "completed" || status == "excluded" || status == "expired" {
		return tx.Commit()
	}
	if status != "running" || previousToken != token || leaseUntilMS <= nowMS {
		return storage.ErrMinuteAuditLeaseLost
	}
	if outcome == "completed" && kind != "daily" {
		_, err = tx.ExecContext(ctx, `DELETE FROM kline_minute_audit_task WHERE id=? AND lease_token=?`, id, token)
	} else {
		_, err = tx.ExecContext(ctx, `UPDATE kline_minute_audit_task
			SET status=?,next_attempt_ms=?,force_replay=?,last_error=?,updated_at_ms=?,lease_until_ms=0,lease_token=''
			WHERE id=? AND lease_token=?`, outcome, nextAttemptMS, forceReplay, storage.TruncateMinuteAuditError(lastError), nowMS, id, token)
	}
	if err != nil {
		return err
	}
	if kind == "daily" || outcome == "excluded" || outcome == "expired" {
		// Terminal rows are bounded evidence, not a permanent task log. Exclusion
		// and expiry also prune when daily auditing is disabled. Pending work is
		// never removed by this retention operation.
		cutoff := nowMS - int64(7*24*time.Hour/time.Millisecond)
		if _, err := tx.ExecContext(ctx, `DELETE FROM kline_minute_audit_task
			WHERE status IN ('completed','excluded','expired') AND updated_at_ms<? LIMIT 1000`, cutoff); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) LoadMinuteAuditCursor(ctx context.Context, exchange, symbol, kind string) (*market.MinuteAuditCursor, error) {
	var cursor market.MinuteAuditCursor
	err := s.db.QueryRowContext(ctx, `SELECT exchange,symbol,kind,through_ms,updated_at_ms
		FROM kline_minute_audit_cursor WHERE exchange=? AND symbol=? AND kind=?`,
		strings.ToLower(exchange), strings.ToUpper(symbol), strings.ToLower(kind)).
		Scan(&cursor.Exchange, &cursor.Symbol, &cursor.Kind, &cursor.ThroughMS, &cursor.UpdatedAtMS)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &cursor, err
}

func (s *Store) SaveMinuteAuditCursor(ctx context.Context, cursor market.MinuteAuditCursor) error {
	cursor, err := storage.NormalizeMinuteAuditCursor(cursor)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO kline_minute_audit_cursor(exchange,symbol,kind,through_ms,updated_at_ms)
		VALUES (?,?,?,?,?) ON DUPLICATE KEY UPDATE through_ms=GREATEST(through_ms,VALUES(through_ms)),
		updated_at_ms=GREATEST(updated_at_ms,VALUES(updated_at_ms))`,
		cursor.Exchange, cursor.Symbol, cursor.Kind, cursor.ThroughMS, cursor.UpdatedAtMS)
	return err
}

func (s *Store) MinuteAuditSummary(ctx context.Context, nowMS int64) (market.MinuteAuditSummary, error) {
	summary := market.MinuteAuditSummary{UpdatedAtMS: nowMS}
	const dayMS = int64(24 * time.Hour / time.Millisecond)
	err := s.db.QueryRowContext(ctx, `SELECT
		COALESCE(SUM(status IN ('pending','retry') OR (status='running' AND lease_until_ms<=?)),0),
		COALESCE(SUM(status='running' AND lease_until_ms>?),0),
		COALESCE(SUM(status='blocked'),0),COALESCE(SUM(status='expired'),0),COALESCE(SUM(status='excluded'),0),
		COALESCE(MIN(CASE WHEN status IN ('pending','retry','running','blocked') THEN start_ms END),0),
		COALESCE(MAX(CASE WHEN kind='daily' THEN (start_ms DIV ?) * ? END),0)
		FROM kline_minute_audit_task`, nowMS, nowMS, dayMS, dayMS).
		Scan(&summary.Pending, &summary.Running, &summary.Blocked, &summary.Expired, &summary.Excluded, &summary.OldestStartMS, &summary.DailyDayStartMS)
	if err != nil {
		return summary, err
	}
	if summary.DailyDayStartMS > 0 {
		err = s.db.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(status='completed'),0)
			FROM kline_minute_audit_task WHERE kind='daily' AND start_ms>=? AND start_ms<?`,
			summary.DailyDayStartMS, summary.DailyDayStartMS+dayMS).Scan(&summary.DailyTotal, &summary.DailyCompleted)
	}
	return summary, err
}
