package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"crypto-ticket/internal/market"
)

var ErrMinuteAuditLeaseLost = errors.New("minute audit lease lost")

// MinuteAuditTaskStore keeps outstanding ranges independent of the rolling
// audit window. Each lease owns an immutable range and a completion token.
type MinuteAuditTaskStore interface {
	EnqueueMinuteAuditTask(context.Context, market.MinuteAuditTask) (int64, error)
	LoadMinuteAuditTask(context.Context, int64) (*market.MinuteAuditTask, error)
	LeaseMinuteAuditTask(context.Context, int64, string, int64, int64) (bool, error)
	ListDueMinuteAuditTasks(context.Context, string, int64, int) ([]market.MinuteAuditTask, error)
	ListDueMinuteAuditTasksByKind(context.Context, string, string, int64, int) ([]market.MinuteAuditTask, error)
	FinishMinuteAuditTask(context.Context, int64, string, string, int64, bool, string, int64) error
	LoadMinuteAuditCursor(context.Context, string, string, string) (*market.MinuteAuditCursor, error)
	SaveMinuteAuditCursor(context.Context, market.MinuteAuditCursor) error
	MinuteAuditSummary(context.Context, int64) (market.MinuteAuditSummary, error)
}

var _ MinuteAuditTaskStore = (*MemoryHistoricalStore)(nil)

// NormalizeMinuteAuditTask is shared by the SQL and memory repositories so
// invalid or differently-cased identities cannot create divergent work.
func NormalizeMinuteAuditTask(task market.MinuteAuditTask) (market.MinuteAuditTask, error) {
	task.Exchange = strings.ToLower(strings.TrimSpace(task.Exchange))
	task.Symbol = strings.ToUpper(strings.TrimSpace(task.Symbol))
	task.MarketType = strings.TrimSpace(task.MarketType)
	task.Kind = strings.ToLower(strings.TrimSpace(task.Kind))
	if task.Exchange == "" || task.Symbol == "" || len(task.Exchange) > 16 || len(task.Symbol) > 64 || len(task.MarketType) > 32 {
		return task, fmt.Errorf("invalid minute audit market identity")
	}
	switch task.Kind {
	case "window", "daily", "conflict", "catchup":
	default:
		return task, fmt.Errorf("invalid minute audit kind %q", task.Kind)
	}
	if task.StartMS <= 0 || task.EndMS < task.StartMS || task.StartMS%60_000 != 0 || task.EndMS%60_000 != 0 {
		return task, fmt.Errorf("invalid minute audit candle range")
	}
	if task.CreatedAtMS <= 0 {
		task.CreatedAtMS = task.UpdatedAtMS
	}
	if task.CreatedAtMS <= 0 {
		task.CreatedAtMS = time.Now().UnixMilli()
	}
	if task.UpdatedAtMS <= 0 {
		task.UpdatedAtMS = task.CreatedAtMS
	}
	if task.NextAttemptMS <= 0 {
		task.NextAttemptMS = task.CreatedAtMS
	}
	task.ID, task.Attempts, task.LeaseUntilMS, task.LeaseToken = 0, 0, 0, ""
	task.Status = "pending"
	task.LastError = TruncateMinuteAuditError(task.LastError)
	return task, nil
}

func NormalizeMinuteAuditCursor(cursor market.MinuteAuditCursor) (market.MinuteAuditCursor, error) {
	cursor.Exchange = strings.ToLower(strings.TrimSpace(cursor.Exchange))
	cursor.Symbol = strings.ToUpper(strings.TrimSpace(cursor.Symbol))
	cursor.Kind = strings.ToLower(strings.TrimSpace(cursor.Kind))
	if cursor.Exchange == "" || cursor.Symbol == "" || len(cursor.Exchange) > 16 || len(cursor.Symbol) > 64 || len(cursor.Kind) == 0 || len(cursor.Kind) > 16 || cursor.ThroughMS < 0 {
		return cursor, fmt.Errorf("invalid minute audit cursor")
	}
	return cursor, nil
}

func NormalizeMinuteAuditOutcome(outcome string) (string, error) {
	if outcome == "done" {
		outcome = "completed"
	}
	switch outcome {
	case "completed", "retry", "blocked", "excluded", "expired":
		return outcome, nil
	default:
		return "", fmt.Errorf("invalid minute audit outcome %q", outcome)
	}
}

func TruncateMinuteAuditError(value string) string {
	if chars := []rune(value); len(chars) > 1024 {
		return string(chars[:1024])
	}
	return value
}
