package storage

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"crypto-ticket/internal/market"
)

func (m *MemoryHistoricalStore) EnqueueMinuteAuditTask(ctx context.Context, task market.MinuteAuditTask) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	task, err := NormalizeMinuteAuditTask(task)
	if err != nil {
		return 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.minuteAuditTasks == nil {
		m.minuteAuditTasks = make(map[int64]market.MinuteAuditTask)
	}
	for _, existing := range m.minuteAuditTasks {
		if existing.Exchange == task.Exchange && existing.Symbol == task.Symbol && existing.Kind == task.Kind && existing.StartMS == task.StartMS && existing.EndMS == task.EndMS {
			return existing.ID, nil
		}
	}
	m.minuteAuditNextID++
	task.ID = m.minuteAuditNextID
	m.minuteAuditTasks[task.ID] = task
	return task.ID, nil
}

func (m *MemoryHistoricalStore) LeaseMinuteAuditTask(ctx context.Context, id int64, token string, nowMS, leaseUntilMS int64) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if token == "" || len(token) > 64 || leaseUntilMS <= nowMS {
		return false, fmt.Errorf("invalid minute audit lease")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	task, ok := m.minuteAuditTasks[id]
	if !ok || !minuteAuditTaskDue(task, nowMS) {
		return false, nil
	}
	task.Status, task.LeaseToken, task.LeaseUntilMS = "running", token, leaseUntilMS
	task.Attempts++
	task.UpdatedAtMS = nowMS
	m.minuteAuditTasks[id] = task
	return true, nil
}

func (m *MemoryHistoricalStore) LoadMinuteAuditTask(ctx context.Context, id int64) (*market.MinuteAuditTask, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	task, ok := m.minuteAuditTasks[id]
	if !ok {
		return nil, nil
	}
	return &task, nil
}

func (m *MemoryHistoricalStore) ListDueMinuteAuditTasks(ctx context.Context, exchange string, nowMS int64, limit int) ([]market.MinuteAuditTask, error) {
	return m.listDueMinuteAuditTasks(ctx, exchange, "", nowMS, limit)
}

// By-kind filtering happens before the limit so a large rolling backlog cannot
// hide daily work from its independent worker, or vice versa.
func (m *MemoryHistoricalStore) ListDueMinuteAuditTasksByKind(ctx context.Context, exchange, kind string, nowMS int64, limit int) ([]market.MinuteAuditTask, error) {
	if kind != "daily" && kind != "non_daily" {
		return nil, fmt.Errorf("invalid minute audit work group %q", kind)
	}
	return m.listDueMinuteAuditTasks(ctx, exchange, kind, nowMS, limit)
}

func (m *MemoryHistoricalStore) listDueMinuteAuditTasks(ctx context.Context, exchange, kind string, nowMS int64, limit int) ([]market.MinuteAuditTask, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 100
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	var tasks []market.MinuteAuditTask
	for _, task := range m.minuteAuditTasks {
		if (kind == "daily" && task.Kind != "daily") || (kind == "non_daily" && task.Kind == "daily") {
			continue
		}
		if (exchange == "" || strings.EqualFold(task.Exchange, exchange)) && minuteAuditTaskDue(task, nowMS) {
			tasks = append(tasks, task)
		}
	}
	sort.Slice(tasks, func(i, j int) bool {
		if tasks[i].NextAttemptMS != tasks[j].NextAttemptMS {
			return tasks[i].NextAttemptMS < tasks[j].NextAttemptMS
		}
		return tasks[i].ID < tasks[j].ID
	})
	if len(tasks) > limit {
		tasks = tasks[:limit]
	}
	return tasks, nil
}

func minuteAuditTaskDue(task market.MinuteAuditTask, nowMS int64) bool {
	switch task.Status {
	case "pending", "retry", "blocked":
		return task.NextAttemptMS <= nowMS
	case "running":
		return task.LeaseUntilMS <= nowMS
	default:
		return false
	}
}

func (m *MemoryHistoricalStore) FinishMinuteAuditTask(ctx context.Context, id int64, token, outcome string, nextAttemptMS int64, forceReplay bool, lastError string, nowMS int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	outcome, err := NormalizeMinuteAuditOutcome(outcome)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	task, ok := m.minuteAuditTasks[id]
	if !ok || task.Status == "completed" || task.Status == "excluded" || task.Status == "expired" {
		return nil
	}
	if task.Status != "running" || task.LeaseToken != token || task.LeaseUntilMS <= nowMS {
		return ErrMinuteAuditLeaseLost
	}
	if outcome == "completed" && task.Kind != "daily" {
		delete(m.minuteAuditTasks, id)
		return nil
	}
	task.Status, task.LeaseToken, task.LeaseUntilMS = outcome, "", 0
	task.NextAttemptMS, task.ForceReplay = nextAttemptMS, forceReplay
	task.LastError, task.UpdatedAtMS = TruncateMinuteAuditError(lastError), nowMS
	m.minuteAuditTasks[id] = task
	if task.Kind == "daily" || outcome == "excluded" || outcome == "expired" {
		cutoff := nowMS - int64(7*24*time.Hour/time.Millisecond)
		for taskID, old := range m.minuteAuditTasks {
			if (old.Status == "completed" || old.Status == "excluded" || old.Status == "expired") && old.UpdatedAtMS < cutoff {
				delete(m.minuteAuditTasks, taskID)
			}
		}
	}
	return nil
}

func (m *MemoryHistoricalStore) LoadMinuteAuditCursor(ctx context.Context, exchange, symbol, kind string) (*market.MinuteAuditCursor, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	cursor, ok := m.minuteAuditCursors[minuteAuditCursorKey(exchange, symbol, kind)]
	if !ok {
		return nil, nil
	}
	return &cursor, nil
}

func (m *MemoryHistoricalStore) SaveMinuteAuditCursor(ctx context.Context, cursor market.MinuteAuditCursor) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	cursor, err := NormalizeMinuteAuditCursor(cursor)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.minuteAuditCursors == nil {
		m.minuteAuditCursors = make(map[string]market.MinuteAuditCursor)
	}
	key := minuteAuditCursorKey(cursor.Exchange, cursor.Symbol, cursor.Kind)
	previous := m.minuteAuditCursors[key]
	if previous.ThroughMS > cursor.ThroughMS {
		cursor.ThroughMS = previous.ThroughMS
	}
	if previous.UpdatedAtMS > cursor.UpdatedAtMS {
		cursor.UpdatedAtMS = previous.UpdatedAtMS
	}
	m.minuteAuditCursors[key] = cursor
	return nil
}

func minuteAuditCursorKey(exchange, symbol, kind string) string {
	return strings.ToLower(exchange) + ":" + strings.ToUpper(symbol) + ":" + strings.ToLower(kind)
}

func (m *MemoryHistoricalStore) MinuteAuditSummary(ctx context.Context, nowMS int64) (market.MinuteAuditSummary, error) {
	if err := ctx.Err(); err != nil {
		return market.MinuteAuditSummary{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	summary := market.MinuteAuditSummary{UpdatedAtMS: nowMS}
	for _, task := range m.minuteAuditTasks {
		addMinuteAuditSummary(&summary, task, nowMS)
	}
	for _, task := range m.minuteAuditTasks {
		if task.Kind == "daily" && minuteAuditDayStart(task.StartMS) == summary.DailyDayStartMS {
			summary.DailyTotal++
			if task.Status == "completed" {
				summary.DailyCompleted++
			}
		}
	}
	return summary, nil
}

func addMinuteAuditSummary(summary *market.MinuteAuditSummary, task market.MinuteAuditTask, nowMS int64) {
	unverified := false
	switch task.Status {
	case "pending", "retry":
		summary.Pending++
		unverified = true
	case "running":
		if task.LeaseUntilMS <= nowMS {
			summary.Pending++
		} else {
			summary.Running++
		}
		unverified = true
	case "blocked":
		summary.Blocked++
		unverified = true
	case "expired":
		summary.Expired++
	case "excluded":
		summary.Excluded++
	}
	if unverified && (summary.OldestStartMS == 0 || task.StartMS < summary.OldestStartMS) {
		summary.OldestStartMS = task.StartMS
	}
	if task.Kind == "daily" && minuteAuditDayStart(task.StartMS) > summary.DailyDayStartMS {
		summary.DailyDayStartMS = minuteAuditDayStart(task.StartMS)
	}
}

func minuteAuditDayStart(startMS int64) int64 {
	const dayMS = int64(24 * time.Hour / time.Millisecond)
	return startMS / dayMS * dayMS
}
