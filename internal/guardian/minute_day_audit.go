package guardian

import (
	"context"
	"fmt"
	"log"
	"time"

	"crypto-ticket/internal/timeframe"
)

// Minute-day audits revisit the prior UTC day after the official settlement
// delay. They register durable minute tasks; one background worker per exchange
// executes them under both its one-HTTP-per-second and shared REST budgets.
func (g *Guardian) runMinuteDayAudits(ctx context.Context) error {
	if g.minuteTaskStore() == nil {
		return fmt.Errorf("minute day audit requires a durable minute audit task store")
	}
	for {
		if err := g.MinuteDayAuditOnce(ctx); err != nil && ctx.Err() == nil {
			log.Printf("kline guardian minute day scheduling failed: %v", err)
		}
		timer := time.NewTimer(time.Minute)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// MinuteDayAuditOnce schedules settled UTC days without fetching them inline.
// Its cursor is a registration watermark, never evidence of successful repair.
func (g *Guardian) MinuteDayAuditOnce(ctx context.Context) error {
	g.minuteDayRunMu.Lock()
	defer g.minuteDayRunMu.Unlock()
	tasks := g.minuteTaskStore()
	if tasks == nil {
		return fmt.Errorf("minute day audit requires a durable minute audit task store")
	}
	targets, err := g.auditTargets(ctx)
	if err != nil {
		return err
	}
	dayMS := timeframe.DurationMS("1D")
	latestDay := timeframe.FloorStartMS(g.now().Add(-g.cfg.MinuteDayAuditOffset).UnixMilli(), "1D") - dayMS
	if latestDay <= 0 {
		return nil
	}
	registered := 0
	for _, target := range targets {
		cursor, err := tasks.LoadMinuteAuditCursor(ctx, target.Exchange, target.Symbol, "daily")
		if err != nil {
			return err
		}
		firstDay := latestDay // First rollout checks yesterday, without a history flood.
		if cursor != nil {
			firstDay = timeframe.FloorStartMS(cursor.ThroughMS+timeframe.MinuteMS, "1D")
		}
		// A prolonged shutdown does not schedule years of obsolete minute reads.
		// Preserve the skipped interval as an expiring task rather than claiming
		// it was verified. The remaining whole days fit configured retention.
		oldestDay := timeframe.FloorStartMS(g.now().Add(-g.cfg.MinuteRetention).UnixMilli(), "1D") + dayMS
		if firstDay < oldestDay && firstDay <= latestDay {
			end := oldestDay - timeframe.MinuteMS
			task := g.newMinuteTask(target, firstDay, end, "daily")
			if _, err := tasks.EnqueueMinuteAuditTask(ctx, task); err != nil {
				return err
			}
			if err := g.saveMinuteCursor(ctx, target, "daily", end); err != nil {
				return err
			}
			firstDay = oldestDay
		}
		for day := firstDay; day <= latestDay; day += dayMS {
			end := day + dayMS - timeframe.MinuteMS
			task := g.newMinuteTask(target, day, end, "daily")
			if _, err := tasks.EnqueueMinuteAuditTask(ctx, task); err != nil {
				return err
			}
			if err := g.saveMinuteCursor(ctx, target, "daily", end); err != nil {
				return err
			}
			registered++
		}
	}
	if registered > 0 {
		log.Printf("kline guardian minute day tasks registered=%d latest_day=%s", registered, time.UnixMilli(latestDay).UTC().Format("2006-01-02"))
	}
	return nil
}
