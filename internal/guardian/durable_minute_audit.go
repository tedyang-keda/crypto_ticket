package guardian

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"crypto-ticket/internal/market"
	"crypto-ticket/internal/storage"
	"crypto-ticket/internal/timeframe"
)

const minuteTaskLease = 10 * time.Minute

type minuteLockContextKey struct{}

func minuteSymbolKey(exchange, symbol string) string {
	return normalizeExchange(exchange) + ":" + normalizeSymbol(symbol)
}

func (g *Guardian) minuteTaskStore() storage.MinuteAuditTaskStore {
	store, _ := g.store.(storage.MinuteAuditTaskStore)
	return store
}

func (g *Guardian) minuteLock(exchange, symbol string) *sync.Mutex {
	key := minuteSymbolKey(exchange, symbol)
	var hash uint32 = 2166136261
	for i := range key {
		hash = (hash ^ uint32(key[i])) * 16777619
	}
	return &g.minuteLocks[hash%uint32(len(g.minuteLocks))]
}

func (g *Guardian) newMinuteTask(target auditTarget, startMS, endMS int64, kind string) market.MinuteAuditTask {
	nowMS := g.now().UnixMilli()
	return market.MinuteAuditTask{
		Exchange: normalizeExchange(target.Exchange), Symbol: normalizeSymbol(target.Symbol),
		MarketType: normalizeMarketType(target.MarketType), Kind: kind,
		StartMS: timeframe.FloorStartMS(startMS, "1m"), EndMS: timeframe.FloorStartMS(endMS, "1m"),
		ExpectedStartMS: target.ListedAtMS, ExpectData: target.ListedAtMS > 0,
		CreatedAtMS: nowMS, UpdatedAtMS: nowMS, NextAttemptMS: nowMS,
	}
}

// Register every interval before fetching. A task is acknowledged only after
// official validation, source writes, verification markers and cascades return.
func (g *Guardian) repairDurableMinuteRange(ctx context.Context, fetcher Fetcher, exchangeName, symbol string, startMS, endMS int64, reason string) (repairResult, error) {
	kind := "window"
	if reason == "watermark_gap" {
		kind = "catchup"
	}
	task := g.newMinuteTask(auditTarget{Exchange: exchangeName, Symbol: symbol, MarketType: fetcher.MarketType()}, startMS, endMS, kind)
	id, err := g.minuteTaskStore().EnqueueMinuteAuditTask(ctx, task)
	if err != nil {
		return repairResult{}, fmt.Errorf("register minute audit: %w", err)
	}
	task.ID = id
	return g.executeMinuteTask(ctx, task, fetcher)
}

func (g *Guardian) auditMinuteWindow(ctx context.Context, target auditTarget, startMS, endMS int64) (repairResult, error) {
	tasks := g.minuteTaskStore()
	if tasks == nil {
		return g.repairRangeWithFetcher(ctx, target.Fetcher, target.Exchange, target.Symbol, startMS, endMS, "window_audit")
	}
	cursor, err := tasks.LoadMinuteAuditCursor(ctx, target.Exchange, target.Symbol, "window")
	if err != nil {
		return repairResult{}, err
	}
	from := startMS
	if cursor != nil {
		from = cursor.ThroughMS + timeframe.MinuteMS
	}
	retainedStart := timeframe.FloorStartMS(g.now().Add(-g.cfg.MinuteRetention).UnixMilli(), "1m") + timeframe.MinuteMS
	if from < retainedStart && from < startMS {
		end := retainedStart - timeframe.MinuteMS
		if end >= startMS {
			end = startMS - timeframe.MinuteMS
		}
		if _, err := tasks.EnqueueMinuteAuditTask(ctx, g.newMinuteTask(target, from, end, "catchup")); err != nil {
			return repairResult{}, err
		}
		if err := g.saveMinuteCursor(ctx, target, "window", end); err != nil {
			return repairResult{}, err
		}
		from = end + timeframe.MinuteMS
	}
	// Register downtime gaps separately from the current rolling range. Daily
	// chunks bound pagination and lease duration, without a full-history crawl.
	for from < startMS {
		end := from + 24*timeframe.DurationMS("1H") - timeframe.MinuteMS
		if end >= startMS {
			end = startMS - timeframe.MinuteMS
		}
		task := g.newMinuteTask(target, from, end, "catchup")
		if _, err := tasks.EnqueueMinuteAuditTask(ctx, task); err != nil {
			return repairResult{}, err
		}
		if err := g.saveMinuteCursor(ctx, target, "window", end); err != nil {
			return repairResult{}, err
		}
		from = end + timeframe.MinuteMS
	}
	task := g.newMinuteTask(target, startMS, endMS, "window")
	task.ID, err = tasks.EnqueueMinuteAuditTask(ctx, task)
	if err != nil {
		return repairResult{}, err
	}
	if err := g.saveMinuteCursor(ctx, target, "window", endMS); err != nil {
		return repairResult{}, err
	}
	return g.executeMinuteTask(ctx, task, target.Fetcher)
}

func (g *Guardian) saveMinuteCursor(ctx context.Context, target auditTarget, kind string, throughMS int64) error {
	return g.minuteTaskStore().SaveMinuteAuditCursor(ctx, market.MinuteAuditCursor{
		Exchange: target.Exchange, Symbol: target.Symbol, Kind: kind,
		ThroughMS: throughMS, UpdatedAtMS: g.now().UnixMilli(),
	})
}

func (g *Guardian) executeMinuteTask(ctx context.Context, task market.MinuteAuditTask, fetcher Fetcher) (repairResult, error) {
	// Waiting for another REST repair must not consume this task's lease. Once
	// the symbol lock is held, reload, claim, and execute under a shorter deadline.
	lock := g.minuteLock(task.Exchange, task.Symbol)
	lock.Lock()
	defer lock.Unlock()
	if err := ctx.Err(); err != nil {
		return repairResult{}, err
	}
	ctx = context.WithValue(ctx, minuteLockContextKey{}, minuteSymbolKey(task.Exchange, task.Symbol))
	tasks := g.minuteTaskStore()
	stored, err := tasks.LoadMinuteAuditTask(ctx, task.ID)
	if err != nil {
		return repairResult{}, err
	}
	if stored == nil {
		// Another worker may have completed and deleted this deduplicated range
		// while the caller waited for the symbol lock.
		return repairResult{}, nil
	}
	// Enqueue deduplicates identities without resetting existing backoff or
	// replay flags. Restore that persisted work before attempting a lease.
	if task.ExpectedStartMS > 0 && stored.ExpectedStartMS == 0 {
		stored.ExpectedStartMS, stored.ExpectData = task.ExpectedStartMS, task.ExpectData
	}
	task = *stored
	var tokenBytes [16]byte
	if _, err := rand.Read(tokenBytes[:]); err != nil {
		return repairResult{}, err
	}
	token := fmt.Sprintf("%x", tokenBytes)
	nowMS := g.now().UnixMilli()
	leased, err := tasks.LeaseMinuteAuditTask(ctx, task.ID, token, nowMS, nowMS+minuteTaskLease.Milliseconds())
	if err != nil {
		return repairResult{}, err
	}
	if !leased {
		// Running/backoff tasks remain visible in the durable summary. They are
		// not a new failure of this rolling round.
		return repairResult{}, nil
	}
	return g.executeLeasedMinuteTask(ctx, task, token, fetcher)
}

func (g *Guardian) executeLeasedMinuteTask(ctx context.Context, task market.MinuteAuditTask, token string, fetcher Fetcher) (repairResult, error) {
	requestCtx, requestCancel := context.WithTimeout(ctx, 8*time.Minute)
	defer requestCancel()
	if task.Kind == "daily" {
		requestCtx = context.WithValue(requestCtx, minuteDayAuditContextKey{}, true)
	}
	startMS := task.StartMS
	if task.ExpectedStartMS > startMS && task.ExpectedStartMS <= task.EndMS+timeframe.MinuteMS-1 {
		startMS = timeframe.FloorStartMS(task.ExpectedStartMS, "1m")
	}
	result, repairErr := g.repairTimeframeRange(requestCtx, fetcher, timeframeRepairRequest{
		Exchange: task.Exchange, Symbol: task.Symbol, Timeframe: "1m",
		StartMS: startMS, EndMS: task.EndMS, ExpectedStartMS: task.ExpectedStartMS,
		ExpectData: task.ExpectData, ForceReplay: task.ForceReplay || task.Attempts > 0,
		Reason: "minute_" + task.Kind,
	})
	outcome, nextMS, lastError := "completed", int64(0), ""
	if repairErr != nil {
		outcome = "retry"
		if strings.Contains(repairErr.Error(), "unchecked official") {
			outcome = "blocked"
		}
		lastError = repairErr.Error()
		nextMS = g.now().UnixMilli() + minuteRetryDelay(task.Attempts).Milliseconds()
	}
	// Cancellation must leave an explicit retry when possible; a hard exit is
	// still safe because an unfinished lease becomes claimable after expiry.
	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	finishErr := g.minuteTaskStore().FinishMinuteAuditTask(finishCtx, task.ID, token, outcome, nextMS, repairErr != nil, lastError, g.now().UnixMilli())
	return result, errors.Join(repairErr, finishErr)
}

func minuteRetryDelay(attempts int) time.Duration {
	if attempts < 0 {
		attempts = 0
	}
	if attempts > 5 {
		attempts = 5
	}
	delay := 30 * time.Second * time.Duration(1<<attempts)
	if delay > 15*time.Minute {
		return 15 * time.Minute
	}
	return delay
}

func (g *Guardian) runMinuteRetries(ctx context.Context) error {
	var wg sync.WaitGroup
	for exchangeName := range g.fetchersByExchange {
		for _, daily := range []bool{false, true} {
			if daily && !g.cfg.MinuteDayAuditEnabled {
				continue
			}
			wg.Add(1)
			go func(exchangeName string, daily bool) {
				defer wg.Done()
				g.runMinuteRetryWorker(ctx, exchangeName, daily)
			}(exchangeName, daily)
		}
	}
	wg.Wait()
	return ctx.Err()
}

func (g *Guardian) runMinuteRetryWorker(ctx context.Context, exchangeName string, daily bool) {
	for ctx.Err() == nil {
		processed, err := g.minuteRetryPass(ctx, exchangeName, daily)
		if err != nil && ctx.Err() == nil {
			log.Printf("kline guardian minute retry exchange=%s daily=%t err=%v", exchangeName, daily, err)
		}
		if processed > 0 && err == nil {
			continue
		}
		timer := time.NewTimer(g.cfg.MinuteRetryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
}

func (g *Guardian) minuteRetryPass(ctx context.Context, exchangeName string, daily bool) (int, error) {
	tasks := g.minuteTaskStore()
	kind := "non_daily"
	if daily {
		kind = "daily"
	}
	due, err := tasks.ListDueMinuteAuditTasksByKind(ctx, exchangeName, kind, g.now().UnixMilli(), 32)
	if err != nil || len(due) == 0 {
		return 0, err
	}
	targets, err := g.auditTargets(ctx)
	if err != nil {
		return 0, err
	}
	bySymbol := make(map[string]auditTarget)
	for _, target := range targets {
		if target.Exchange == exchangeName {
			bySymbol[target.Symbol] = target
		}
	}
	// Fresh official metadata distinguishes an intentional exclusion from an
	// unavailable inventory. An unavailable snapshot must never acknowledge work.
	snapshots := make(map[string]market.SymbolSnapshot)
	snapshotSyncs := make(map[string]market.SymbolSnapshotSync)
	if reader, ok := g.store.(symbolSnapshotReader); ok {
		items, readErr := reader.ListSymbolSnapshots(ctx, exchangeName)
		if readErr != nil {
			return 0, readErr
		}
		for _, item := range items {
			snapshots[normalizeSymbol(item.Symbol)] = item
		}
		syncs, readErr := reader.ListSymbolSnapshotSyncs(ctx)
		if readErr != nil {
			return 0, readErr
		}
		for _, item := range syncs {
			snapshotSyncs[fetcherKey(item.Exchange, item.MarketType)] = item
		}
	}
	processed := 0
	for _, task := range due {
		if (task.Kind == "daily") != daily || ctx.Err() != nil {
			continue
		}
		if processed >= 32 {
			break
		}
		processed++
		target, eligible := bySymbol[task.Symbol]
		if eligible && target.ListedAtMS > 0 {
			task.ExpectedStartMS, task.ExpectData = target.ListedAtMS, true
		}
		outcome, reason := "", ""
		retainedStart := timeframe.FloorStartMS(g.now().Add(-g.cfg.MinuteRetention).UnixMilli(), "1m") + timeframe.MinuteMS
		if task.StartMS < retainedStart {
			if task.EndMS >= retainedStart {
				remainder := task
				remainder.ID, remainder.StartMS, remainder.CreatedAtMS = 0, retainedStart, g.now().UnixMilli()
				remainder.NextAttemptMS, remainder.ForceReplay = remainder.CreatedAtMS, true
				if _, err := tasks.EnqueueMinuteAuditTask(ctx, remainder); err != nil {
					return processed, err
				}
			}
			outcome, reason = "expired", "minute source range exceeded configured retention; retained remainder stays queued"
		} else if task.ExpectedStartMS > task.EndMS+timeframe.MinuteMS-1 {
			outcome, reason = "excluded", "range precedes the official listing timestamp"
		} else if !eligible {
			outcome, reason = "blocked", "subscription or fresh official snapshot is unavailable"
			if item, ok := snapshots[task.Symbol]; ok {
				sync := snapshotSyncs[fetcherKey(item.Exchange, item.MarketType)]
				if sync.Status == "ok" && sync.LastSuccessMS == item.CapturedAtMS && item.Presence == "present" && (item.Delisted || !item.Subscribed) && item.CapturedAtMS > 0 && item.CapturedAtMS <= g.now().UnixMilli() && g.now().UnixMilli()-item.CapturedAtMS <= g.cfg.SymbolMaxAge.Milliseconds() {
					outcome, reason = "excluded", "officially delisted or explicitly not subscribed: "+item.SubscriptionReason
				}
			}
		}
		if outcome != "" {
			if err := g.deferMinuteTask(ctx, task, outcome, reason); err != nil {
				return processed, err
			}
			continue
		}
		if _, err := g.executeMinuteTask(ctx, task, target.Fetcher); err != nil && ctx.Err() == nil {
			log.Printf("kline guardian durable minute task failed task=%d exchange=%s symbol=%s kind=%s start_ms=%d end_ms=%d err=%v", task.ID, task.Exchange, task.Symbol, task.Kind, task.StartMS, task.EndMS, err)
		}
	}
	return processed, ctx.Err()
}

func (g *Guardian) deferMinuteTask(ctx context.Context, task market.MinuteAuditTask, outcome, reason string) error {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return err
	}
	token := fmt.Sprintf("%x", bytes)
	nowMS := g.now().UnixMilli()
	leased, err := g.minuteTaskStore().LeaseMinuteAuditTask(ctx, task.ID, token, nowMS, nowMS+minuteTaskLease.Milliseconds())
	if err != nil || !leased {
		return err
	}
	return g.minuteTaskStore().FinishMinuteAuditTask(ctx, task.ID, token, outcome, nowMS+5*time.Minute.Milliseconds(), task.ForceReplay, reason, nowMS)
}
