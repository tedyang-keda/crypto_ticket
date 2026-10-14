package guardian

import (
	"context"
	"log"
	"sort"
	"time"

	"crypto-ticket/internal/timeframe"
)

// dailyAuditRange identifies a UTC day block that must be retried when the
// official request or repair failed. It is kept in memory intentionally: a
// restart simply performs the recent window again and resumes historical
// coverage from the current cursor.
type dailyAuditRange struct {
	StartMS int64
	EndMS   int64
}

func (g *Guardian) runDailyAudits(ctx context.Context) error {
	// Run once at startup so enabling the worker repairs existing gaps without
	// waiting for the first hourly tick.
	if err := g.dailyAuditOnce(ctx); err != nil && ctx.Err() == nil {
		log.Printf("kline guardian daily audit failed: %v", err)
	}
	ticker := time.NewTicker(g.cfg.DailyAuditInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := g.dailyAuditOnce(ctx); err != nil && ctx.Err() == nil {
				log.Printf("kline guardian daily audit failed: %v", err)
			}
		}
	}
}

// DailyAuditOnce runs one immediate official UTC daily audit. It is exported
// for startup diagnostics and deterministic tests; the normal worker invokes
// the same method from its startup and hourly loops.
func (g *Guardian) DailyAuditOnce(ctx context.Context) error {
	return g.dailyAuditOnce(ctx)
}

func (g *Guardian) dailyAuditOnce(ctx context.Context) error {
	targets, err := g.dailyAuditTargets(ctx)
	if err != nil {
		return err
	}
	if len(targets) == 0 {
		return nil
	}
	now := time.UnixMilli(g.now().UnixMilli()).UTC()
	lastDay := time.Date(now.Year(), now.Month(), now.Day()-1, 0, 0, 0, 0, time.UTC)
	dayMS := timeframe.DurationMS("1D")
	recentDays := int64(g.cfg.DailyAuditDays)
	if recentDays < 1 {
		recentDays = 1
	}
	recent := dailyAuditRange{StartMS: lastDay.UnixMilli() - (recentDays-1)*dayMS, EndMS: lastDay.UnixMilli()}
	lookbackDays := int64(g.cfg.DailyAuditLookbackDays)
	if lookbackDays < recentDays {
		lookbackDays = recentDays
	}
	historicalEnd := lastDay.UnixMilli() - recentDays*dayMS
	historicalStart := lastDay.UnixMilli() - lookbackDays*dayMS
	blockDays := recentDays
	blockCount := (lookbackDays + blockDays - 1) / blockDays
	block := int64(g.dailyAuditCursor) % blockCount
	historical := dailyAuditRange{
		StartMS: historicalStart + block*blockDays*dayMS,
		EndMS:   historicalStart + (block+1)*blockDays*dayMS - dayMS,
	}
	if historical.EndMS > historicalEnd {
		historical.EndMS = historicalEnd
	}
	g.dailyAuditCursor = int((block + 1) % blockCount)

	var firstErr error
	for _, target := range targets {
		for _, r := range []dailyAuditRange{recent, historical} {
			if r.EndMS < r.StartMS || ctx.Err() != nil {
				continue
			}
			key := target.Exchange + ":" + target.Symbol
			g.dailyAuditMu.Lock()
			retry := g.dailyRetries[key] != nil && g.dailyRetries[key][r]
			g.dailyAuditMu.Unlock()
			result, err := g.repairTimeframeRange(ctx, target.Fetcher, timeframeRepairRequest{
				Exchange: target.Exchange, Symbol: target.Symbol, Timeframe: "1D",
				StartMS: r.StartMS, EndMS: r.EndMS, Reason: "daily_audit",
				ExpectData:      target.FirstSeenAtMS > 0 && target.FirstSeenAtMS <= r.EndMS,
				ExpectedStartMS: target.FirstSeenAtMS,
				ForceReplay:     retry,
			})
			_ = result
			g.dailyAuditMu.Lock()
			if err != nil {
				if g.dailyRetries[key] == nil {
					g.dailyRetries[key] = make(map[dailyAuditRange]bool)
				}
				g.dailyRetries[key][r] = true
				if firstErr == nil {
					firstErr = err
				}
			} else if g.dailyRetries[key] != nil {
				delete(g.dailyRetries[key], r)
			}
			g.dailyAuditMu.Unlock()
		}
	}
	return firstErr
}

func (g *Guardian) dailyAuditTargets(ctx context.Context) ([]auditTarget, error) {
	active := true
	nowMS := g.now().UnixMilli()
	var targets []auditTarget
	for exchangeName := range g.fetchersByExchange {
		symbols, err := g.store.ListSymbols(ctx, exchangeName, &active)
		if err != nil {
			return nil, err
		}
		for _, symbol := range symbols {
			if !symbol.IsActive {
				continue
			}
			fetcher := g.fetcherFor(symbol.Exchange, symbol.MarketType)
			if fetcher == nil {
				continue
			}
			// An entry that has never been observed is still useful for the
			// official daily check; the repair layer handles empty pre-listing
			// ranges without fabricating bars.
			if symbol.LastSeenAtMS > 0 && nowMS-symbol.LastSeenAtMS > int64(30*24*time.Hour/time.Millisecond) {
				continue
			}
			targets = append(targets, auditTarget{Exchange: normalizeExchange(symbol.Exchange), Symbol: normalizeSymbol(symbol.Symbol), MarketType: normalizeMarketType(symbol.MarketType), Fetcher: fetcher, FirstSeenAtMS: symbol.FirstSeenAtMS})
		}
	}
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].Exchange != targets[j].Exchange {
			return targets[i].Exchange < targets[j].Exchange
		}
		return targets[i].Symbol < targets[j].Symbol
	})
	return targets, nil
}
