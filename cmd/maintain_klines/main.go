package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"crypto-ticket/internal/config"
	"crypto-ticket/internal/retention"
	mysqlstore "crypto-ticket/internal/storage/mysql"
	"crypto-ticket/internal/timeframe"
)

type options struct {
	mode            string
	timeframes      []string
	dryRun          bool
	batchSize       int
	maxBatches      int
	now             time.Time
	tableName       string
	partitionStart  time.Time
	partitionMonths int
}

func main() {
	cfg := config.Load()
	opts, err := parseOptions()
	if err != nil {
		log.Fatalf("invalid options: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch opts.mode {
	case "retention":
		if err := runRetention(ctx, cfg, opts); err != nil {
			log.Fatalf("retention: %v", err)
		}
	case "partition-create-sql":
		fmt.Print(mysqlstore.BuildTimeframePartitionMigrationSQL(partitionOptions(opts)))
	case "partition-add-sql":
		fmt.Print(mysqlstore.BuildAddTimeframePartitionsSQL(partitionOptions(opts)))
	case "partition-drop-sql":
		fmt.Print(mysqlstore.BuildDropExpiredTimeframePartitionsSQL(partitionOptions(opts), opts.now))
	default:
		log.Fatalf("unsupported mode: %s", opts.mode)
	}
}

func parseOptions() (options, error) {
	var opts options
	var timeframesRaw string
	var nowRaw string
	var partitionStartRaw string
	flag.StringVar(&opts.mode, "mode", "retention", "retention, partition-create-sql, partition-add-sql, or partition-drop-sql")
	flag.StringVar(&timeframesRaw, "timeframes", strings.Join(timeframe.Order, ","), "comma-separated timeframes for retention")
	flag.BoolVar(&opts.dryRun, "dry-run", true, "log/count only; set false to delete")
	flag.IntVar(&opts.batchSize, "batch-size", 10_000, "delete batch size for retention")
	flag.IntVar(&opts.maxBatches, "max-batches", 0, "max delete batches per timeframe; 0 means unlimited")
	flag.StringVar(&nowRaw, "now", "", "optional current time override: unix ms, RFC3339, or YYYY-MM-DD")
	flag.StringVar(&opts.tableName, "table", "bar_history", "bar history table name for partition SQL")
	flag.StringVar(&partitionStartRaw, "partition-start", "2026-01", "first generated partition month, YYYY-MM")
	flag.IntVar(&opts.partitionMonths, "partition-months", 12, "number of month partitions per expiring timeframe")
	flag.Parse()

	opts.timeframes = normalizeTimeframes(timeframesRaw)
	if len(opts.timeframes) == 0 {
		return opts, fmt.Errorf("at least one timeframe is required")
	}
	if opts.batchSize <= 0 {
		opts.batchSize = 10_000
	}
	now, err := parseTimeOption(nowRaw)
	if err != nil {
		return opts, fmt.Errorf("parse -now: %w", err)
	}
	opts.now = now
	start, err := parseMonthOption(partitionStartRaw)
	if err != nil {
		return opts, fmt.Errorf("parse -partition-start: %w", err)
	}
	opts.partitionStart = start
	return opts, nil
}

func runRetention(ctx context.Context, cfg config.Config, opts options) error {
	store, err := mysqlstore.New(cfg.MySQLDSN)
	if err != nil {
		return err
	}
	defer store.Close()
	if err := store.EnsureSchema(ctx); err != nil {
		return err
	}

	for _, tf := range opts.timeframes {
		rule := retention.RuleFor(tf)
		cutoffMS, ok := retention.CutoffMS(rule, opts.now)
		if !ok {
			log.Printf("retention timeframe=%s keep=forever skip", tf)
			continue
		}
		if opts.dryRun {
			count, err := store.CountBarsBefore(ctx, tf, cutoffMS)
			if err != nil {
				return err
			}
			log.Printf("dry-run retention timeframe=%s keep_days=%d cutoff_ms=%d rows=%d", tf, rule.KeepDays, cutoffMS, count)
			continue
		}
		var total int64
		for batch := 1; ; batch++ {
			deleted, err := store.DeleteBarsBefore(ctx, tf, cutoffMS, opts.batchSize)
			if err != nil {
				return err
			}
			total += deleted
			log.Printf("retention timeframe=%s batch=%d deleted=%d total=%d cutoff_ms=%d", tf, batch, deleted, total, cutoffMS)
			if deleted < int64(opts.batchSize) || (opts.maxBatches > 0 && batch >= opts.maxBatches) {
				break
			}
		}
	}
	return nil
}

func partitionOptions(opts options) mysqlstore.TimeframePartitionOptions {
	return mysqlstore.TimeframePartitionOptions{
		TableName:  opts.tableName,
		StartMonth: opts.partitionStart,
		Months:     opts.partitionMonths,
	}
}

func normalizeTimeframes(raw string) []string {
	seen := map[string]bool{}
	var out []string
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		tf := timeframe.MustNormalize(item)
		if seen[tf] {
			continue
		}
		seen[tf] = true
		out = append(out, tf)
	}
	return out
}

func parseTimeOption(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Now().UTC(), nil
	}
	if parsed, err := time.Parse(time.RFC3339, raw); err == nil {
		return parsed.UTC(), nil
	}
	if parsed, err := time.Parse("2006-01-02", raw); err == nil {
		return parsed.UTC(), nil
	}
	var ms int64
	if _, err := fmt.Sscanf(raw, "%d", &ms); err == nil && ms > 0 {
		return time.UnixMilli(ms).UTC(), nil
	}
	return time.Time{}, fmt.Errorf("unsupported time %q", raw)
}

func parseMonthOption(raw string) (time.Time, error) {
	parsed, err := time.Parse("2006-01", strings.TrimSpace(raw))
	if err != nil {
		return time.Time{}, err
	}
	return time.Date(parsed.Year(), parsed.Month(), 1, 0, 0, 0, 0, time.UTC), nil
}
