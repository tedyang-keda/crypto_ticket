package mysql

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"crypto-ticket/internal/retention"
	"crypto-ticket/internal/timeframe"
)

type TimeframePartitionOptions struct {
	TableName  string
	StartMonth time.Time
	Months     int
}

func BuildTimeframePartitionMigrationSQL(options TimeframePartitionOptions) string {
	tableName := partitionTableName(options.TableName)
	nextTable := tableName + "_timeframe"
	var out strings.Builder
	out.WriteString("DROP TABLE IF EXISTS `" + nextTable + "`;\n")
	out.WriteString("CREATE TABLE `" + nextTable + "` (\n")
	out.WriteString(barHistoryColumnsDDL())
	out.WriteString("\n)\n")
	out.WriteString(BuildTimeframePartitionClause(options))
	out.WriteString(";\n\n")
	out.WriteString("INSERT INTO `" + nextTable + "` SELECT * FROM `" + tableName + "`;\n")
	out.WriteString("RENAME TABLE `" + tableName + "` TO `" + tableName + "_exchange_partition_backup`, `" + nextTable + "` TO `" + tableName + "`;\n")
	return out.String()
}

func BuildTimeframePartitionClause(options TimeframePartitionOptions) string {
	months := normalizedPartitionMonths(options.StartMonth, options.Months)
	var out strings.Builder
	out.WriteString("PARTITION BY RANGE COLUMNS(timeframe, start_ms) (\n")
	frames := lexicographicTimeframes()
	for frameIndex, tf := range frames {
		if hasMonthlyPartitions(tf) {
			for _, monthStart := range months {
				partitionEnd := monthStart.AddDate(0, 1, 0)
				out.WriteString(fmt.Sprintf("  PARTITION %s VALUES LESS THAN ('%s', %d),\n", partitionName(tf, monthStart), tf, partitionEnd.UnixMilli()))
			}
		}
		out.WriteString("  " + futurePartitionDDL(tf, frameIndex, frames, frameIndex != len(frames)-1) + "\n")
	}
	out.WriteString(")")
	return out.String()
}

func BuildAddTimeframePartitionsSQL(options TimeframePartitionOptions) string {
	months := normalizedPartitionMonths(options.StartMonth, options.Months)
	tableName := partitionTableName(options.TableName)
	frames := lexicographicTimeframes()
	var out strings.Builder
	for frameIndex, tf := range frames {
		if !hasMonthlyPartitions(tf) {
			continue
		}
		out.WriteString("ALTER TABLE `" + tableName + "` REORGANIZE PARTITION " + futurePartitionName(tf) + " INTO (\n")
		for _, monthStart := range months {
			partitionEnd := monthStart.AddDate(0, 1, 0)
			out.WriteString(fmt.Sprintf("  PARTITION %s VALUES LESS THAN ('%s', %d),\n", partitionName(tf, monthStart), tf, partitionEnd.UnixMilli()))
		}
		out.WriteString("  " + futurePartitionDDL(tf, frameIndex, frames, false) + "\n")
		out.WriteString(");\n")
	}
	return out.String()
}

func BuildDropExpiredTimeframePartitionsSQL(options TimeframePartitionOptions, now time.Time) string {
	months := normalizedPartitionMonths(options.StartMonth, options.Months)
	tableName := partitionTableName(options.TableName)
	var names []string
	for _, tf := range timeframe.Order {
		rule := retention.RuleFor(tf)
		cutoffMS, ok := retention.CutoffMS(rule, now)
		if !ok {
			continue
		}
		for _, monthStart := range months {
			partitionEnd := monthStart.AddDate(0, 1, 0)
			if partitionEnd.UnixMilli() <= cutoffMS {
				names = append(names, partitionName(tf, monthStart))
			}
		}
	}
	if len(names) == 0 {
		return "-- no fully expired timeframe partitions\n"
	}
	sort.Strings(names)
	return "ALTER TABLE `" + tableName + "` DROP PARTITION " + strings.Join(names, ", ") + ";\n"
}

func normalizedPartitionMonths(start time.Time, months int) []time.Time {
	if months <= 0 {
		months = 36
	}
	if start.IsZero() {
		now := time.Now().UTC()
		start = time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	}
	start = start.UTC()
	start = time.Date(start.Year(), start.Month(), 1, 0, 0, 0, 0, time.UTC)
	out := make([]time.Time, 0, months)
	for i := 0; i < months; i++ {
		out = append(out, start.AddDate(0, i, 0))
	}
	return out
}

func lexicographicTimeframes() []string {
	frames := append([]string(nil), timeframe.Order...)
	sort.Strings(frames)
	return frames
}

func partitionTableName(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "bar_history"
	}
	return value
}

func partitionName(tf string, monthStart time.Time) string {
	return fmt.Sprintf("p_tf_%s_%04d_%02d", partitionFrameName(tf), monthStart.Year(), int(monthStart.Month()))
}

func futurePartitionName(tf string) string {
	return "p_tf_" + partitionFrameName(tf) + "_future"
}

func hasMonthlyPartitions(tf string) bool {
	return !retention.RuleFor(tf).KeepForever
}

func futurePartitionDDL(tf string, frameIndex int, frames []string, trailingComma bool) string {
	suffix := ""
	if trailingComma {
		suffix = ","
	}
	if frameIndex == len(frames)-1 {
		return fmt.Sprintf("PARTITION %s VALUES LESS THAN (MAXVALUE, MAXVALUE)%s", futurePartitionName(tf), suffix)
	}
	nextFrame := frames[frameIndex+1]
	return fmt.Sprintf("PARTITION %s VALUES LESS THAN ('%s', 0)%s", futurePartitionName(tf), nextFrame, suffix)
}

func partitionFrameName(tf string) string {
	if strings.HasSuffix(tf, "m") {
		return strings.TrimSuffix(tf, "m") + "min"
	}
	if strings.HasSuffix(tf, "M") {
		return strings.TrimSuffix(tf, "M") + "mon"
	}
	return strings.ToLower(tf)
}

func barHistoryColumnsDDL() string {
	return `  exchange VARCHAR(16) NOT NULL,
  source_market VARCHAR(48) NOT NULL DEFAULT '',
  symbol VARCHAR(64) NOT NULL,
  instrument_type VARCHAR(32) NOT NULL DEFAULT '',
  asset_class VARCHAR(24) NOT NULL DEFAULT '',
  rule_type VARCHAR(24) NOT NULL DEFAULT '',
  lifecycle_phase VARCHAR(24) NOT NULL DEFAULT '',
  margin_type VARCHAR(16) NOT NULL DEFAULT '',
  timeframe VARCHAR(8) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  start_ms BIGINT NOT NULL,
  end_ms BIGINT NOT NULL,
  open_price DECIMAL(28, 12) NOT NULL,
  high_price DECIMAL(28, 12) NOT NULL,
  low_price DECIMAL(28, 12) NOT NULL,
  close_price DECIMAL(28, 12) NOT NULL,
  volume DECIMAL(30, 12) NOT NULL DEFAULT 0,
  volume_unit VARCHAR(16) NOT NULL DEFAULT '',
  quote_volume DECIMAL(30, 12) NOT NULL DEFAULT 0,
  quote_unit VARCHAR(16) NOT NULL DEFAULT '',
  contract_volume DECIMAL(30, 12) NOT NULL DEFAULT 0,
  trade_count BIGINT NOT NULL DEFAULT 0,
  prev_close DECIMAL(28, 12) NOT NULL DEFAULT 0,
  chg DECIMAL(18, 8) NOT NULL DEFAULT 0,
  amp DECIMAL(18, 8) NOT NULL DEFAULT 0,
  last_tick_ms BIGINT NOT NULL,
  is_final TINYINT(1) NOT NULL DEFAULT 0,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (exchange, symbol, timeframe, start_ms),
  KEY idx_bar_source_market (exchange, source_market, symbol, timeframe, start_ms)`
}

func adjustedBarHistoryColumnsDDL() string {
	return `  exchange VARCHAR(16) NOT NULL,
  source_market VARCHAR(48) NOT NULL DEFAULT '',
  symbol VARCHAR(64) NOT NULL,
  adj_mode VARCHAR(24) NOT NULL,
  instrument_type VARCHAR(32) NOT NULL DEFAULT '',
  asset_class VARCHAR(24) NOT NULL DEFAULT '',
  rule_type VARCHAR(24) NOT NULL DEFAULT '',
  lifecycle_phase VARCHAR(24) NOT NULL DEFAULT '',
  margin_type VARCHAR(16) NOT NULL DEFAULT '',
  timeframe VARCHAR(8) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  start_ms BIGINT NOT NULL,
  end_ms BIGINT NOT NULL,
  open_price DECIMAL(28, 12) NOT NULL,
  high_price DECIMAL(28, 12) NOT NULL,
  low_price DECIMAL(28, 12) NOT NULL,
  close_price DECIMAL(28, 12) NOT NULL,
  volume DECIMAL(30, 12) NOT NULL DEFAULT 0,
  volume_unit VARCHAR(16) NOT NULL DEFAULT '',
  quote_volume DECIMAL(30, 12) NOT NULL DEFAULT 0,
  quote_unit VARCHAR(16) NOT NULL DEFAULT '',
  contract_volume DECIMAL(30, 12) NOT NULL DEFAULT 0,
  trade_count BIGINT NOT NULL DEFAULT 0,
  prev_close DECIMAL(28, 12) NOT NULL DEFAULT 0,
  chg DECIMAL(18, 8) NOT NULL DEFAULT 0,
  amp DECIMAL(18, 8) NOT NULL DEFAULT 0,
  last_tick_ms BIGINT NOT NULL,
  is_final TINYINT(1) NOT NULL DEFAULT 0,
  adjustment_status VARCHAR(24) NOT NULL DEFAULT 'adjusted',
  adjustment_provider VARCHAR(64) NOT NULL DEFAULT '',
  adjustment_provider_version VARCHAR(64) NOT NULL DEFAULT '',
  adjustment_event_type VARCHAR(32) NOT NULL DEFAULT '',
  price_multiplier DECIMAL(28, 12) NOT NULL DEFAULT 1,
  volume_multiplier DECIMAL(28, 12) NOT NULL DEFAULT 1,
  raw_open_price DECIMAL(28, 12) NOT NULL DEFAULT 0,
  raw_high_price DECIMAL(28, 12) NOT NULL DEFAULT 0,
  raw_low_price DECIMAL(28, 12) NOT NULL DEFAULT 0,
  raw_close_price DECIMAL(28, 12) NOT NULL DEFAULT 0,
  raw_volume DECIMAL(30, 12) NOT NULL DEFAULT 0,
  raw_quote_volume DECIMAL(30, 12) NOT NULL DEFAULT 0,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (exchange, source_market, symbol, adj_mode, timeframe, start_ms),
  KEY idx_adjusted_lookup (exchange, symbol, adj_mode, timeframe, start_ms)`
}
