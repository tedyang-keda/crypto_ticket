package mysql

import (
	"strings"
	"testing"
	"time"
)

func TestBuildTimeframePartitionClause(t *testing.T) {
	sql := BuildTimeframePartitionClause(TimeframePartitionOptions{
		StartMonth: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Months:     2,
	})
	if !strings.Contains(sql, "PARTITION BY RANGE COLUMNS(timeframe, start_ms)") {
		t.Fatalf("expected timeframe partition clause: %s", sql)
	}
	if !strings.Contains(sql, "PARTITION p_tf_1min_2026_01 VALUES LESS THAN ('1m', 1769904000000)") {
		t.Fatalf("expected 1m January partition: %s", sql)
	}
	if !strings.Contains(sql, "PARTITION p_tf_3mon_future VALUES LESS THAN ('4H', 0)") {
		t.Fatalf("expected lexicographic future partition boundary: %s", sql)
	}
	if strings.Contains(sql, "p_tf_1mon_2026_01") {
		t.Fatalf("did not expect month partitions for keep-forever 1M: %s", sql)
	}
}

func TestBuildDropExpiredTimeframePartitionsSQL(t *testing.T) {
	sql := BuildDropExpiredTimeframePartitionsSQL(
		TimeframePartitionOptions{
			TableName:  "bar_history",
			StartMonth: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			Months:     6,
		},
		time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC),
	)
	if !strings.Contains(sql, "p_tf_1min_2026_04") {
		t.Fatalf("expected expired 1m April partition: %s", sql)
	}
	if strings.Contains(sql, "p_tf_1d_") {
		t.Fatalf("did not expect keep-forever 1D partitions: %s", sql)
	}
}

func TestBuildAddTimeframePartitionsSQL(t *testing.T) {
	sql := BuildAddTimeframePartitionsSQL(TimeframePartitionOptions{
		TableName:  "bar_history",
		StartMonth: time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
		Months:     1,
	})
	if !strings.Contains(sql, "REORGANIZE PARTITION p_tf_1min_future") {
		t.Fatalf("expected 1m future partition reorganize: %s", sql)
	}
	if !strings.Contains(sql, "PARTITION p_tf_1min_2027_01 VALUES LESS THAN ('1m', 1801440000000)") {
		t.Fatalf("expected 1m January 2027 partition: %s", sql)
	}
	if !strings.Contains(sql, "PARTITION p_tf_1min_future VALUES LESS THAN ('2D', 0)") {
		t.Fatalf("expected 1m future boundary: %s", sql)
	}
	if strings.Contains(sql, "REORGANIZE PARTITION p_tf_1mon_future") {
		t.Fatalf("did not expect add SQL for keep-forever 1M: %s", sql)
	}
}

func TestCreateBarHistoryTableStatementUsesTimeframePartitions(t *testing.T) {
	sql := createBarHistoryTableStatement(time.Date(2026, 6, 4, 0, 0, 0, 0, time.UTC))
	if !strings.Contains(sql, "CREATE TABLE IF NOT EXISTS bar_history") {
		t.Fatalf("expected create table statement: %s", sql)
	}
	if !strings.Contains(sql, "PARTITION BY RANGE COLUMNS(timeframe, start_ms)") {
		t.Fatalf("expected timeframe partitioning: %s", sql)
	}
	if strings.Contains(sql, "idx_bar_lookup") {
		t.Fatalf("did not expect duplicate lookup index: %s", sql)
	}
}
