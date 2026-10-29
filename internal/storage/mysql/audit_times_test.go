package mysql

import (
	"strings"
	"testing"
)

func TestTimeAuditSQLIsReadOnlyPartitionPrunablePrimaryStream(t *testing.T) {
	for _, symbols := range [][]string{nil, {"BTCUSDT", "ETHUSDT"}} {
		q := timeAuditSQL(symbols)
		upper := strings.ToUpper(q)
		if !strings.HasPrefix(upper, "SELECT ") || !strings.Contains(q, "FORCE INDEX (PRIMARY)") || !strings.Contains(q, "exchange=? AND timeframe=?") || !strings.HasSuffix(q, "ORDER BY symbol,start_ms") {
			t.Fatalf("query=%s", q)
		}
		for _, bad := range []string{"INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP", " LAG(", " OVER(", "GROUP BY", "OPEN_PRICE", "VOLUME", ";"} {
			if strings.Contains(upper, bad) {
				t.Fatalf("forbidden %q in %s", bad, q)
			}
		}
		if got, want := strings.Count(q, "?"), 3+len(symbols); got != want {
			t.Errorf("placeholders=%d want=%d", got, want)
		}
	}
}
