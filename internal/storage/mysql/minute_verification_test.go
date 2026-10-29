package mysql

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"

	"crypto-ticket/internal/market"
)

func TestMinuteVerificationMySQLIntegration(t *testing.T) {
	dsn := os.Getenv("CRYPTO_TICKET_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("dedicated MySQL test DSN is not configured")
	}
	config, err := mysqldriver.ParseDSN(dsn)
	if err != nil || !strings.Contains(config.DBName, "_guardian_test_") {
		t.Fatal("refusing verification integration outside an isolated _guardian_test_ database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := New(dsn)
	if err != nil {
		t.Fatal("open dedicated MySQL database failed")
	}
	defer store.Close()
	// A fresh test table initially lacks the field, exercising the same
	// instant-only alteration used for the existing production table.
	ddl := strings.Replace(barHistoryColumnsDDL(), "  verified_at_ms BIGINT NOT NULL DEFAULT 0,\n", "", 1)
	if _, err := store.db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS bar_history ("+ddl+") PARTITION BY RANGE(start_ms) (PARTITION p_test VALUES LESS THAN MAXVALUE)"); err != nil {
		t.Fatal(err)
	}
	if err := store.ensureMinuteVerificationColumn(ctx); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Truncate(time.Minute).Add(-time.Hour).UnixMilli()
	bar := market.Bar{Exchange: "binance", Symbol: "VERIFICATION_INTEGRATION", Timeframe: "1m",
		StartMS: base, EndMS: base + 59_999, OpenPrice: 100, HighPrice: 102, LowPrice: 99,
		ClosePrice: 101, Volume: 1, QuoteVolume: 100, TradeCount: 3, LastTickMS: base + 59_999, IsFinal: true}
	if err := store.UpsertBars(ctx, []market.Bar{bar}); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkMinutesVerified(ctx, []market.Bar{bar}, 1000); err != nil {
		t.Fatal(err)
	}
	conflict := bar
	conflict.ClosePrice, conflict.HighPrice = 120, 120
	if err := store.UpsertBars(ctx, []market.Bar{conflict}); err != nil {
		t.Fatal(err)
	}
	rows, err := store.LoadMinuteVerifications(ctx, bar.Exchange, bar.Symbol, base, base)
	if err != nil || len(rows) != 1 || rows[0].Bar.ClosePrice != 101 || rows[0].VerifiedAtMS != 1000 {
		t.Fatalf("ordinary writer bypassed verification gate: %+v %v", rows, err)
	}
	accepted, err := store.UpsertMinuteRevisions(ctx, []market.Bar{conflict}, 2000)
	if err != nil || len(accepted) != 1 {
		t.Fatalf("new official revision rejected: %+v %v", accepted, err)
	}
	if err := store.MarkMinutesVerified(ctx, accepted, 2000); err != nil {
		t.Fatal(err)
	}
	accepted, err = store.UpsertMinuteRevisions(ctx, []market.Bar{bar}, 1500)
	if err != nil || len(accepted) != 0 {
		t.Fatalf("older official request accepted: %+v %v", accepted, err)
	}
	rows, err = store.LoadMinuteVerifications(ctx, bar.Exchange, bar.Symbol, base, base)
	if err != nil || len(rows) != 1 || rows[0].Bar.ClosePrice != 120 || rows[0].VerifiedAtMS != 2000 {
		t.Fatalf("revision or generation was not persisted: %+v %v", rows, err)
	}
	if err := store.MarkMinutesVerified(ctx, []market.Bar{bar}, 3000); err == nil {
		t.Fatal("marker blessed values that differed from the verified REST response")
	}
	rows, err = store.LoadMinuteVerifications(ctx, bar.Exchange, bar.Symbol, base, base)
	if err != nil || len(rows) != 1 || rows[0].VerifiedAtMS != 2000 {
		t.Fatalf("failed CAS advanced verification marker: %+v %v", rows, err)
	}
}

func TestMinuteVerificationSQLComparesBinanceOfficialZeroTradeCount(t *testing.T) {
	for _, exchange := range []string{"binance", "okx"} {
		predicate, _ := minuteVerificationCondition(market.Bar{Exchange: exchange, TradeCount: 0})
		if got, want := strings.Contains(predicate, "trade_count=?"), exchange == "binance"; got != want {
			t.Fatalf("%s zero-count predicate: got constrained=%t want %t", exchange, got, want)
		}
	}
}
