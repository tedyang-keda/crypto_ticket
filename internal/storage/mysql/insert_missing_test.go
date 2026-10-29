package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"crypto-ticket/internal/market"
)

func TestInsertMissingBarsLeavesExistingFinalAndNonFinalRowsUntouched(t *testing.T) {
	store, state := newInsertMissingTestStore(t)
	existingFinal := missingTestBar(1_700_000_000_000)
	existingNonFinal := missingTestBar(existingFinal.StartMS + 60_000)
	existingNonFinal.IsFinal = false
	for _, bar := range []market.Bar{existingFinal, existingNonFinal} {
		values := missingTestValues(bar)
		state.rows[missingTestKey(values)] = values
	}
	beforeFinal := append([]driver.Value(nil), state.rows[missingTestKey(missingTestValues(existingFinal))]...)
	beforeNonFinal := append([]driver.Value(nil), state.rows[missingTestKey(missingTestValues(existingNonFinal))]...)
	existingFinal.ClosePrice = 999
	existingNonFinal.IsFinal = true
	existingNonFinal.ClosePrice = 888
	newBar := missingTestBar(existingNonFinal.StartMS + 60_000)
	newBar.Exchange = "BINANCE"
	newBar.Symbol = "btcusdt"

	inserted, err := store.InsertMissingBars(context.Background(), []market.Bar{existingFinal, existingNonFinal, newBar, newBar})
	if err != nil || inserted != 1 {
		t.Fatalf("inserted=%d err=%v", inserted, err)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if len(state.rows) != 3 || !reflect.DeepEqual(state.rows[missingTestKey(beforeFinal)], beforeFinal) ||
		!reflect.DeepEqual(state.rows[missingTestKey(beforeNonFinal)], beforeNonFinal) {
		t.Fatalf("existing rows were changed: %+v", state.rows)
	}
	if state.commits != 1 || state.rollbacks != 0 || state.prepares != 1 {
		t.Fatalf("unexpected transaction lifecycle: commits=%d rollbacks=%d prepares=%d", state.commits, state.rollbacks, state.prepares)
	}
	if state.query != insertMissingBarSQL || strings.Contains(strings.ToUpper(state.query), "IGNORE") ||
		strings.Count(state.query, "?") != len(barArgs(newBar)) ||
		!strings.HasSuffix(strings.TrimSpace(state.query), "ON DUPLICATE KEY UPDATE start_ms=start_ms") {
		t.Fatalf("unexpected insert-only SQL: %s", state.query)
	}
	if got := state.rows[missingTestKey(missingTestValues(newBar))]; !reflect.DeepEqual(got, missingTestValues(newBar)) {
		t.Fatalf("barArgs order or normalization changed: %v", got)
	}
}

func TestInsertMissingBarsRollsBackFailedBatch(t *testing.T) {
	wantErr := errors.New("database failure")
	for _, stage := range []string{"begin", "prepare", "exec", "affected", "affected-count", "commit"} {
		t.Run(stage, func(t *testing.T) {
			store, state := newInsertMissingTestStore(t)
			switch stage {
			case "begin":
				state.beginErr = wantErr
			case "prepare":
				state.prepareErr = wantErr
			case "exec":
				state.execErrAt, state.execErr = 2, wantErr
			case "affected":
				state.affectedErr = wantErr
			case "affected-count":
				state.affectedOverride = 2
			case "commit":
				state.commitErr = wantErr
			}
			bars := []market.Bar{missingTestBar(1_700_000_000_000), missingTestBar(1_700_000_060_000)}
			inserted, err := store.InsertMissingBars(context.Background(), bars)
			if inserted != 0 || err == nil || (stage != "affected-count" && !errors.Is(err, wantErr)) {
				t.Fatalf("inserted=%d err=%v", inserted, err)
			}
			state.mu.Lock()
			defer state.mu.Unlock()
			if len(state.rows) != 0 {
				t.Fatalf("failed batch persisted rows: %+v", state.rows)
			}
			if stage != "begin" && stage != "commit" && state.rollbacks != 1 {
				t.Fatalf("expected rollback, got %d", state.rollbacks)
			}
		})
	}
}

func TestInsertMissingBarsValidatesEntireBatchBeforeBeginning(t *testing.T) {
	cases := map[string]func(*market.Bar){
		"exchange":  func(b *market.Bar) { b.Exchange = " " },
		"symbol":    func(b *market.Bar) { b.Symbol = "" },
		"timeframe": func(b *market.Bar) { b.Timeframe = "1h" },
		"start":     func(b *market.Bar) { b.StartMS = 0 },
		"end":       func(b *market.Bar) { b.EndMS = b.StartMS - 1 },
		"non-final": func(b *market.Bar) { b.IsFinal = false },
		"nan":       func(b *market.Bar) { b.Volume = math.NaN() },
		"infinity":  func(b *market.Bar) { b.ClosePrice = math.Inf(1) },
	}
	for name, invalidate := range cases {
		t.Run(name, func(t *testing.T) {
			store, state := newInsertMissingTestStore(t)
			valid := missingTestBar(1_700_000_000_000)
			invalid := valid
			invalidate(&invalid)
			inserted, err := store.InsertMissingBars(context.Background(), []market.Bar{valid, invalid})
			if err == nil || inserted != 0 || state.begins != 0 {
				t.Fatalf("inserted=%d err=%v begins=%d", inserted, err, state.begins)
			}
		})
	}
}

func TestInsertMissingBarsHandlesEmptyContextAndMissingDatabase(t *testing.T) {
	store, state := newInsertMissingTestStore(t)
	if count, err := store.InsertMissingBars(context.Background(), nil); count != 0 || err != nil || state.begins != 0 {
		t.Fatalf("empty batch: count=%d err=%v begins=%d", count, err, state.begins)
	}
	if _, err := store.InsertMissingBars(nil, nil); err == nil {
		t.Fatal("nil context must fail")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if count, err := store.InsertMissingBars(ctx, []market.Bar{missingTestBar(1_700_000_000_000)}); count != 0 || !errors.Is(err, context.Canceled) || state.begins != 0 {
		t.Fatalf("canceled context: count=%d err=%v begins=%d", count, err, state.begins)
	}
	for _, unavailable := range []*Store{nil, {}} {
		if _, err := unavailable.InsertMissingBars(context.Background(), []market.Bar{missingTestBar(1_700_000_000_000)}); err == nil {
			t.Fatal("missing database must fail")
		}
	}
}

func TestInsertMissingBarsCancellationRollsBack(t *testing.T) {
	store, state := newInsertMissingTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	state.afterExec = cancel
	count, err := store.InsertMissingBars(ctx, []market.Bar{missingTestBar(1_700_000_000_000), missingTestBar(1_700_000_060_000)})
	if count != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("count=%d err=%v", count, err)
	}
	select {
	case <-state.rolledBack:
	case <-time.After(time.Second):
		t.Fatal("canceled transaction was not rolled back")
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if len(state.rows) != 0 || state.commits != 0 || state.execs != 1 {
		t.Fatalf("canceled batch was persisted: rows=%d commits=%d execs=%d", len(state.rows), state.commits, state.execs)
	}
}

func missingTestBar(start int64) market.Bar {
	return market.Bar{
		Exchange: "binance", Symbol: "BTCUSDT", MarginType: "umargin", Timeframe: "1m",
		StartMS: start, EndMS: start + 59_999,
		OpenPrice: 100, HighPrice: 110, LowPrice: 90, ClosePrice: 105,
		Volume: 1, QuoteVolume: 100, VolumeUnit: "BTC", QuoteUnit: "USDT",
		ContractVolume: 2, TradeCount: 3, PrevClose: 99, Chg: 6.060606, Amp: 22.222222,
		LastTickMS: start + 59_999, IsFinal: true,
	}
}

func missingTestValues(bar market.Bar) []driver.Value {
	args := barArgs(bar)
	values := make([]driver.Value, len(args))
	for i, arg := range args {
		values[i], _ = driver.DefaultParameterConverter.ConvertValue(arg)
	}
	return values
}

func missingTestKey(values []driver.Value) string {
	return values[0].(string) + ":" + values[1].(string) + ":" + values[3].(string) + ":" + strconv.FormatInt(values[4].(int64), 10)
}

type insertMissingTestState struct {
	mu               sync.Mutex
	rows             map[string][]driver.Value
	query            string
	begins           int
	prepares         int
	execs            int
	commits          int
	rollbacks        int
	beginErr         error
	prepareErr       error
	execErr          error
	execErrAt        int
	affectedErr      error
	affectedOverride int64
	commitErr        error
	afterExec        func()
	rolledBack       chan struct{}
}

func newInsertMissingTestStore(t *testing.T) (*Store, *insertMissingTestState) {
	t.Helper()
	state := &insertMissingTestState{rows: make(map[string][]driver.Value), rolledBack: make(chan struct{}, 1)}
	db := sql.OpenDB(insertMissingTestConnector{state: state})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return &Store{db: db}, state
}

type insertMissingTestConnector struct{ state *insertMissingTestState }

func (c insertMissingTestConnector) Connect(context.Context) (driver.Conn, error) {
	return &insertMissingTestConn{state: c.state}, nil
}
func (c insertMissingTestConnector) Driver() driver.Driver { return insertMissingTestDriver{} }

type insertMissingTestDriver struct{}

func (insertMissingTestDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("test driver requires connector")
}

type insertMissingTestConn struct {
	state *insertMissingTestState
	tx    *insertMissingTestTx
}

func (c *insertMissingTestConn) Close() error { return nil }
func (c *insertMissingTestConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}
func (c *insertMissingTestConn) BeginTx(ctx context.Context, _ driver.TxOptions) (driver.Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	c.state.begins++
	if c.state.beginErr != nil {
		return nil, c.state.beginErr
	}
	c.tx = &insertMissingTestTx{state: c.state, pending: make(map[string][]driver.Value)}
	return c.tx, nil
}
func (c *insertMissingTestConn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}
func (c *insertMissingTestConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	c.state.prepares++
	c.state.query = query
	if c.state.prepareErr != nil {
		return nil, c.state.prepareErr
	}
	return &insertMissingTestStmt{tx: c.tx}, nil
}

type insertMissingTestTx struct {
	state   *insertMissingTestState
	pending map[string][]driver.Value
}

func (tx *insertMissingTestTx) Commit() error {
	tx.state.mu.Lock()
	defer tx.state.mu.Unlock()
	tx.state.commits++
	if tx.state.commitErr != nil {
		return tx.state.commitErr
	}
	for key, values := range tx.pending {
		tx.state.rows[key] = values
	}
	return nil
}
func (tx *insertMissingTestTx) Rollback() error {
	tx.state.mu.Lock()
	defer tx.state.mu.Unlock()
	tx.state.rollbacks++
	select {
	case tx.state.rolledBack <- struct{}{}:
	default:
	}
	return nil
}

type insertMissingTestStmt struct{ tx *insertMissingTestTx }

func (*insertMissingTestStmt) Close() error  { return nil }
func (*insertMissingTestStmt) NumInput() int { return 21 }
func (*insertMissingTestStmt) Query([]driver.Value) (driver.Rows, error) {
	return nil, errors.New("unexpected query")
}
func (s *insertMissingTestStmt) Exec(values []driver.Value) (driver.Result, error) {
	state := s.tx.state
	state.mu.Lock()
	defer state.mu.Unlock()
	state.execs++
	if state.execErr != nil && state.execs == state.execErrAt {
		return nil, state.execErr
	}
	key := missingTestKey(values)
	affected := int64(0)
	if _, exists := state.rows[key]; !exists {
		if _, pending := s.tx.pending[key]; !pending {
			s.tx.pending[key] = append([]driver.Value(nil), values...)
			affected = 1
		}
	}
	if state.afterExec != nil {
		state.afterExec()
	}
	if state.affectedOverride != 0 {
		affected = state.affectedOverride
	}
	return insertMissingTestResult{affected: affected, err: state.affectedErr}, nil
}

type insertMissingTestResult struct {
	affected int64
	err      error
}

func (r insertMissingTestResult) LastInsertId() (int64, error) { return 0, nil }
func (r insertMissingTestResult) RowsAffected() (int64, error) { return r.affected, r.err }
