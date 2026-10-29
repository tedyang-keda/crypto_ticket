package mysql

import (
	"context"
	"fmt"
	"math"
	"strings"

	"crypto-ticket/internal/market"
	"crypto-ticket/internal/timeframe"
)

const insertMissingBarSQL = `INSERT INTO bar_history
	(exchange, symbol, margin_type, timeframe, start_ms, end_ms, open_price, high_price, low_price, close_price,
	 volume, volume_unit, quote_volume, quote_unit, contract_volume, trade_count, prev_close, chg, amp, last_tick_ms, is_final)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON DUPLICATE KEY UPDATE start_ms=start_ms`

// InsertMissingBars inserts final history bars without changing any existing
// primary key, even when that key belongs to a non-final bar. The batch commits
// atomically; a failed batch returns zero. It never creates or migrates tables.
// Accurate inserted-row counts require MySQL's default clientFoundRows=false.
func (s *Store) InsertMissingBars(ctx context.Context, bars []market.Bar) (int, error) {
	if ctx == nil {
		return 0, fmt.Errorf("insert missing bars: nil context")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if len(bars) == 0 {
		return 0, nil
	}
	if s == nil || s.db == nil {
		return 0, fmt.Errorf("insert missing bars: nil database")
	}
	for i, bar := range bars {
		if err := validateMissingBar(bar); err != nil {
			return 0, fmt.Errorf("insert missing bars: bar %d: %w", i, err)
		}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("insert missing bars: begin: %w", err)
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, insertMissingBarSQL)
	if err != nil {
		return 0, fmt.Errorf("insert missing bars: prepare: %w", err)
	}
	defer stmt.Close()

	inserted := 0
	for i, bar := range bars {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		result, err := stmt.ExecContext(ctx, barArgs(bar)...)
		if err != nil {
			return 0, fmt.Errorf("insert missing bars: bar %d: %w", i, err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return 0, fmt.Errorf("insert missing bars: bar %d affected rows: %w", i, err)
		}
		if affected < 0 || affected > 1 {
			return 0, fmt.Errorf("insert missing bars: bar %d unexpected affected rows %d", i, affected)
		}
		inserted += int(affected)
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("insert missing bars: commit: %w", err)
	}
	return inserted, nil
}

func validateMissingBar(bar market.Bar) error {
	if strings.TrimSpace(bar.Exchange) == "" || strings.TrimSpace(bar.Symbol) == "" {
		return fmt.Errorf("exchange and symbol are required")
	}
	if _, err := timeframe.Normalize(bar.Timeframe); err != nil {
		return err
	}
	if bar.StartMS <= 0 || bar.EndMS < bar.StartMS {
		return fmt.Errorf("invalid time range")
	}
	if !bar.IsFinal {
		return fmt.Errorf("history bar must be final")
	}
	values := []float64{
		bar.OpenPrice, bar.HighPrice, bar.LowPrice, bar.ClosePrice,
		bar.Volume, bar.QuoteVolume, bar.ContractVolume, bar.PrevClose, bar.Chg, bar.Amp,
	}
	for _, value := range values {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return fmt.Errorf("bar contains a non-finite number")
		}
	}
	return nil
}
