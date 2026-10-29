package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"crypto-ticket/internal/market"
	"crypto-ticket/internal/storage"
)

const minuteVerificationColumnDDL = `ALTER TABLE bar_history ADD COLUMN verified_at_ms BIGINT NOT NULL DEFAULT 0, ALGORITHM=INSTANT`

// Instant-only migration deliberately fails instead of rebuilding the large
// partitioned history table when the running MySQL cannot add it instantly.
func (s *Store) ensureMinuteVerificationColumn(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema=DATABASE() AND table_name='bar_history' AND column_name='verified_at_ms'`).Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		return nil
	}
	_, err := s.db.ExecContext(ctx, minuteVerificationColumnDDL)
	return err
}

// barUpsertSQL(false) is used by ordinary writers. A calibrated minute can
// only be changed through the explicit revision path, never by Source=rest.
func barUpsertSQL(allowVerified bool) string {
	columns := []string{
		"margin_type", "end_ms", "open_price", "high_price", "low_price", "close_price",
		"volume", "volume_unit", "quote_volume", "quote_unit", "contract_volume", "trade_count",
		"prev_close", "chg", "amp", "last_tick_ms", "is_final",
	}
	updates := make([]string, 0, len(columns))
	for _, column := range columns {
		value := "VALUES(" + column + ")"
		if !allowVerified {
			value = "IF(timeframe='1m' AND verified_at_ms>0," + column + "," + value + ")"
		}
		updates = append(updates, column+"="+value)
	}
	return `INSERT INTO bar_history
		(exchange,symbol,margin_type,timeframe,start_ms,end_ms,open_price,high_price,low_price,close_price,
		 volume,volume_unit,quote_volume,quote_unit,contract_volume,trade_count,prev_close,chg,amp,last_tick_ms,is_final)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE ` + strings.Join(updates, ",")
}

func (s *Store) LoadMinuteVerifications(ctx context.Context, exchange, symbol string, startMS, endMS int64) ([]storage.VerifiedMinuteBar, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT exchange,symbol,margin_type,timeframe,start_ms,end_ms,
		open_price,high_price,low_price,close_price,volume,volume_unit,quote_volume,quote_unit,contract_volume,
		trade_count,prev_close,chg,amp,last_tick_ms,is_final,verified_at_ms FROM bar_history
		WHERE exchange=? AND symbol=? AND timeframe='1m' AND start_ms>=? AND start_ms<=? AND is_final=1
		ORDER BY start_ms`, strings.ToLower(exchange), strings.ToUpper(symbol), startMS, endMS)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []storage.VerifiedMinuteBar
	for rows.Next() {
		var row storage.VerifiedMinuteBar
		bar := &row.Bar
		if err := rows.Scan(&bar.Exchange, &bar.Symbol, &bar.MarginType, &bar.Timeframe, &bar.StartMS, &bar.EndMS,
			&bar.OpenPrice, &bar.HighPrice, &bar.LowPrice, &bar.ClosePrice, &bar.Volume, &bar.VolumeUnit,
			&bar.QuoteVolume, &bar.QuoteUnit, &bar.ContractVolume, &bar.TradeCount, &bar.PrevClose,
			&bar.Chg, &bar.Amp, &bar.LastTickMS, &bar.IsFinal, &row.VerifiedAtMS); err != nil {
			return nil, err
		}
		bar.Source = "mysql"
		row.Bar = market.DecorateBar(*bar)
		result = append(result, row)
	}
	return result, rows.Err()
}

func (s *Store) UpsertMinuteRevisions(ctx context.Context, bars []market.Bar, generationMS int64) ([]market.Bar, error) {
	if generationMS <= 0 {
		return nil, fmt.Errorf("minute verification generation must be positive")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	accepted := make([]market.Bar, 0, len(bars))
	for _, bar := range bars {
		if bar.Timeframe != "1m" || !bar.IsFinal {
			return nil, fmt.Errorf("minute revision requires a final 1m bar")
		}
		var verifiedAtMS int64
		err := tx.QueryRowContext(ctx, `SELECT verified_at_ms FROM bar_history
			WHERE exchange=? AND symbol=? AND timeframe='1m' AND start_ms=? FOR UPDATE`,
			strings.ToLower(bar.Exchange), strings.ToUpper(bar.Symbol), bar.StartMS).Scan(&verifiedAtMS)
		if err != nil && err != sql.ErrNoRows {
			return nil, err
		}
		if verifiedAtMS > generationMS {
			continue
		}
		if _, err := tx.ExecContext(ctx, barUpsertSQL(true), barArgs(bar)...); err != nil {
			return nil, err
		}
		accepted = append(accepted, bar)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return accepted, nil
}

func (s *Store) MarkMinutesVerified(ctx context.Context, bars []market.Bar, generationMS int64) error {
	if generationMS <= 0 {
		return fmt.Errorf("minute verification generation must be positive")
	}
	// Group only the supplied keys; never mark the unreturned holes of a
	// partially successful REST request as verified.
	bySymbol := make(map[string][]market.Bar)
	exchanges := make(map[string]string)
	symbols := make(map[string]string)
	for _, bar := range bars {
		if bar.Timeframe != "1m" || !bar.IsFinal {
			return fmt.Errorf("minute verification requires a final 1m bar")
		}
		key := strings.ToLower(bar.Exchange) + ":" + strings.ToUpper(bar.Symbol)
		bySymbol[key] = append(bySymbol[key], bar)
		exchanges[key], symbols[key] = strings.ToLower(bar.Exchange), strings.ToUpper(bar.Symbol)
	}
	for key, minutes := range bySymbol {
		for offset := 0; offset < len(minutes); offset += 100 {
			end := offset + 100
			if end > len(minutes) {
				end = len(minutes)
			}
			args := []any{generationMS, exchanges[key], symbols[key]}
			conditions := make([]string, 0, end-offset)
			for _, bar := range minutes[offset:end] {
				condition, values := minuteVerificationCondition(bar)
				conditions = append(conditions, condition)
				args = append(args, values...)
			}
			predicate := ` WHERE exchange=? AND symbol=? AND timeframe='1m' AND is_final=1 AND (` + strings.Join(conditions, " OR ") + `)`
			_, err := s.db.ExecContext(ctx, `UPDATE bar_history SET verified_at_ms=GREATEST(verified_at_ms,?)`+predicate, args...)
			if err != nil {
				return err
			}
			// A direct maintenance writer may bypass the application's symbol
			// lock. Do not bless its changed value using an earlier REST result.
			var verified int
			checkArgs := append([]any(nil), args[1:]...)
			checkArgs = append(checkArgs, generationMS)
			if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM bar_history`+predicate+` AND verified_at_ms>=?`, checkArgs...).Scan(&verified); err != nil {
				return err
			}
			if verified != end-offset {
				return fmt.Errorf("minute values changed before verification: %s matched=%d expected=%d", key, verified, end-offset)
			}
		}
	}
	return nil
}

func minuteVerificationCondition(bar market.Bar) (string, []any) {
	conditions := []string{"start_ms=?", "end_ms=?"}
	args := []any{bar.StartMS, bar.EndMS}
	for _, field := range []struct {
		column string
		value  float64
	}{
		{"open_price", bar.OpenPrice}, {"high_price", bar.HighPrice}, {"low_price", bar.LowPrice},
		{"close_price", bar.ClosePrice}, {"volume", bar.Volume}, {"quote_volume", bar.QuoteVolume},
		{"contract_volume", bar.ContractVolume},
	} {
		conditions = append(conditions, "ABS("+field.column+"-?)<=GREATEST(0.00000001,ABS("+field.column+")*0.00000001)")
		args = append(args, field.value)
	}
	if market.OfficialTradeCountAvailable(bar) {
		conditions = append(conditions, "trade_count=?")
		args = append(args, bar.TradeCount)
	}
	return "(" + strings.Join(conditions, " AND ") + ")", args
}
