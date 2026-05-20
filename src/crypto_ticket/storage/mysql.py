from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from decimal import Decimal
from typing import Any, Iterable, Iterator, Optional

import pymysql

from ..models import BarEvent, SymbolInfo, TickEvent


class MySQLHotStore:
    def __init__(
        self,
        *,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        schema_path: Optional[str] = None,
    ):
        self.conn_kwargs = {
            "host": host,
            "port": int(port),
            "user": user,
            "password": password,
            "database": database,
            "charset": "utf8mb4",
            "autocommit": True,
            "cursorclass": pymysql.cursors.Cursor,
        }
        self.schema_path = Path(schema_path) if schema_path else None

    def connect(self):
        return pymysql.connect(**self.conn_kwargs)

    def ensure_schema(self) -> None:
        if not self.schema_path or not self.schema_path.exists():
            return
        sql = self.schema_path.read_text(encoding="utf-8")
        statements = [statement.strip() for statement in sql.split(";") if statement.strip()]
        with self.connect() as conn:
            with conn.cursor() as cursor:
                for statement in statements:
                    cursor.execute(statement)

    def list_symbol_exchanges(self) -> list[str]:
        sql = """
            SELECT DISTINCT exchange
            FROM symbol_registry
            ORDER BY exchange
        """
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                rows = cursor.fetchall()
        return [str(row[0]) for row in rows]

    def list_symbols(self, exchange: str, *, active_only: Optional[bool] = None) -> list[dict[str, Any]]:
        filters = ["exchange = %s"]
        params: list[Any] = [exchange]
        if active_only is True:
            filters.append("is_active = 1")
        elif active_only is False:
            filters.append("is_active = 0")

        sql = f"""
            SELECT exchange, symbol, market_type, is_active, first_seen_at_ms, last_seen_at_ms, last_status, created_at, updated_at
            FROM symbol_registry
            WHERE {' AND '.join(filters)}
            ORDER BY is_active DESC, symbol
        """
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                rows = cursor.fetchall()
                columns = [column[0] for column in cursor.description or []]
        return [self._row_to_dict(columns, row) for row in rows]

    def get_symbol_registry(self, exchange: str, symbol: str) -> Optional[dict[str, Any]]:
        sql = """
            SELECT exchange, symbol, market_type, is_active, first_seen_at_ms, last_seen_at_ms, last_status, created_at, updated_at
            FROM symbol_registry
            WHERE exchange = %s AND symbol = %s
            LIMIT 1
        """
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (exchange, symbol))
                row = cursor.fetchone()
                columns = [column[0] for column in cursor.description or []]
        return self._row_to_dict(columns, row) if row else None

    def get_bar_checkpoint(self, exchange: str, symbol: str, timeframe: str) -> Optional[dict[str, Any]]:
        sql = """
            SELECT exchange, symbol, timeframe, start_ms, end_ms, open_price, high_price, low_price,
                   close_price, volume, quote_volume, trade_count, last_tick_ms, is_final, created_at, updated_at
            FROM bar_checkpoint
            WHERE exchange = %s AND symbol = %s AND timeframe = %s
            LIMIT 1
        """
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (exchange, symbol, timeframe))
                row = cursor.fetchone()
                columns = [column[0] for column in cursor.description or []]
        return self._row_to_dict(columns, row) if row else None

    def upsert_symbol_registry(self, symbols: Iterable[SymbolInfo]) -> int:
        rows = []
        for symbol in symbols:
            rows.append(
                (
                    symbol.exchange,
                    symbol.symbol,
                    symbol.market_type,
                    1 if symbol.is_active else 0,
                    int(symbol.raw.get("first_seen_at_ms", 0) or 0),
                    int(symbol.raw.get("last_seen_at_ms", 0) or 0),
                    symbol.status,
                    json.dumps(symbol.to_dict(), ensure_ascii=False, separators=(",", ":")),
                )
            )
        if not rows:
            return 0
        sql = """
            INSERT INTO symbol_registry
            (exchange, symbol, market_type, is_active, first_seen_at_ms, last_seen_at_ms, last_status, raw_json)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              market_type = VALUES(market_type),
              is_active = VALUES(is_active),
              first_seen_at_ms = LEAST(first_seen_at_ms, VALUES(first_seen_at_ms)),
              last_seen_at_ms = GREATEST(last_seen_at_ms, VALUES(last_seen_at_ms)),
              last_status = VALUES(last_status),
              raw_json = VALUES(raw_json)
        """
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(sql, rows)
        return len(rows)

    def upsert_latest_quote(self, tick: TickEvent) -> None:
        sql = """
            INSERT INTO latest_quote
            (exchange, symbol, ts_ms, price, size, side, raw_json)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              ts_ms = VALUES(ts_ms),
              price = VALUES(price),
              size = VALUES(size),
              side = VALUES(side),
              raw_json = VALUES(raw_json)
        """
        payload = json.dumps(tick.to_dict(), ensure_ascii=False, separators=(",", ":"))
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    sql,
                    (
                        tick.exchange,
                        tick.symbol,
                        int(tick.ts_ms),
                        tick.price,
                        tick.size,
                        tick.side,
                        payload,
                    ),
                )

    def upsert_bar_checkpoint(self, bar: BarEvent) -> None:
        self.upsert_bar_checkpoints([bar])

    def list_recent_bars(self, exchange: str, symbol: str, timeframe: str, *, limit: int = 400) -> list[dict[str, Any]]:
        row_limit = max(1, min(int(limit), 5000))
        sql = """
            SELECT exchange, symbol, timeframe, start_ms, end_ms, open_price, high_price, low_price,
                   close_price, volume, quote_volume, trade_count, last_tick_ms, is_final, created_at, updated_at
            FROM bar_history FORCE INDEX (PRIMARY)
            WHERE exchange = %s AND symbol = %s AND timeframe = %s
            ORDER BY start_ms DESC
            LIMIT %s
        """
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (exchange, symbol, timeframe, row_limit))
                rows = cursor.fetchall()
                columns = [column[0] for column in cursor.description or []]
        normalized = [self._row_to_dict(columns, row) for row in rows]
        normalized.reverse()
        return normalized

    def iter_history_bars(
        self,
        *,
        timeframe: str = "1m",
        exchange: Optional[str] = None,
        symbol: Optional[str] = None,
    ) -> Iterator[BarEvent]:
        filters = ["timeframe = %s"]
        params: list[Any] = [timeframe]
        if exchange:
            filters.append("exchange = %s")
            params.append(exchange)
        if symbol:
            filters.append("symbol = %s")
            params.append(symbol)

        sql = f"""
            SELECT exchange, symbol, timeframe, start_ms, end_ms, open_price, high_price, low_price,
                   close_price, volume, quote_volume, trade_count, last_tick_ms, is_final
            FROM bar_history FORCE INDEX (PRIMARY)
            WHERE {' AND '.join(filters)}
            ORDER BY exchange, symbol, start_ms
        """
        with self.connect() as conn:
            with conn.cursor(pymysql.cursors.SSCursor) as cursor:
                cursor.execute(sql, params)
                for row in cursor:
                    yield self._bar_event_from_history_row(row)

    def upsert_bar_checkpoints(self, bars: Iterable[BarEvent], *, batch_size: int = 500) -> int:
        rows = [self._bar_checkpoint_row(bar) for bar in bars or []]
        if not rows:
            return 0

        sql = """
            INSERT INTO bar_checkpoint
            (exchange, symbol, timeframe, start_ms, end_ms, open_price, high_price, low_price, close_price,
             volume, quote_volume, trade_count, last_tick_ms, is_final, raw_json)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              start_ms = VALUES(start_ms),
              end_ms = VALUES(end_ms),
              open_price = VALUES(open_price),
              high_price = VALUES(high_price),
              low_price = VALUES(low_price),
              close_price = VALUES(close_price),
              volume = VALUES(volume),
              quote_volume = VALUES(quote_volume),
              trade_count = VALUES(trade_count),
              last_tick_ms = VALUES(last_tick_ms),
              is_final = VALUES(is_final),
              raw_json = VALUES(raw_json)
        """
        chunk_size = max(1, int(batch_size))
        total_rows = 0
        with self.connect() as conn:
            with conn.cursor() as cursor:
                for index in range(0, len(rows), chunk_size):
                    chunk = rows[index : index + chunk_size]
                    cursor.executemany(sql, chunk)
                    total_rows += len(chunk)
        return total_rows

    def upsert_bar_history(self, bars: Iterable[BarEvent], *, batch_size: int = 1000) -> int:
        rows = [self._bar_history_row(bar) for bar in bars or []]
        if not rows:
            return 0

        sql = """
            INSERT INTO bar_history
            (exchange, symbol, timeframe, start_ms, end_ms, open_price, high_price, low_price, close_price,
             volume, quote_volume, trade_count, last_tick_ms, is_final)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              end_ms = VALUES(end_ms),
              open_price = VALUES(open_price),
              high_price = VALUES(high_price),
              low_price = VALUES(low_price),
              close_price = VALUES(close_price),
              volume = VALUES(volume),
              quote_volume = VALUES(quote_volume),
              trade_count = VALUES(trade_count),
              last_tick_ms = VALUES(last_tick_ms),
              is_final = VALUES(is_final)
        """
        chunk_size = max(1, int(batch_size))
        total_rows = 0
        with self.connect() as conn:
            with conn.cursor() as cursor:
                for index in range(0, len(rows), chunk_size):
                    chunk = rows[index : index + chunk_size]
                    cursor.executemany(sql, chunk)
                    total_rows += len(chunk)
        return total_rows

    def _bar_checkpoint_row(self, bar: BarEvent) -> tuple:
        payload = json.dumps(bar.to_dict(), ensure_ascii=False, separators=(",", ":"))
        return (
            bar.exchange,
            bar.symbol,
            bar.timeframe,
            int(bar.start_ms),
            int(bar.end_ms),
            bar.open_price,
            bar.high_price,
            bar.low_price,
            bar.close_price,
            bar.volume,
            bar.quote_volume,
            bar.trade_count,
            bar.last_tick_ms,
            1 if bar.is_final else 0,
            payload,
        )

    def _bar_history_row(self, bar: BarEvent) -> tuple:
        return (
            bar.exchange,
            bar.symbol,
            bar.timeframe,
            int(bar.start_ms),
            int(bar.end_ms),
            bar.open_price,
            bar.high_price,
            bar.low_price,
            bar.close_price,
            bar.volume,
            bar.quote_volume,
            bar.trade_count,
            bar.last_tick_ms,
            1 if bar.is_final else 0,
        )

    def _bar_event_from_history_row(self, row: tuple[Any, ...]) -> BarEvent:
        return BarEvent(
            exchange=str(row[0]),
            symbol=str(row[1]),
            timeframe=str(row[2]),
            start_ms=int(row[3]),
            end_ms=int(row[4]),
            open_price=float(row[5]),
            high_price=float(row[6]),
            low_price=float(row[7]),
            close_price=float(row[8]),
            volume=float(row[9] or 0),
            quote_volume=float(row[10] or 0),
            trade_count=int(row[11] or 0),
            last_tick_ms=int(row[12] or row[4]),
            is_final=bool(row[13]),
            source="mysql",
            reason="rebuild",
        )

    def _row_to_dict(self, columns: list[str], row: tuple[Any, ...]) -> dict[str, Any]:
        return {
            column: self._normalize_sql_value(value)
            for column, value in zip(columns, row)
        }

    def _normalize_sql_value(self, value: Any) -> Any:
        if isinstance(value, Decimal):
            return float(value)
        if isinstance(value, datetime):
            return value.isoformat(sep=" ")
        if isinstance(value, date):
            return value.isoformat()
        return value

    def upsert_archive_manifest(
        self,
        *,
        exchange: str,
        timeframe: str,
        partition_key: str,
        file_path: str,
        start_ms: int,
        end_ms: int,
        bar_count: int,
    ) -> None:
        sql = """
            INSERT INTO archive_manifest
            (exchange, timeframe, partition_key, file_path, start_ms, end_ms, bar_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              file_path = VALUES(file_path),
              start_ms = LEAST(start_ms, VALUES(start_ms)),
              end_ms = GREATEST(end_ms, VALUES(end_ms)),
              bar_count = bar_count + VALUES(bar_count)
        """
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    sql,
                    (
                        exchange,
                        timeframe,
                        partition_key,
                        file_path,
                        int(start_ms),
                        int(end_ms),
                        int(bar_count),
                    ),
                )
