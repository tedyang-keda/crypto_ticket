from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Iterable, Optional

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
        payload = json.dumps(bar.to_dict(), ensure_ascii=False, separators=(",", ":"))
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    sql,
                    (
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
                    ),
                )

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

