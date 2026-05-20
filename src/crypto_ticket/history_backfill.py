from __future__ import annotations

import gzip
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from .aggregation import MinuteBarRollupAggregator
from .models import BarEvent
from .storage.mysql import MySQLHotStore
from .timeframes import TIMEFRAME_ORDER, normalize_timeframe


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class BackfillStats:
    files: int = 0
    rows: int = 0
    skipped: int = 0


def backfill_bar_history(
    mysql: MySQLHotStore,
    archive_root: str | Path,
    *,
    exchange: Optional[str] = None,
    timeframe: Optional[str] = None,
    symbol: Optional[str] = None,
    max_files: Optional[int] = None,
    batch_size: int = 1000,
) -> BackfillStats:
    files = _list_archive_files(archive_root, exchange=exchange, timeframe=timeframe)
    if max_files is not None:
        files = files[: max(0, int(max_files))]

    stats = BackfillStats()
    batch: list[BarEvent] = []
    normalized_exchange = exchange.lower() if exchange else None
    normalized_symbol = symbol.upper() if symbol else None
    normalized_timeframe = normalize_timeframe(timeframe) if timeframe else None
    chunk_size = max(1, int(batch_size))

    for file_path in files:
        stats.files += 1
        try:
            with gzip.open(file_path, "rt", encoding="utf-8") as handle:
                for line in handle:
                    payload = _decode_archive_line(line)
                    if payload is None:
                        stats.skipped += 1
                        continue
                    if not _matches_filters(
                        payload,
                        exchange=normalized_exchange,
                        timeframe=normalized_timeframe,
                        symbol=normalized_symbol,
                    ):
                        continue
                    bar = _bar_from_payload(payload)
                    if bar is None:
                        stats.skipped += 1
                        continue
                    batch.append(bar)
                    if len(batch) >= chunk_size:
                        stats.rows += mysql.upsert_bar_history(batch, batch_size=chunk_size)
                        batch.clear()
        except OSError as exc:
            stats.skipped += 1
            logger.warning("failed to read archive file %s: %s", file_path, exc)

    if batch:
        stats.rows += mysql.upsert_bar_history(batch, batch_size=chunk_size)
    return stats


def rebuild_rollups_from_history(
    mysql: MySQLHotStore,
    *,
    exchange: Optional[str] = None,
    symbol: Optional[str] = None,
    batch_size: int = 1000,
) -> BackfillStats:
    stats = BackfillStats()
    rollup = MinuteBarRollupAggregator(TIMEFRAME_ORDER)
    batch: list[BarEvent] = []
    chunk_size = max(1, int(batch_size))

    for one_minute_bar in mysql.iter_history_bars(timeframe="1m", exchange=exchange, symbol=symbol):
        update = rollup.on_1m_bar(one_minute_bar)
        batch.extend(update.bars_for_history)
        if len(batch) >= chunk_size:
            stats.rows += mysql.upsert_bar_history(batch, batch_size=chunk_size)
            batch.clear()

    if batch:
        stats.rows += mysql.upsert_bar_history(batch, batch_size=chunk_size)
    return stats


def _list_archive_files(
    archive_root: str | Path,
    *,
    exchange: Optional[str] = None,
    timeframe: Optional[str] = None,
) -> list[Path]:
    root = Path(archive_root)
    if not root.exists():
        return []
    normalized_exchange = exchange.lower() if exchange else None
    normalized_timeframe = normalize_timeframe(timeframe) if timeframe else None
    files = [path for path in root.rglob("bars.jsonl.gz") if path.is_file()]
    files = [path for path in files if _archive_path_matches(root, path, normalized_exchange, normalized_timeframe)]
    return sorted(files, key=lambda path: path.stat().st_mtime, reverse=True)


def _archive_path_matches(
    root: Path,
    path: Path,
    exchange: Optional[str],
    timeframe: Optional[str],
) -> bool:
    try:
        parts = path.relative_to(root).parts
    except ValueError:
        return False
    if exchange and (len(parts) < 1 or parts[0].lower() != exchange):
        return False
    if timeframe and (len(parts) < 2 or parts[1] != timeframe):
        return False
    return True


def _decode_archive_line(line: str) -> Optional[dict[str, Any]]:
    if not line.strip():
        return None
    try:
        payload = json.loads(line)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def _matches_filters(
    payload: dict[str, Any],
    *,
    exchange: Optional[str],
    timeframe: Optional[str],
    symbol: Optional[str],
) -> bool:
    if exchange and str(payload.get("exchange", "")).lower() != exchange:
        return False
    if timeframe and str(payload.get("timeframe", "")) != timeframe:
        return False
    if symbol and str(payload.get("symbol", "")).upper() != symbol:
        return False
    return True


def _bar_from_payload(payload: dict[str, Any]) -> Optional[BarEvent]:
    try:
        raw = payload.get("raw") or {}
        return BarEvent(
            exchange=str(payload["exchange"]),
            symbol=str(payload["symbol"]),
            timeframe=normalize_timeframe(str(payload["timeframe"])),
            start_ms=int(payload["start_ms"]),
            end_ms=int(payload["end_ms"]),
            open_price=float(payload["open_price"]),
            high_price=float(payload["high_price"]),
            low_price=float(payload["low_price"]),
            close_price=float(payload["close_price"]),
            volume=float(payload.get("volume") or 0.0),
            quote_volume=float(payload.get("quote_volume") or 0.0),
            trade_count=int(payload.get("trade_count") or 0),
            last_tick_ms=int(payload.get("last_tick_ms") or payload["end_ms"]),
            is_final=bool(payload.get("is_final", True)),
            source=str(payload.get("source") or "archive"),
            reason=str(payload.get("reason") or "backfill"),
            raw=raw if isinstance(raw, dict) else {},
        )
    except (KeyError, TypeError, ValueError):
        return None
