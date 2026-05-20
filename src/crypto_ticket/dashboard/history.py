from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any

from ..timeframes import normalize_timeframe


def list_archive_files(archive_root: str | Path, exchange: str, timeframe: str) -> list[Path]:
    root = Path(archive_root)
    tf = normalize_timeframe(timeframe)
    base = root / exchange.lower() / tf
    if not base.exists():
        return []
    files = [path for path in base.rglob("bars.jsonl.gz") if path.is_file()]
    return sorted(files, key=lambda path: path.stat().st_mtime, reverse=True)


def load_archive_bars(
    archive_root: str | Path,
    exchange: str,
    timeframe: str,
    symbol: str,
    *,
    limit: int = 400,
    max_files: int = 24,
) -> list[dict[str, Any]]:
    limit = max(1, int(limit))
    max_files = max(1, int(max_files))
    files = list_archive_files(archive_root, exchange, timeframe)[:max_files]
    rows: list[dict[str, Any]] = []
    target_exchange = exchange.lower()
    target_symbol = symbol.upper()
    target_timeframe = normalize_timeframe(timeframe)

    for file_path in files:
        try:
            with gzip.open(file_path, "rt", encoding="utf-8") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    try:
                        payload = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if (
                        str(payload.get("exchange", "")).lower() != target_exchange
                        or str(payload.get("symbol", "")).upper() != target_symbol
                        or str(payload.get("timeframe", "")) != target_timeframe
                    ):
                        continue
                    rows.append(payload)
        except OSError:
            continue

    rows.sort(key=lambda item: int(item.get("start_ms") or 0))
    if len(rows) > limit:
        rows = rows[-limit:]
    return rows

