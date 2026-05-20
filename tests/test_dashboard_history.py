from __future__ import annotations

import gzip
import json
from pathlib import Path

from crypto_ticket.dashboard.history import load_archive_bars


def test_load_archive_bars_filters_and_orders(tmp_path: Path):
    archive_root = tmp_path / "archive"
    archive_file = archive_root / "binance" / "15m" / "year=2026" / "week=21" / "bars.jsonl.gz"
    archive_file.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "exchange": "binance",
            "symbol": "BTCUSDT",
            "timeframe": "15m",
            "start_ms": 10,
            "end_ms": 19,
            "open_price": 1.0,
            "high_price": 2.0,
            "low_price": 0.5,
            "close_price": 1.5,
            "volume": 10,
            "quote_volume": 15,
            "trade_count": 2,
            "last_tick_ms": 18,
            "is_final": True,
        },
        {
            "exchange": "binance",
            "symbol": "BTCUSDT",
            "timeframe": "15m",
            "start_ms": 20,
            "end_ms": 29,
            "open_price": 1.5,
            "high_price": 2.5,
            "low_price": 1.0,
            "close_price": 2.0,
            "volume": 20,
            "quote_volume": 30,
            "trade_count": 3,
            "last_tick_ms": 28,
            "is_final": True,
        },
        {
            "exchange": "binance",
            "symbol": "ETHUSDT",
            "timeframe": "15m",
            "start_ms": 30,
            "end_ms": 39,
            "open_price": 2.0,
            "high_price": 3.0,
            "low_price": 1.5,
            "close_price": 2.5,
            "volume": 30,
            "quote_volume": 45,
            "trade_count": 4,
            "last_tick_ms": 38,
            "is_final": True,
        },
    ]
    with gzip.open(archive_file, "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False))
            handle.write("\n")

    bars = load_archive_bars(archive_root, "binance", "15m", "BTCUSDT", limit=10)

    assert [row["start_ms"] for row in bars] == [10, 20]
    assert all(row["symbol"] == "BTCUSDT" for row in bars)
