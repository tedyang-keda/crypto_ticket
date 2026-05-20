from __future__ import annotations

from crypto_ticket.config import load_config
from crypto_ticket.models import BarEvent
from crypto_ticket.storage.mysql import MySQLHotStore


class _FakeCursor:
    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, tuple | list | None]] = []
        self.executemany_calls: list[tuple[str, list[tuple]]] = []
        self.fetchall_rows: list[tuple] = []
        self.description: list[tuple[str]] = []

    def execute(self, sql: str, params: tuple | list | None = None) -> None:
        self.execute_calls.append((sql, params))

    def executemany(self, sql: str, rows: list[tuple]) -> None:
        self.executemany_calls.append((sql, list(rows)))

    def fetchall(self) -> list[tuple]:
        return list(self.fetchall_rows)

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


class _FakeConnection:
    def __init__(self) -> None:
        self.cursor_obj = _FakeCursor()

    def cursor(self) -> _FakeCursor:
        return self.cursor_obj

    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


def test_load_config_defaults_disable_tick_writes(monkeypatch):
    monkeypatch.delenv("MYSQL_TICK_WRITES_ENABLED", raising=False)
    config = load_config()
    assert config.mysql_tick_writes_enabled is False


def test_upsert_bar_checkpoints_batches_rows():
    store = MySQLHotStore(
        host="127.0.0.1",
        port=3306,
        user="root",
        password="root123",
        database="crypto_ticket",
    )
    fake_conn = _FakeConnection()
    store.connect = lambda: fake_conn  # type: ignore[method-assign]

    bars = [
        BarEvent(
            exchange="binance",
            symbol="BTCUSDT",
            timeframe="1m",
            start_ms=1,
            end_ms=60_000,
            open_price=1.0,
            high_price=2.0,
            low_price=0.5,
            close_price=1.5,
            volume=10.0,
            quote_volume=15.0,
            trade_count=2,
            last_tick_ms=59_000,
            is_final=True,
        ),
        BarEvent(
            exchange="okx",
            symbol="BTC-USDT-SWAP",
            timeframe="1m",
            start_ms=61_000,
            end_ms=120_000,
            open_price=1.5,
            high_price=2.5,
            low_price=1.0,
            close_price=2.0,
            volume=20.0,
            quote_volume=30.0,
            trade_count=3,
            last_tick_ms=119_000,
            is_final=True,
        ),
    ]

    saved = store.upsert_bar_checkpoints(bars)

    assert saved == 2
    assert len(fake_conn.cursor_obj.executemany_calls) == 1
    sql, rows = fake_conn.cursor_obj.executemany_calls[0]
    assert "INSERT INTO bar_checkpoint" in sql
    assert len(rows) == 2


def test_upsert_bar_history_batches_rows():
    store = MySQLHotStore(
        host="127.0.0.1",
        port=3306,
        user="root",
        password="root123",
        database="crypto_ticket",
    )
    fake_conn = _FakeConnection()
    store.connect = lambda: fake_conn  # type: ignore[method-assign]

    saved = store.upsert_bar_history(
        [
            BarEvent(
                exchange="binance",
                symbol="BTCUSDT",
                timeframe="1m",
                start_ms=1,
                end_ms=60_000,
                open_price=1.0,
                high_price=2.0,
                low_price=0.5,
                close_price=1.5,
                volume=10.0,
                quote_volume=15.0,
                trade_count=2,
                last_tick_ms=59_000,
                is_final=True,
            )
        ]
    )

    assert saved == 1
    sql, rows = fake_conn.cursor_obj.executemany_calls[0]
    assert "INSERT INTO bar_history" in sql
    assert len(rows) == 1
    assert len(rows[0]) == 14


def test_list_recent_bars_uses_primary_index_and_returns_ascending():
    store = MySQLHotStore(
        host="127.0.0.1",
        port=3306,
        user="root",
        password="root123",
        database="crypto_ticket",
    )
    fake_conn = _FakeConnection()
    fake_conn.cursor_obj.description = [
        ("exchange",),
        ("symbol",),
        ("timeframe",),
        ("start_ms",),
        ("end_ms",),
        ("open_price",),
        ("high_price",),
        ("low_price",),
        ("close_price",),
        ("volume",),
        ("quote_volume",),
        ("trade_count",),
        ("last_tick_ms",),
        ("is_final",),
        ("created_at",),
        ("updated_at",),
    ]
    fake_conn.cursor_obj.fetchall_rows = [
        ("binance", "BTCUSDT", "1m", 20, 29, 1.5, 2.5, 1.0, 2.0, 20.0, 30.0, 3, 28, 1, None, None),
        ("binance", "BTCUSDT", "1m", 10, 19, 1.0, 2.0, 0.5, 1.5, 10.0, 15.0, 2, 18, 1, None, None),
    ]
    store.connect = lambda: fake_conn  # type: ignore[method-assign]

    bars = store.list_recent_bars("binance", "BTCUSDT", "1m", limit=200)

    sql, params = fake_conn.cursor_obj.execute_calls[0]
    assert "FROM bar_history FORCE INDEX (PRIMARY)" in sql
    assert "ORDER BY start_ms DESC" in sql
    assert params == ("binance", "BTCUSDT", "1m", 200)
    assert [row["start_ms"] for row in bars] == [10, 20]
