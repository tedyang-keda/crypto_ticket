from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Tuple

from .exchange.binance import BinanceFuturesAdapter
from .exchange.okx import OkxAdapter


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw)
    except ValueError:
        return default


@dataclass(slots=True)
class ExchangeConfig:
    name: str
    market_type: str
    rest_url: str
    ws_url: str
    subscription_chunk_size: int
    target_streams_per_conn: int
    adapter: object


@dataclass(slots=True)
class AppConfig:
    app_env: str
    log_level: str
    archive_root: Path
    redis_url: str
    redis_stream_maxlen: int
    redis_consumer_group: str
    redis_consumer_name: str
    feishu_webhook_url: str
    health_report_interval_seconds: int
    symbol_refresh_interval_seconds: int
    archive_flush_interval_seconds: int
    bar_close_grace_seconds: int
    reconnect_base_delay_seconds: float
    reconnect_max_delay_seconds: float
    enable_mysql: bool
    mysql_tick_writes_enabled: bool
    mysql_host: str
    mysql_port: int
    mysql_user: str
    mysql_password: str
    mysql_database: str
    schema_path: Path | None
    exchanges: Tuple[ExchangeConfig, ...]


def load_config() -> AppConfig:
    binance_kind = os.getenv("BINANCE_KIND", "um_futures")
    okx_kind = os.getenv("OKX_KIND", "swap")
    binance_rest_url = os.getenv("BINANCE_REST_URL", "https://fapi.binance.com")
    binance_ws_url = os.getenv("BINANCE_WS_URL", "wss://fstream.binance.com/ws")
    okx_rest_url = os.getenv("OKX_REST_URL", "https://www.okx.com")
    okx_ws_url = os.getenv("OKX_WS_URL", "wss://ws.okx.com:8443/ws/v5/public")

    binance_adapter = BinanceFuturesAdapter(
        market_type=binance_kind,
        rest_url=binance_rest_url,
        ws_url=binance_ws_url,
        stream_chunk_size=env_int("BINANCE_SUBSCRIPTION_CHUNK_SIZE", 200),
        target_streams_per_conn=env_int("BINANCE_TARGET_STREAMS_PER_CONN", 1000),
    )
    okx_adapter = OkxAdapter(
        inst_type=okx_kind.upper(),
        rest_url=okx_rest_url,
        ws_url=okx_ws_url,
        stream_chunk_size=env_int("OKX_SUBSCRIPTION_CHUNK_SIZE", 120),
        target_streams_per_conn=env_int("OKX_TARGET_STREAMS_PER_CONN", 300),
    )

    schema_path = Path(os.getenv("MYSQL_SCHEMA_PATH", "./sql/schema.sql")).resolve()
    return AppConfig(
        app_env=os.getenv("APP_ENV", "dev"),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        archive_root=Path(os.getenv("ARCHIVE_ROOT", "./data/archive")).resolve(),
        redis_url=os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0"),
        redis_stream_maxlen=env_int("REDIS_STREAM_MAXLEN", 200_000),
        redis_consumer_group=os.getenv("REDIS_CONSUMER_GROUP", "crypto_ticket"),
        redis_consumer_name=os.getenv("REDIS_CONSUMER_NAME", "local-1"),
        feishu_webhook_url=os.getenv("FEISHU_WEBHOOK_URL", ""),
        health_report_interval_seconds=env_int("HEALTH_REPORT_INTERVAL_SECONDS", 30),
        symbol_refresh_interval_seconds=env_int("SYMBOL_REFRESH_INTERVAL_SECONDS", 120),
        archive_flush_interval_seconds=env_int("ARCHIVE_FLUSH_INTERVAL_SECONDS", 5),
        bar_close_grace_seconds=env_int("BAR_CLOSE_GRACE_SECONDS", 2),
        reconnect_base_delay_seconds=env_float("RECONNECT_BASE_DELAY_SECONDS", 1.0),
        reconnect_max_delay_seconds=env_float("RECONNECT_MAX_DELAY_SECONDS", 60.0),
        enable_mysql=env_bool("ENABLE_MYSQL", False),
        mysql_tick_writes_enabled=env_bool("MYSQL_TICK_WRITES_ENABLED", False),
        mysql_host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        mysql_port=env_int("MYSQL_PORT", 3306),
        mysql_user=os.getenv("MYSQL_USER", "root"),
        mysql_password=os.getenv("MYSQL_PASSWORD", ""),
        mysql_database=os.getenv("MYSQL_DATABASE", "crypto_ticket"),
        schema_path=schema_path if schema_path.exists() else None,
        exchanges=(
            ExchangeConfig(
                name="binance",
                market_type=binance_kind,
                rest_url=binance_rest_url,
                ws_url=binance_ws_url,
                subscription_chunk_size=binance_adapter.stream_chunk_size,
                target_streams_per_conn=binance_adapter.target_streams_per_conn,
                adapter=binance_adapter,
            ),
            ExchangeConfig(
                name="okx",
                market_type=okx_kind.upper(),
                rest_url=okx_rest_url,
                ws_url=okx_ws_url,
                subscription_chunk_size=okx_adapter.stream_chunk_size,
                target_streams_per_conn=okx_adapter.target_streams_per_conn,
                adapter=okx_adapter,
            ),
        ),
    )
