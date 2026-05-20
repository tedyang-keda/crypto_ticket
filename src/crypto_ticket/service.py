from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import httpx
import websockets

from .aggregation import MultiTimeframeAggregator
from .archive import WeeklyArchiveWriter
from .config import AppConfig, ExchangeConfig
from .logging import configure_logging
from .models import BarEvent, SymbolInfo, TickEvent
from .monitoring.feishu import FeishuNotifier
from .storage.redis_store import RedisStore
from .storage.mysql import MySQLHotStore
from .symbols import diff_symbol_sets
from .timeframes import TIMEFRAME_ORDER


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ExchangeRuntime:
    config: ExchangeConfig
    adapter: Any
    symbols: set[str] = field(default_factory=set)
    symbol_meta: dict[str, SymbolInfo] = field(default_factory=dict)
    consumers: list[asyncio.Task] = field(default_factory=list)
    collector_task: Optional[asyncio.Task] = None
    health_task: Optional[asyncio.Task] = None
    last_tick_at: float = 0.0
    last_message_at: float = 0.0


class CryptoTicketService:
    def __init__(self, config: AppConfig):
        self.config = config
        configure_logging(config.log_level)
        self.redis = RedisStore(config.redis_url, stream_maxlen=config.redis_stream_maxlen)
        self.archive = WeeklyArchiveWriter(config.archive_root)
        self.notifier = FeishuNotifier(config.feishu_webhook_url)
        self.aggregator = MultiTimeframeAggregator(TIMEFRAME_ORDER)
        self.mysql = None
        if config.enable_mysql:
            self.mysql = MySQLHotStore(
                host=config.mysql_host,
                port=config.mysql_port,
                user=config.mysql_user,
                password=config.mysql_password,
                database=config.mysql_database,
                schema_path=str(config.schema_path) if config.schema_path else None,
            )
        self.http = httpx.AsyncClient(timeout=20.0)
        self.exchanges: dict[str, ExchangeRuntime] = {}
        self._stop = asyncio.Event()

    async def close(self) -> None:
        self._stop.set()
        await self.http.aclose()
        await self.redis.close()

    async def run(self) -> None:
        if self.mysql:
            await asyncio.to_thread(self.mysql.ensure_schema)
        archive_task = asyncio.create_task(self.archive.run(), name="archive-writer")
        aggregator_tasks: list[asyncio.Task] = []
        for exchange in self.config.exchanges:
            runtime = ExchangeRuntime(config=exchange, adapter=exchange.adapter)
            self.exchanges[exchange.name] = runtime
            aggregator_tasks.append(asyncio.create_task(self._run_exchange(runtime), name=f"exchange-{exchange.name}"))
            aggregator_tasks.append(asyncio.create_task(self._run_aggregator(runtime), name=f"aggregator-{exchange.name}"))
        try:
            await asyncio.gather(*aggregator_tasks)
        finally:
            self._stop.set()
            for task in aggregator_tasks:
                task.cancel()
            await asyncio.gather(*aggregator_tasks, return_exceptions=True)
            await self.archive.close()
            await archive_task
            await self.close()

    async def _run_exchange(self, runtime: ExchangeRuntime) -> None:
        runtime.collector_task = asyncio.create_task(
            self._collector_worker(runtime),
            name=f"collector-{runtime.config.name}",
        )
        runtime.health_task = asyncio.create_task(
            self._run_health_loop(runtime),
            name=f"health-{runtime.config.name}",
        )
        while not self._stop.is_set():
            try:
                await self._reconcile_symbols(runtime)
                if runtime.symbols and len(runtime.symbols) > runtime.config.target_streams_per_conn:
                    await self.notifier.send_text(
                        f"[crypto-ticket] {runtime.config.name} active symbols={len(runtime.symbols)} exceed soft limit={runtime.config.target_streams_per_conn}",
                        key=f"symbol-limit:{runtime.config.name}",
                        cooldown_seconds=600,
                    )
                await asyncio.sleep(self.config.symbol_refresh_interval_seconds)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("collector loop failed for %s: %s", runtime.config.name, exc)
                await self.notifier.send_text(
                    f"[crypto-ticket] {runtime.config.name} collector failed: {exc}",
                    key=f"collector:{runtime.config.name}",
                    cooldown_seconds=120,
                )
                await asyncio.sleep(self.config.reconnect_max_delay_seconds)
        if runtime.collector_task:
            runtime.collector_task.cancel()
            with contextlib.suppress(Exception):
                await runtime.collector_task
        if runtime.health_task:
            runtime.health_task.cancel()
            with contextlib.suppress(Exception):
                await runtime.health_task

    async def _reconcile_symbols(self, runtime: ExchangeRuntime) -> None:
        symbols = await self._fetch_active_symbols(runtime)
        current = {symbol.symbol for symbol in symbols if symbol.is_active}
        previous = set(runtime.symbols)
        added, removed = diff_symbol_sets(previous, current)
        runtime.symbol_meta = {symbol.symbol: symbol for symbol in symbols}
        runtime.symbols = current
        if self.mysql:
            await asyncio.to_thread(self.mysql.upsert_symbol_registry, symbols)
        await self.redis.set_json(
            f"symbol-universe:{runtime.config.name}",
            {
                "exchange": runtime.config.name,
                "market_type": runtime.config.market_type,
                "active_symbols": sorted(current),
                "updated_at_ms": int(time.time() * 1000),
            },
            ttl_seconds=24 * 3600,
        )
        if added:
            await self.notifier.send_text(
                f"[crypto-ticket] {runtime.config.name} new symbols: {len(added)}",
                key=f"symbol-add:{runtime.config.name}",
                cooldown_seconds=300,
            )
        if removed:
            await self.notifier.send_text(
                f"[crypto-ticket] {runtime.config.name} delisted symbols: {len(removed)}",
                key=f"symbol-remove:{runtime.config.name}",
                cooldown_seconds=300,
            )

    async def _fetch_active_symbols(self, runtime: ExchangeRuntime) -> list[SymbolInfo]:
        return await runtime.adapter.fetch_symbols(self.http)

    async def _collector_worker(self, runtime: ExchangeRuntime) -> None:
        backoff = self.config.reconnect_base_delay_seconds
        while not self._stop.is_set():
            if not runtime.symbols:
                await asyncio.sleep(min(10, self.config.symbol_refresh_interval_seconds))
                continue
            try:
                await self._collector_connection(runtime)
                backoff = self.config.reconnect_base_delay_seconds
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("%s reconnect after error: %s", runtime.config.name, exc)
                await self.notifier.send_text(
                    f"[crypto-ticket] {runtime.config.name} ws error: {exc}",
                    key=f"ws-error:{runtime.config.name}",
                    cooldown_seconds=120,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, self.config.reconnect_max_delay_seconds)

    async def _collector_connection(self, runtime: ExchangeRuntime) -> None:
        uri = runtime.config.ws_url
        request_id = random.randint(1, 1_000_000)
        async with websockets.connect(uri, ping_interval=20, ping_timeout=20, max_queue=None) as websocket:
            subscribed_symbols: set[str] = set()
            await self._sync_ws_subscriptions(runtime, websocket, subscribed_symbols, request_id)
            logger.info("%s websocket connected with %s symbols", runtime.config.name, len(subscribed_symbols))
            last_heartbeat = time.time()
            last_sync = time.time()
            while not self._stop.is_set():
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=10.0)
                except asyncio.TimeoutError:
                    await self._sync_ws_subscriptions(runtime, websocket, subscribed_symbols, request_id)
                    last_sync = time.time()
                    continue
                if isinstance(message, bytes):
                    message = message.decode("utf-8")
                if not message:
                    continue
                ticks = runtime.adapter.parse_message(message)
                if not ticks:
                    if time.time() - last_heartbeat > 30:
                        last_heartbeat = time.time()
                    continue
                for tick in ticks:
                    runtime.last_tick_at = time.time()
                    runtime.last_message_at = time.time()
                    await self.redis.xadd_tick(f"ticks:{runtime.config.name}", tick)
                    if self.mysql and self.config.mysql_tick_writes_enabled:
                        await asyncio.to_thread(self.mysql.upsert_latest_quote, tick)
                last_heartbeat = time.time()
                if time.time() - last_sync >= 5:
                    await self._sync_ws_subscriptions(runtime, websocket, subscribed_symbols, request_id)
                    last_sync = time.time()

    async def _sync_ws_subscriptions(
        self,
        runtime: ExchangeRuntime,
        websocket,
        subscribed_symbols: set[str],
        request_id: int,
    ) -> None:
        desired = set(runtime.symbols)
        if desired == subscribed_symbols:
            return
        added, removed = diff_symbol_sets(subscribed_symbols, desired)
        if added:
            await self._send_subscription_messages(
                websocket,
                runtime.adapter.build_subscribe_payload,
                sorted(added),
                request_id,
                runtime.config.subscription_chunk_size,
            )
        if removed:
            await self._send_subscription_messages(
                websocket,
                runtime.adapter.build_unsubscribe_payload,
                sorted(removed),
                request_id + 1,
                runtime.config.subscription_chunk_size,
            )
        subscribed_symbols.clear()
        subscribed_symbols.update(desired)
        if added or removed:
            logger.info(
                "%s ws resynced added=%s removed=%s total=%s",
                runtime.config.name,
                len(added),
                len(removed),
                len(subscribed_symbols),
            )

    async def _send_subscription_messages(
        self,
        websocket,
        payload_builder,
        symbols: list[str],
        request_id: int,
        chunk_size: int,
    ) -> None:
        chunks = self._chunk_for_subscribe(symbols, chunk_size)
        current_id = request_id
        for chunk in chunks:
            payload = payload_builder(chunk, current_id)
            await websocket.send(payload)
            current_id += 1

    def _chunk_for_subscribe(self, symbols: list[str], chunk_size: int) -> list[list[str]]:
        if not symbols:
            return []
        size = max(1, int(chunk_size))
        return [symbols[index : index + size] for index in range(0, len(symbols), size)]

    async def _run_aggregator(self, runtime: ExchangeRuntime) -> None:
        stream = f"ticks:{runtime.config.name}"
        group = self.config.redis_consumer_group
        consumer = self.config.redis_consumer_name
        await self.redis.xgroup_create(stream, group)
        pending_flush: list[BarEvent] = []
        last_flush = time.time()
        last_due_check = time.monotonic()
        while not self._stop.is_set():
            now_monotonic = time.monotonic()
            if now_monotonic - last_due_check >= 1.0:
                due_bars = self.aggregator.close_due_bars(
                    int(time.time() * 1000),
                    self.config.bar_close_grace_seconds * 1000,
                )
                if due_bars:
                    pending_flush.extend(due_bars)
                last_due_check = now_monotonic
            batches = await self.redis.xreadgroup(group, consumer, [stream], count=200, block_ms=1000)
            if not batches:
                await self._flush_bars(pending_flush)
                pending_flush.clear()
                last_flush = time.time()
                continue
            for stream_name, entries in batches:
                ack_ids = []
                for entry_id, fields in entries:
                    tick = self._tick_from_stream_fields(runtime.config.name, fields)
                    if tick is None:
                        ack_ids.append(entry_id)
                        continue
                    bars = self.aggregator.on_tick(tick)
                    for bar in bars:
                        pending_flush.append(bar)
                    ack_ids.append(entry_id)
                    if len(pending_flush) >= 200 or time.time() - last_flush >= self.config.archive_flush_interval_seconds:
                        await self._flush_bars(pending_flush)
                        pending_flush.clear()
                        last_flush = time.time()
                if ack_ids:
                    await self.redis.xack(stream_name, group, *ack_ids)
        if pending_flush:
            await self._flush_bars(pending_flush)

    async def _run_health_loop(self, runtime: ExchangeRuntime) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(self.config.health_report_interval_seconds)
            if self._stop.is_set():
                break
            try:
                await self._health_check(runtime)
            except Exception as exc:
                logger.warning("health check failed for %s: %s", runtime.config.name, exc)

    async def _health_check(self, runtime: ExchangeRuntime) -> None:
        now = time.time()
        active_symbols = len(runtime.symbols)
        tick_age = now - runtime.last_tick_at if runtime.last_tick_at else None
        message_age = now - runtime.last_message_at if runtime.last_message_at else None
        queue_size = self.archive.queue.qsize()
        stale_threshold = max(self.config.health_report_interval_seconds * 4, 120)

        if active_symbols > 0 and (tick_age is None or tick_age > stale_threshold):
            await self.notifier.send_text(
                f"[crypto-ticket] {runtime.config.name} stale feed active_symbols={active_symbols} tick_age={tick_age}",
                key=f"stale-feed:{runtime.config.name}",
                cooldown_seconds=300,
            )
        if queue_size > 10_000:
            await self.notifier.send_text(
                f"[crypto-ticket] archive queue backlog={queue_size} exchange={runtime.config.name}",
                key=f"archive-backlog:{runtime.config.name}",
                cooldown_seconds=300,
            )
        if runtime.collector_task and runtime.collector_task.done():
            await self.notifier.send_text(
                f"[crypto-ticket] collector task stopped unexpectedly for {runtime.config.name}",
                key=f"collector-done:{runtime.config.name}",
                cooldown_seconds=120,
            )

    async def _flush_bars(self, bars: list[BarEvent]) -> None:
        if not bars:
            return
        await asyncio.gather(*(self.archive.submit(bar) for bar in bars))
        if self.mysql:
            await asyncio.to_thread(self._write_mysql_bars, bars)

    def _write_mysql_bars(self, bars: list[BarEvent]) -> None:
        if not self.mysql:
            return
        self.mysql.upsert_bar_checkpoints(bars)
        if self.config.mysql_bar_history_enabled:
            self.mysql.upsert_bar_history(bars)

    def _tick_from_stream_fields(self, exchange: str, fields: dict[str, str]) -> Optional[TickEvent]:
        try:
            raw = json.loads(fields.get("raw", "{}"))
            return TickEvent(
                exchange=exchange,
                symbol=str(fields["symbol"]),
                ts_ms=int(fields["ts_ms"]),
                price=float(fields["price"]),
                size=float(fields.get("size") or 0.0),
                side=fields.get("side") or None,
                trade_id=fields.get("trade_id") or None,
                event_type=fields.get("event_type") or "trade",
                source=fields.get("source") or "ws",
                raw=raw if isinstance(raw, dict) else {},
            )
        except Exception:
            logger.exception("failed to decode tick fields: %s", fields)
            return None
