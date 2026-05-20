from __future__ import annotations

import json
from typing import Any, Iterable, Optional

import redis.asyncio as redis
from redis.exceptions import ResponseError

from ..models import TickEvent


class RedisStore:
    def __init__(self, redis_url: str, *, stream_maxlen: int = 200_000):
        self.redis = redis.from_url(redis_url, decode_responses=True)
        self.stream_maxlen = int(stream_maxlen)

    async def close(self) -> None:
        await self.redis.aclose()

    async def ping(self) -> bool:
        return bool(await self.redis.ping())

    async def set_json(self, key: str, value: dict[str, Any], *, ttl_seconds: Optional[int] = None) -> None:
        payload = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        await self.redis.set(key, payload)
        if ttl_seconds:
            await self.redis.expire(key, int(ttl_seconds))

    async def get_json(self, key: str) -> Optional[dict[str, Any]]:
        raw = await self.redis.get(key)
        if not raw:
            return None
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return None
        return data if isinstance(data, dict) else None

    async def delete(self, *keys: str) -> int:
        return int(await self.redis.delete(*keys))

    async def sadd(self, key: str, *values: str) -> int:
        return int(await self.redis.sadd(key, *values))

    async def smembers(self, key: str) -> set[str]:
        return set(await self.redis.smembers(key))

    async def xgroup_create(self, stream: str, group: str) -> None:
        try:
            await self.redis.xgroup_create(stream, group, id="$", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def xadd_tick(self, stream: str, tick: TickEvent) -> str:
        fields = {
            "exchange": tick.exchange,
            "symbol": tick.symbol,
            "ts_ms": str(tick.ts_ms),
            "price": repr(tick.price),
            "size": repr(tick.size),
            "side": tick.side or "",
            "trade_id": tick.trade_id or "",
            "event_type": tick.event_type,
            "source": tick.source,
            "raw": json.dumps(dict(tick.raw), ensure_ascii=False, separators=(",", ":")),
        }
        return await self.redis.xadd(stream, fields, maxlen=self.stream_maxlen, approximate=True)

    async def xreadgroup(
        self,
        group: str,
        consumer: str,
        streams: Iterable[str],
        *,
        count: int = 200,
        block_ms: int = 1000,
    ):
        items = list(streams)
        if not items:
            return []
        pairs = {stream: ">" for stream in items}
        return await self.redis.xreadgroup(group, consumer, streams=pairs, count=count, block=block_ms)

    async def xack(self, stream: str, group: str, *ids: str) -> int:
        return int(await self.redis.xack(stream, group, *ids))

    async def save_bar_state(self, key: str, bar: dict[str, Any], *, ttl_seconds: int) -> None:
        await self.set_json(key, bar, ttl_seconds=ttl_seconds)

    async def load_bar_state(self, key: str) -> Optional[dict[str, Any]]:
        return await self.get_json(key)

    async def save_latest_quote(self, key: str, payload: dict[str, Any], *, ttl_seconds: int = 7 * 24 * 3600) -> None:
        await self.set_json(key, payload, ttl_seconds=ttl_seconds)

    async def load_latest_quote(self, key: str) -> Optional[dict[str, Any]]:
        return await self.get_json(key)
