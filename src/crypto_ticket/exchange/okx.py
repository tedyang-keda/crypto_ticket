from __future__ import annotations

import json
from typing import Sequence

import httpx

from ..models import SymbolInfo, TickEvent
from .base import ExchangeAdapter


class OkxAdapter(ExchangeAdapter):
    def __init__(
        self,
        *,
        inst_type: str,
        rest_url: str,
        ws_url: str,
        stream_chunk_size: int = 120,
        target_streams_per_conn: int = 300,
    ):
        self.name = "okx"
        self.market_type = inst_type
        self.rest_url = rest_url.rstrip("/")
        self.ws_url = ws_url.rstrip("/")
        self.stream_chunk_size = int(stream_chunk_size)
        self.target_streams_per_conn = int(target_streams_per_conn)

    async def fetch_symbols(self, client: httpx.AsyncClient) -> list[SymbolInfo]:
        url = f"{self.rest_url}/api/v5/public/instruments"
        response = await client.get(url, params={"instType": self.market_type}, timeout=20.0)
        response.raise_for_status()
        payload = response.json()
        symbols: list[SymbolInfo] = []
        for item in payload.get("data", []):
            symbol = str(item.get("instId", "")).upper()
            state = str(item.get("state", "")).lower()
            if not symbol:
                continue
            is_active = state == "live"
            symbols.append(
                SymbolInfo(
                    exchange=self.name,
                    symbol=symbol,
                    market_type=self.market_type,
                    status=state,
                    is_active=is_active,
                    raw=item,
                )
            )
        return symbols

    def symbol_to_stream(self, symbol: str) -> str:
        return symbol.upper()

    def _build_args(self, symbols: Sequence[str]) -> list[dict[str, str]]:
        return [{"channel": "trades", "instId": self.symbol_to_stream(symbol)} for symbol in symbols]

    def build_subscribe_payload(self, symbols: Sequence[str], request_id: int) -> str:
        return json.dumps({"op": "subscribe", "args": self._build_args(symbols), "id": request_id})

    def build_unsubscribe_payload(self, symbols: Sequence[str], request_id: int) -> str:
        return json.dumps({"op": "unsubscribe", "args": self._build_args(symbols), "id": request_id})

    def parse_message(self, payload: str) -> list[TickEvent]:
        data = json.loads(payload)
        if not isinstance(data, dict):
            return []
        if data.get("event"):
            return []
        channel = str(data.get("arg", {}).get("channel", "")).lower()
        if channel != "trades":
            return []
        ticks: list[TickEvent] = []
        for item in data.get("data", []):
            symbol = str(item.get("instId", "")).upper()
            price = float(item.get("px") or 0.0)
            size = float(item.get("sz") or 0.0)
            ts_ms = int(item.get("ts") or 0)
            if not symbol or price <= 0 or ts_ms <= 0:
                continue
            ticks.append(
                TickEvent(
                    exchange=self.name,
                    symbol=symbol,
                    ts_ms=ts_ms,
                    price=price,
                    size=size,
                    side=str(item.get("side") or None),
                    trade_id=str(item.get("tradeId") or ""),
                    event_type=channel,
                    source="ws",
                    raw=item,
                )
            )
        return ticks

