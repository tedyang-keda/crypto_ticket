from __future__ import annotations

import json
from typing import Sequence

import httpx

from ..models import SymbolInfo, TickEvent
from .base import ExchangeAdapter


class BinanceFuturesAdapter(ExchangeAdapter):
    def __init__(
        self,
        *,
        market_type: str,
        rest_url: str,
        ws_url: str,
        stream_chunk_size: int = 200,
        target_streams_per_conn: int = 800,
    ):
        self.name = "binance"
        self.market_type = market_type
        self.rest_url = rest_url.rstrip("/")
        self.ws_url = ws_url.rstrip("/")
        self.stream_chunk_size = int(stream_chunk_size)
        self.target_streams_per_conn = int(target_streams_per_conn)

    async def fetch_symbols(self, client: httpx.AsyncClient) -> list[SymbolInfo]:
        url = f"{self.rest_url}/fapi/v1/exchangeInfo" if self.market_type == "um_futures" else f"{self.rest_url}/api/v3/exchangeInfo"
        response = await client.get(url, timeout=20.0)
        response.raise_for_status()
        payload = response.json()
        symbols: list[SymbolInfo] = []
        for item in payload.get("symbols", []):
            status = str(item.get("status", "")).upper()
            symbol = str(item.get("symbol", "")).upper()
            if not symbol:
                continue
            is_active = status == "TRADING"
            symbols.append(
                SymbolInfo(
                    exchange=self.name,
                    symbol=symbol,
                    market_type=self.market_type,
                    status=status,
                    is_active=is_active,
                    raw=item,
                )
            )
        return symbols

    def symbol_to_stream(self, symbol: str) -> str:
        suffix = "@aggTrade" if self.market_type == "um_futures" else "@trade"
        return f"{symbol.lower()}{suffix}"

    def build_subscribe_payload(self, symbols: Sequence[str], request_id: int) -> str:
        params = [self.symbol_to_stream(symbol) for symbol in symbols]
        return json.dumps({"method": "SUBSCRIBE", "params": params, "id": request_id})

    def build_unsubscribe_payload(self, symbols: Sequence[str], request_id: int) -> str:
        params = [self.symbol_to_stream(symbol) for symbol in symbols]
        return json.dumps({"method": "UNSUBSCRIBE", "params": params, "id": request_id})

    def parse_message(self, payload: str) -> list[TickEvent]:
        data = json.loads(payload)
        if not isinstance(data, dict):
            return []
        event_type = str(data.get("e", "")).lower()
        if event_type not in {"aggtrade", "trade"}:
            return []
        symbol = str(data.get("s", "")).upper()
        if not symbol:
            return []
        price = float(data.get("p") or 0.0)
        size = float(data.get("q") or 0.0)
        ts_ms = int(data.get("T") or data.get("E") or 0)
        if price <= 0 or ts_ms <= 0:
            return []
        side = "sell" if bool(data.get("m")) else "buy"
        return [
            TickEvent(
                exchange=self.name,
                symbol=symbol,
                ts_ms=ts_ms,
                price=price,
                size=size,
                side=side,
                trade_id=str(data.get("a") or data.get("t") or ""),
                event_type=event_type,
                source="ws",
                raw=data,
            )
        ]

