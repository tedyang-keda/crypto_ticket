from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable, Sequence

import httpx

from ..models import SymbolInfo, TickEvent


class ExchangeAdapter(ABC):
    name: str
    market_type: str
    ws_url: str
    rest_url: str
    stream_chunk_size: int
    target_streams_per_conn: int

    @abstractmethod
    async def fetch_symbols(self, client: httpx.AsyncClient) -> list[SymbolInfo]:
        raise NotImplementedError

    @abstractmethod
    def symbol_to_stream(self, symbol: str) -> str:
        raise NotImplementedError

    @abstractmethod
    def build_subscribe_payload(self, symbols: Sequence[str], request_id: int) -> str:
        raise NotImplementedError

    @abstractmethod
    def build_unsubscribe_payload(self, symbols: Sequence[str], request_id: int) -> str:
        raise NotImplementedError

    @abstractmethod
    def parse_message(self, payload: str) -> list[TickEvent]:
        raise NotImplementedError

    def chunk_symbols(self, symbols: Iterable[str]) -> list[list[str]]:
        items = [symbol for symbol in symbols if symbol]
        size = max(1, int(self.stream_chunk_size))
        return [items[index : index + size] for index in range(0, len(items), size)]

