from __future__ import annotations

import time
from typing import Iterable

from .models import SymbolInfo


def snapshot_symbols(symbols: Iterable[SymbolInfo]) -> list[dict]:
    now_ms = int(time.time() * 1000)
    payload = []
    for symbol in symbols:
        item = symbol.to_dict()
        item["raw"] = dict(symbol.raw)
        item["raw"]["first_seen_at_ms"] = item["raw"].get("first_seen_at_ms", now_ms)
        item["raw"]["last_seen_at_ms"] = now_ms
        payload.append(item)
    return payload


def diff_symbol_sets(previous: set[str], current: set[str]) -> tuple[set[str], set[str]]:
    added = current - previous
    removed = previous - current
    return added, removed
