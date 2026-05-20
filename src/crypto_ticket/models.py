from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Mapping, Optional


@dataclass(slots=True)
class SymbolInfo:
    exchange: str
    symbol: str
    market_type: str
    status: str
    is_active: bool
    raw: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["raw"] = dict(self.raw)
        return payload


@dataclass(slots=True)
class TickEvent:
    exchange: str
    symbol: str
    ts_ms: int
    price: float
    size: float
    side: Optional[str] = None
    trade_id: Optional[str] = None
    event_type: str = "trade"
    source: str = "ws"
    raw: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["raw"] = dict(self.raw)
        return payload


@dataclass(slots=True)
class BarEvent:
    exchange: str
    symbol: str
    timeframe: str
    start_ms: int
    end_ms: int
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float = 0.0
    quote_volume: float = 0.0
    trade_count: int = 0
    last_tick_ms: int = 0
    is_final: bool = False
    source: str = "aggregator"
    reason: str = "close"
    raw: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["raw"] = dict(self.raw)
        return payload


@dataclass(slots=True)
class RollingBarState:
    exchange: str
    symbol: str
    timeframe: str
    start_ms: int
    end_ms: int
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float = 0.0
    quote_volume: float = 0.0
    trade_count: int = 0
    last_tick_ms: int = 0

    @classmethod
    def from_tick(cls, tick: TickEvent, timeframe: str, start_ms: int, end_ms: int) -> "RollingBarState":
        quote_volume = tick.price * tick.size if tick.size else 0.0
        return cls(
            exchange=tick.exchange,
            symbol=tick.symbol,
            timeframe=timeframe,
            start_ms=start_ms,
            end_ms=end_ms,
            open_price=tick.price,
            high_price=tick.price,
            low_price=tick.price,
            close_price=tick.price,
            volume=tick.size,
            quote_volume=quote_volume,
            trade_count=1,
            last_tick_ms=tick.ts_ms,
        )

    @classmethod
    def from_bar(cls, bar: BarEvent, timeframe: str, start_ms: int, end_ms: int) -> "RollingBarState":
        return cls(
            exchange=bar.exchange,
            symbol=bar.symbol,
            timeframe=timeframe,
            start_ms=start_ms,
            end_ms=end_ms,
            open_price=bar.open_price,
            high_price=bar.high_price,
            low_price=bar.low_price,
            close_price=bar.close_price,
            volume=bar.volume,
            quote_volume=bar.quote_volume,
            trade_count=bar.trade_count,
            last_tick_ms=bar.last_tick_ms or bar.end_ms,
        )

    def update_with_tick(self, tick: TickEvent) -> None:
        self.high_price = max(self.high_price, tick.price)
        self.low_price = min(self.low_price, tick.price)
        self.close_price = tick.price
        self.volume += tick.size
        self.quote_volume += tick.price * tick.size if tick.size else 0.0
        self.trade_count += 1
        self.last_tick_ms = max(self.last_tick_ms, tick.ts_ms)

    def update_with_bar(self, bar: BarEvent) -> None:
        self.high_price = max(self.high_price, bar.high_price)
        self.low_price = min(self.low_price, bar.low_price)
        self.close_price = bar.close_price
        self.volume += bar.volume
        self.quote_volume += bar.quote_volume
        self.trade_count += bar.trade_count
        self.last_tick_ms = max(self.last_tick_ms, bar.last_tick_ms or bar.end_ms)

    def to_bar(self, *, is_final: bool, reason: str) -> BarEvent:
        return BarEvent(
            exchange=self.exchange,
            symbol=self.symbol,
            timeframe=self.timeframe,
            start_ms=self.start_ms,
            end_ms=self.end_ms,
            open_price=self.open_price,
            high_price=self.high_price,
            low_price=self.low_price,
            close_price=self.close_price,
            volume=self.volume,
            quote_volume=self.quote_volume,
            trade_count=self.trade_count,
            last_tick_ms=self.last_tick_ms,
            is_final=is_final,
            reason=reason,
        )

