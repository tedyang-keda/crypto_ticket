from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Iterable, Optional

from .models import BarEvent, RollingBarState, TickEvent
from .timeframes import (
    TIMEFRAME_ORDER,
    bucket_end_ms,
    floor_bucket_start_ms,
    normalize_timeframe,
    timeframe_index,
    timeframe_to_ms,
)


@dataclass(slots=True)
class AggregationResult:
    emitted_bars: list[BarEvent]


class MultiTimeframeAggregator:
    def __init__(self, timeframes: Iterable[str] = TIMEFRAME_ORDER):
        frames = [normalize_timeframe(tf) for tf in timeframes]
        frames.sort(key=timeframe_index)
        if frames[0] != "1m":
            raise ValueError("the aggregator must start from 1m")
        self.timeframes = frames
        self.states: dict[tuple[str, str, str], RollingBarState] = {}

    def state_key(self, exchange: str, symbol: str, timeframe: str) -> tuple[str, str, str]:
        return exchange, symbol, timeframe

    def state_snapshot(self, exchange: str, symbol: str, timeframe: str) -> Optional[dict[str, Any]]:
        state = self.states.get(self.state_key(exchange, symbol, timeframe))
        return asdict(state) if state else None

    def restore_state(self, payload: dict[str, Any]) -> None:
        key = self.state_key(payload["exchange"], payload["symbol"], payload["timeframe"])
        self.states[key] = RollingBarState(**payload)

    def on_tick(self, tick: TickEvent) -> list[BarEvent]:
        final_bars = self._apply_tick_to_base(tick)
        rolled = list(final_bars)
        for bar in final_bars:
            rolled.extend(self._roll_up(bar))
        return rolled

    def _apply_tick_to_base(self, tick: TickEvent) -> list[BarEvent]:
        base_tf = "1m"
        key = self.state_key(tick.exchange, tick.symbol, base_tf)
        bucket_start = floor_bucket_start_ms(tick.ts_ms, base_tf)
        bucket_end = bucket_end_ms(bucket_start, base_tf)
        state = self.states.get(key)
        if state is None:
            self.states[key] = RollingBarState.from_tick(tick, base_tf, bucket_start, bucket_end)
            return []
        if bucket_start == state.start_ms:
            state.update_with_tick(tick)
            return []
        if bucket_start < state.start_ms:
            return []

        finalized: list[BarEvent] = [state.to_bar(is_final=True, reason="close")]
        finalized.extend(self._fill_gap_bars(state, bucket_start))
        self.states[key] = RollingBarState.from_tick(tick, base_tf, bucket_start, bucket_end)
        return finalized

    def _fill_gap_bars(self, previous_state: RollingBarState, next_bucket_start_ms: int) -> list[BarEvent]:
        gap_bars: list[BarEvent] = []
        previous_close = previous_state.close_price
        gap_duration_ms = timeframe_to_ms(previous_state.timeframe)
        gap_start = previous_state.start_ms + gap_duration_ms
        while gap_start < next_bucket_start_ms:
            gap_end = gap_start + gap_duration_ms - 1
            gap_bar = BarEvent(
                exchange=previous_state.exchange,
                symbol=previous_state.symbol,
                timeframe=previous_state.timeframe,
                start_ms=gap_start,
                end_ms=gap_end,
                open_price=previous_close,
                high_price=previous_close,
                low_price=previous_close,
                close_price=previous_close,
                volume=0.0,
                quote_volume=0.0,
                trade_count=0,
                last_tick_ms=previous_state.end_ms,
                is_final=True,
                reason="gap",
            )
            gap_bars.append(gap_bar)
            gap_start += gap_duration_ms
        return gap_bars

    def _roll_up(self, bar: BarEvent) -> list[BarEvent]:
        emitted: list[BarEvent] = []
        carry = bar
        for timeframe in self.timeframes[1:]:
            result = self._apply_bar_to_timeframe(carry, timeframe)
            if result is None:
                break
            emitted.append(result)
            carry = result
        return emitted

    def _apply_bar_to_timeframe(self, bar: BarEvent, timeframe: str) -> Optional[BarEvent]:
        key = self.state_key(bar.exchange, bar.symbol, timeframe)
        bucket_start = floor_bucket_start_ms(bar.start_ms, timeframe)
        bucket_end = bucket_end_ms(bucket_start, timeframe)
        state = self.states.get(key)
        if state is None:
            self.states[key] = RollingBarState.from_bar(bar, timeframe, bucket_start, bucket_end)
            return None
        if bucket_start == state.start_ms:
            state.update_with_bar(bar)
            return None
        if bucket_start < state.start_ms:
            return None

        finalized = state.to_bar(is_final=True, reason="close")
        self.states[key] = RollingBarState.from_bar(bar, timeframe, bucket_start, bucket_end)
        return finalized
