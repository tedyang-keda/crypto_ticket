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
        rolled.extend(self._roll_up(final_bars))
        return rolled

    def close_due_bars(self, now_ms: int, grace_ms: int = 0) -> list[BarEvent]:
        emitted: list[BarEvent] = []
        cutoff_ms = int(now_ms) - max(0, int(grace_ms))
        states = sorted(
            self.states.items(),
            key=lambda item: (timeframe_index(item[0][2]), item[0][0], item[0][1]),
        )
        for _, state in states:
            if state.is_closed or state.end_ms > cutoff_ms:
                continue
            bar = self._finalize_state(state, cutoff_ms)
            emitted.append(bar)
            emitted.extend(self._roll_up([bar]))
        return emitted

    def _apply_tick_to_base(self, tick: TickEvent) -> list[BarEvent]:
        base_tf = "1m"
        key = self.state_key(tick.exchange, tick.symbol, base_tf)
        bucket_start = floor_bucket_start_ms(tick.ts_ms, base_tf)
        bucket_end = bucket_end_ms(bucket_start, base_tf)
        state = self.states.get(key)
        if state is None:
            self.states[key] = RollingBarState.from_tick(tick, base_tf, bucket_start, bucket_end)
            return []
        if state.is_closed:
            if bucket_start <= state.start_ms:
                return []
            finalized: list[BarEvent] = self._fill_gap_bars(state, bucket_start)
            self.states[key] = RollingBarState.from_tick(tick, base_tf, bucket_start, bucket_end)
            return finalized
        if bucket_start == state.start_ms:
            state.update_with_tick(tick)
            return []
        if bucket_start < state.start_ms:
            return []

        finalized: list[BarEvent] = [self._finalize_state(state, tick.ts_ms)]
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

    def _roll_up(self, bars: list[BarEvent]) -> list[BarEvent]:
        emitted: list[BarEvent] = []
        carry = list(bars)
        for timeframe in self.timeframes[1:]:
            next_carry: list[BarEvent] = []
            for bar in carry:
                results = self._apply_bar_to_timeframe(bar, timeframe)
                if results:
                    emitted.extend(results)
                    next_carry.extend(results)
            if not next_carry:
                break
            carry = next_carry
        return emitted

    def _apply_bar_to_timeframe(self, bar: BarEvent, timeframe: str) -> list[BarEvent]:
        key = self.state_key(bar.exchange, bar.symbol, timeframe)
        bucket_start = floor_bucket_start_ms(bar.start_ms, timeframe)
        bucket_end = bucket_end_ms(bucket_start, timeframe)
        state = self.states.get(key)
        if state is None:
            self.states[key] = RollingBarState.from_bar(bar, timeframe, bucket_start, bucket_end)
            return []
        if state.is_closed:
            if bucket_start <= state.start_ms:
                return []
            finalized: list[BarEvent] = self._fill_gap_bars(state, bucket_start)
            self.states[key] = RollingBarState.from_bar(bar, timeframe, bucket_start, bucket_end)
            return finalized
        if bucket_start == state.start_ms:
            state.update_with_bar(bar)
            return []
        if bucket_start < state.start_ms:
            return []

        finalized = self._finalize_state(state, bar.last_tick_ms or bar.end_ms)
        emitted = [finalized]
        emitted.extend(self._fill_gap_bars(state, bucket_start))
        self.states[key] = RollingBarState.from_bar(bar, timeframe, bucket_start, bucket_end)
        return emitted

    def _finalize_state(self, state: RollingBarState, closed_at_ms: int) -> BarEvent:
        state.mark_closed(closed_at_ms)
        return state.to_bar(is_final=True, reason="close")
