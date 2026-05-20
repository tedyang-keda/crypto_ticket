from crypto_ticket.aggregation import MultiTimeframeAggregator
from crypto_ticket.models import TickEvent


def test_aggregator_emits_1m_bars_and_rolls_up():
    agg = MultiTimeframeAggregator()
    ticks = [
        TickEvent(exchange="binance", symbol="BTCUSDT", ts_ms=1716190001000, price=100.0, size=1.0),
        TickEvent(exchange="binance", symbol="BTCUSDT", ts_ms=1716190011000, price=101.0, size=2.0),
        TickEvent(exchange="binance", symbol="BTCUSDT", ts_ms=1716190061000, price=102.0, size=3.0),
    ]
    emitted = []
    for tick in ticks:
        emitted.extend(agg.on_tick(tick))
    assert any(bar.timeframe == "1m" for bar in emitted)
    assert any(bar.timeframe == "5m" for bar in emitted) is False


def test_close_due_bars_finalizes_without_followup_tick():
    agg = MultiTimeframeAggregator()
    base_ms = 1779262200000  # 2026-05-20 15:30:00 +08:00
    ticks = [
        TickEvent(exchange="okx", symbol="BTC-USDT-SWAP", ts_ms=base_ms + 10_000, price=100.0, size=1.0),
        TickEvent(exchange="okx", symbol="BTC-USDT-SWAP", ts_ms=base_ms + 61_000, price=101.0, size=2.0),
        TickEvent(exchange="okx", symbol="BTC-USDT-SWAP", ts_ms=base_ms + 14 * 60_000 + 30_000, price=102.0, size=3.0),
    ]
    for tick in ticks:
        agg.on_tick(tick)

    emitted = agg.close_due_bars(base_ms + 15 * 60_000 + 5_000, grace_ms=0)
    bars_15m = [bar for bar in emitted if bar.timeframe == "15m" and bar.is_final]
    assert bars_15m
    assert bars_15m[0].end_ms == base_ms + 15 * 60_000 - 1


def test_closed_state_can_still_fill_gap_on_next_tick():
    agg = MultiTimeframeAggregator()
    base_ms = 1779262200000  # 2026-05-20 15:30:00 +08:00

    agg.on_tick(TickEvent(exchange="okx", symbol="BTC-USDT-SWAP", ts_ms=base_ms + 10_000, price=100.0, size=1.0))
    agg.close_due_bars(base_ms + 61_000, grace_ms=0)

    emitted = agg.on_tick(
        TickEvent(exchange="okx", symbol="BTC-USDT-SWAP", ts_ms=base_ms + 3 * 60_000 + 5_000, price=101.0, size=1.0)
    )
    gap_bars = [bar for bar in emitted if bar.timeframe == "1m" and bar.reason == "gap"]
    assert len(gap_bars) == 2
