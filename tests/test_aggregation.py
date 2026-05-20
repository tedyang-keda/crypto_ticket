from crypto_ticket.aggregation import MinuteBarRollupAggregator, MultiTimeframeAggregator
from crypto_ticket.models import BarEvent, TickEvent


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


def test_minute_rollup_updates_current_bar_each_minute_and_finalizes_bucket():
    agg = MinuteBarRollupAggregator(("1m", "5m", "15m"))
    base_ms = 1779262200000
    updates = []

    for minute in range(5):
        updates.append(
            agg.on_1m_bar(
                BarEvent(
                    exchange="binance",
                    symbol="BTCUSDT",
                    timeframe="1m",
                    start_ms=base_ms + minute * 60_000,
                    end_ms=base_ms + (minute + 1) * 60_000 - 1,
                    open_price=100.0 + minute,
                    high_price=101.0 + minute,
                    low_price=99.0,
                    close_price=100.5 + minute,
                    volume=10.0,
                    quote_volume=1000.0,
                    trade_count=2,
                    last_tick_ms=base_ms + (minute + 1) * 60_000 - 2,
                    is_final=True,
                )
            )
        )

    assert [bar.timeframe for bar in updates[0].current_bars] == ["5m", "15m"]
    assert any(bar.timeframe == "5m" for bar in updates[3].current_bars)

    final_5m = [bar for bar in updates[4].finalized_bars if bar.timeframe == "5m"]
    assert len(final_5m) == 1
    assert final_5m[0].open_price == 100.0
    assert final_5m[0].close_price == 104.5
    assert final_5m[0].volume == 50.0
    assert final_5m[0].is_final is True

    current_15m = [bar for bar in updates[4].current_bars if bar.timeframe == "15m"]
    assert len(current_15m) == 1
    assert current_15m[0].is_final is False
