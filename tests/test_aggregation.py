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

