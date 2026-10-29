import pytest

from crypto_ticket.timeframes import TIMEFRAME_ORDER, floor_bucket_start_ms, next_bucket_start_ms, normalize_timeframe


def test_1m_bucket_rounding():
    assert floor_bucket_start_ms(1716190033123, "1m") == 1716190020000


def test_month_bucket_rounding():
    ts_ms = 1746057600000  # 2025-05-01 UTC
    assert floor_bucket_start_ms(ts_ms, "3M") == 1743465600000


def test_retired_3d_is_not_supported():
    assert "3D" not in TIMEFRAME_ORDER
    with pytest.raises(ValueError, match="unsupported timeframe"):
        normalize_timeframe("3D")

