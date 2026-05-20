from crypto_ticket.timeframes import floor_bucket_start_ms, next_bucket_start_ms


def test_1m_bucket_rounding():
    assert floor_bucket_start_ms(1716190033123, "1m") == 1716190020000


def test_month_bucket_rounding():
    ts_ms = 1746057600000  # 2025-05-01 UTC
    assert floor_bucket_start_ms(ts_ms, "3M") == 1743465600000

