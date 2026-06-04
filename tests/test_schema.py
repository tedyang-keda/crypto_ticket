from pathlib import Path


def test_timeframe_columns_are_case_sensitive():
    schema = (Path(__file__).resolve().parents[1] / "sql" / "schema.sql").read_text()

    assert schema.count("timeframe VARCHAR(8) CHARACTER SET ascii COLLATE ascii_bin NOT NULL") == 4
    assert "PARTITION BY RANGE COLUMNS(timeframe, start_ms)" in schema
    assert "p_tf_1min_2026_01" in schema
    assert "p_tf_1mon_future" in schema
    assert "p_tf_1mon_2026_01" not in schema
    assert "kline_guardian_state" in schema
    assert "kline_guardian_event" in schema
