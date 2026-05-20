from pathlib import Path


def test_timeframe_columns_are_case_sensitive():
    schema = (Path(__file__).resolve().parents[1] / "sql" / "schema.sql").read_text()

    assert schema.count("timeframe VARCHAR(8) CHARACTER SET ascii COLLATE ascii_bin NOT NULL") == 3
    assert "PARTITION BY RANGE COLUMNS(exchange, start_ms)" in schema
