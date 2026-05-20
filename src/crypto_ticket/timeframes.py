from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable


UTC = timezone.utc
MILLISECOND = 1
SECOND_MS = 1000
MINUTE_MS = 60 * SECOND_MS
HOUR_MS = 60 * MINUTE_MS
DAY_MS = 24 * HOUR_MS
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
EPOCH_MONDAY = datetime(1970, 1, 5, tzinfo=UTC)

FIXED_MINUTE_FRAMES: dict[str, int] = {
    "1m": 1,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1H": 60,
    "2H": 120,
    "4H": 240,
    "6H": 360,
    "12H": 720,
}

DAY_FRAMES: dict[str, int] = {
    "1D": 1,
    "2D": 2,
    "3D": 3,
    "5D": 5,
}

WEEK_FRAMES: dict[str, int] = {
    "1W": 1,
    "2W": 2,
}

MONTH_FRAMES: dict[str, int] = {
    "1M": 1,
    "3M": 3,
}

TIMEFRAME_ORDER = (
    "1m",
    "5m",
    "15m",
    "30m",
    "1H",
    "2H",
    "4H",
    "6H",
    "12H",
    "1D",
    "2D",
    "3D",
    "5D",
    "1W",
    "2W",
    "1M",
    "3M",
)


def normalize_timeframe(timeframe: str) -> str:
    value = str(timeframe or "").strip()
    if value not in TIMEFRAME_ORDER:
        raise ValueError(f"unsupported timeframe: {timeframe}")
    return value


def timeframe_index(timeframe: str) -> int:
    return TIMEFRAME_ORDER.index(normalize_timeframe(timeframe))


def is_supported_timeframe(timeframe: str) -> bool:
    try:
        normalize_timeframe(timeframe)
        return True
    except ValueError:
        return False


def timeframe_to_ms(timeframe: str) -> int:
    tf = normalize_timeframe(timeframe)
    if tf in FIXED_MINUTE_FRAMES:
        return FIXED_MINUTE_FRAMES[tf] * MINUTE_MS
    if tf in DAY_FRAMES:
        return DAY_FRAMES[tf] * DAY_MS
    if tf in WEEK_FRAMES:
        return WEEK_FRAMES[tf] * 7 * DAY_MS
    raise ValueError(f"timeframe does not map to a fixed duration: {timeframe}")


def floor_bucket_start_ms(ts_ms: int, timeframe: str) -> int:
    tf = normalize_timeframe(timeframe)
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=UTC)
    if tf in FIXED_MINUTE_FRAMES:
        duration_ms = timeframe_to_ms(tf)
        return (ts_ms // duration_ms) * duration_ms
    if tf in DAY_FRAMES:
        day_start = datetime(dt.year, dt.month, dt.day, tzinfo=UTC)
        start_index = day_start.timestamp() * 1000
        duration_ms = timeframe_to_ms(tf)
        return int((int(start_index) // duration_ms) * duration_ms)
    if tf in WEEK_FRAMES:
        monday = dt.date() - timedelta(days=dt.weekday())
        monday_dt = datetime(monday.year, monday.month, monday.day, tzinfo=UTC)
        base_days = int((monday_dt - EPOCH_MONDAY).total_seconds() // 86400)
        period_days = WEEK_FRAMES[tf] * 7
        bucket_days = (base_days // period_days) * period_days
        bucket_start = EPOCH_MONDAY + timedelta(days=bucket_days)
        return int(bucket_start.timestamp() * 1000)
    if tf in MONTH_FRAMES:
        month_index = (dt.year * 12) + (dt.month - 1)
        period = MONTH_FRAMES[tf]
        bucket_index = (month_index // period) * period
        year = bucket_index // 12
        month = (bucket_index % 12) + 1
        bucket_start = datetime(year, month, 1, tzinfo=UTC)
        return int(bucket_start.timestamp() * 1000)
    raise ValueError(f"unsupported timeframe: {timeframe}")


def next_bucket_start_ms(start_ms: int, timeframe: str) -> int:
    tf = normalize_timeframe(timeframe)
    dt = datetime.fromtimestamp(start_ms / 1000, tz=UTC)
    if tf in FIXED_MINUTE_FRAMES:
        return start_ms + timeframe_to_ms(tf)
    if tf in DAY_FRAMES:
        return int((dt + timedelta(days=DAY_FRAMES[tf])).timestamp() * 1000)
    if tf in WEEK_FRAMES:
        return int((dt + timedelta(days=WEEK_FRAMES[tf] * 7)).timestamp() * 1000)
    if tf in MONTH_FRAMES:
        return int(_add_months(dt, MONTH_FRAMES[tf]).timestamp() * 1000)
    raise ValueError(f"unsupported timeframe: {timeframe}")


def bucket_end_ms(start_ms: int, timeframe: str) -> int:
    return next_bucket_start_ms(start_ms, timeframe) - 1


def partition_week_key(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=UTC)
    iso_year, iso_week, _ = dt.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"


def partition_year_week(ts_ms: int) -> tuple[int, int]:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=UTC)
    iso_year, iso_week, _ = dt.isocalendar()
    return iso_year, iso_week


def _add_months(dt: datetime, months: int) -> datetime:
    month_index = (dt.year * 12) + (dt.month - 1) + months
    year = month_index // 12
    month = (month_index % 12) + 1
    last_day = monthrange(year, month)[1]
    day = min(dt.day, last_day)
    return datetime(
        year,
        month,
        day,
        dt.hour,
        dt.minute,
        dt.second,
        dt.microsecond,
        tzinfo=UTC,
    )

