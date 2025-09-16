from __future__ import annotations

from datetime import date
from typing import Literal, Tuple

import pandas as pd


def _parse_date_range(start: date, end: date, tz: str) -> Tuple[pd.Timestamp, pd.Timestamp]:
    start_ts = (
        pd.Timestamp(start)
        .tz_localize(tz)
        .replace(hour=0, minute=0, second=0, microsecond=0)
    )
    end_ts = (
        pd.Timestamp(end)
        .tz_localize(tz)
        .replace(hour=23, minute=59, second=59, microsecond=999_999)
    )
    return start_ts, end_ts


def _build_interval_columns(interval: Literal["day", "hour", "week", "month"], tz: str):
    interval = interval.lower()

    if interval in {"day", "date"}:
        expr = (
            "FORMAT_DATE('%Y-%m-%d', DATE(TIMESTAMP_MICROS(event_timestamp), '{tz}')) AS event_date"
        ).format(tz=tz)
        return expr, "event_date", "event_date"

    if interval == "hour":
        expr = (
            "FORMAT_TIMESTAMP('%Y-%m-%d %H:00:00', "
            "TIMESTAMP_TRUNC(TIMESTAMP_MICROS(event_timestamp), HOUR, '{tz}'), '{tz}') AS event_hour"
        ).format(tz=tz)
        return expr, "event_hour", "event_hour"

    if interval == "week":
        expr = (
            "FORMAT_DATE('%Y-%m-%d', "
            "DATE_TRUNC(DATE(TIMESTAMP_MICROS(event_timestamp), '{tz}'), WEEK(MONDAY))) AS event_week"
        ).format(tz=tz)
        return expr, "event_week", "event_week"

    if interval == "month":
        expr = (
            "FORMAT_DATE('%Y-%m', "
            "DATE_TRUNC(DATE(TIMESTAMP_MICROS(event_timestamp), '{tz}'), MONTH)) AS event_month"
        ).format(tz=tz)
        return expr, "event_month", "event_month"

    raise ValueError("interval must be one of: 'day', 'hour', 'week', 'month'")
