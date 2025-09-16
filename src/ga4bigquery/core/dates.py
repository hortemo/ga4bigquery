"""Helpers for handling date ranges and time buckets."""

from __future__ import annotations

from datetime import date
from typing import Literal, Tuple

import pandas as pd


def _parse_date_range(start: date, end: date, tz: str) -> Tuple[pd.Timestamp, pd.Timestamp]:
    """Return timezone aware timestamps covering the inclusive date range."""

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
    """Return SQL expressions for truncating timestamps to the desired bucket."""

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


def _table_suffix_condition(table_id: str, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> str | None:
    """Return the ``_TABLE_SUFFIX`` predicate for wildcard tables, if needed."""

    if not table_id.endswith("*"):
        return None

    lo = start_ts.tz_convert("UTC").date().strftime("%Y%m%d")
    hi = end_ts.tz_convert("UTC").date().strftime("%Y%m%d")
    return "REGEXP_EXTRACT(_TABLE_SUFFIX, r'(\\d+)$') BETWEEN '{lo}' AND '{hi}'".format(lo=lo, hi=hi)
