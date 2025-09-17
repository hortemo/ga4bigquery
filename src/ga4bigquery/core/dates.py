"""Helpers for handling date ranges and time buckets."""

from __future__ import annotations

from dataclasses import dataclass
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


@dataclass(frozen=True)
class _IntervalSpec:
    expression_template: str
    alias: str

    def render(self, tz: str) -> Tuple[str, str, str]:
        expression = self.expression_template.format(tz=tz, alias=self.alias)
        return expression, self.alias, self.alias


_INTERVAL_SPECS = {
    "day": _IntervalSpec(
        "FORMAT_DATE('%Y-%m-%d', DATE(TIMESTAMP_MICROS(event_timestamp), '{tz}')) AS {alias}",
        "event_date",
    ),
    "hour": _IntervalSpec(
        "FORMAT_TIMESTAMP('%Y-%m-%d %H:00:00', "
        "TIMESTAMP_TRUNC(TIMESTAMP_MICROS(event_timestamp), HOUR, '{tz}'), '{tz}') AS {alias}",
        "event_hour",
    ),
    "week": _IntervalSpec(
        "FORMAT_DATE('%Y-%m-%d', "
        "DATE_TRUNC(DATE(TIMESTAMP_MICROS(event_timestamp), '{tz}'), WEEK(MONDAY))) AS {alias}",
        "event_week",
    ),
    "month": _IntervalSpec(
        "FORMAT_DATE('%Y-%m', "
        "DATE_TRUNC(DATE(TIMESTAMP_MICROS(event_timestamp), '{tz}'), MONTH)) AS {alias}",
        "event_month",
    ),
}

_INTERVAL_ALIASES = {"date": "day"}


def _build_interval_columns(interval: Literal["day", "hour", "week", "month"], tz: str):
    """Return SQL expressions for truncating timestamps to the desired bucket."""

    normalized = interval.lower()
    normalized = _INTERVAL_ALIASES.get(normalized, normalized)
    spec = _INTERVAL_SPECS.get(normalized)
    if spec is None:
        raise ValueError("interval must be one of: 'day', 'hour', 'week', 'month'")
    return spec.render(tz)


def _table_suffix_condition(table_id: str, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> str | None:
    """Return the ``_TABLE_SUFFIX`` predicate for wildcard tables, if needed."""

    if not table_id.endswith("*"):
        return None

    lo = start_ts.tz_convert("UTC").date().strftime("%Y%m%d")
    hi = end_ts.tz_convert("UTC").date().strftime("%Y%m%d")
    return "REGEXP_EXTRACT(_TABLE_SUFFIX, r'(\\d+)$') BETWEEN '{lo}' AND '{hi}'".format(lo=lo, hi=hi)
