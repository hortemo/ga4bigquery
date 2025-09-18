"""Shared helper utilities for GA4 BigQuery query construction."""

from __future__ import annotations

from collections.abc import Sequence

import pandas as pd

from .dates import _table_suffix_condition
from .filters import _parse_filters
from .sql import format_literal_list
from .types import EventFilter


def normalize_group_by(group_by: str | Sequence[str] | None) -> list[str]:
    if group_by is None:
        return []
    if isinstance(group_by, str):
        return [group_by]
    return list(group_by)


def normalize_events(events: str | Sequence[str]) -> list[str]:
    if isinstance(events, str):
        return [events]
    return list(events)


def event_name_condition(events: str | Sequence[str]) -> str:
    normalized_events = normalize_events(events)
    return f"event_name IN {format_literal_list(normalized_events)}"


def compile_filters(filters: Sequence[EventFilter] | None) -> list[str]:
    return _parse_filters(filters)


def join_where_clauses(clauses: Sequence[str], *, operator: str = "AND") -> str:
    """Join ``clauses`` with ``operator`` while wrapping each clause in parentheses."""

    joined = f" {operator} ".join(f"({clause})" for clause in clauses)
    return joined


def join_where_clauses_or(clauses: Sequence[str]) -> str:
    return join_where_clauses(clauses, operator="OR")


def timestamp_condition(start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> str:
    return (
        "TIMESTAMP_MICROS(event_timestamp) BETWEEN "
        "TIMESTAMP('{start}') AND TIMESTAMP('{end}')"
    ).format(start=start_ts.isoformat(), end=end_ts.isoformat())


def table_suffix_clauses(
    table_id: str, start_ts: pd.Timestamp, end_ts: pd.Timestamp
) -> tuple[str, ...]:
    clause = _table_suffix_condition(table_id, start_ts, end_ts)
    return (clause,) if clause else tuple()


def prepare_result_dataframe(df: pd.DataFrame, interval_alias: str) -> pd.DataFrame:
    df[interval_alias] = pd.to_datetime(df[interval_alias])
    return df


__all__ = [
    "compile_filters",
    "event_name_condition",
    "normalize_events",
    "normalize_group_by",
    "prepare_result_dataframe",
    "table_suffix_clauses",
    "join_where_clauses",
    "join_where_clauses_or",
    "timestamp_condition",
]
