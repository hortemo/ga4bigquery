"""Standalone request function for GA4 event metrics."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from typing import Literal

import pandas as pd
from google.cloud import bigquery

from .dates import _build_interval_columns, _parse_date_range
from .group_by import _parse_group_by
from .query_helpers import (
    compile_filters,
    event_name_condition,
    normalize_events,
    normalize_group_by,
    prepare_result_dataframe,
    table_suffix_clauses,
    join_where_clauses,
    timestamp_condition,
)
from .types import EventFilter


def request_events(
    *,
    client: bigquery.Client,
    table_id: str,
    tz: str,
    user_id_col: str,
    events: str | Sequence[str],
    start: date,
    end: date,
    measure: Literal["totals", "uniques"] = "totals",
    formula: str | None = None,
    filters: Sequence[EventFilter] | None = None,
    group_by: str | Sequence[str] | None = None,
    interval: Literal["day", "hour", "week", "month"] = "day",
) -> pd.DataFrame:
    """Return a time series of event metrics for ``table_id``."""

    normalized_events = normalize_events(events)
    start_ts, end_ts = _parse_date_range(start, end, tz)
    group_by_list = normalize_group_by(group_by)
    custom_selects, custom_aliases = _parse_group_by(group_by_list)
    interval_select, interval_alias, order_col = _build_interval_columns(interval, tz)
    metric = metric_expression(measure, user_id_col, formula)

    select_cols = [
        interval_select,
        "event_name",
        f"{metric} AS value",
        *custom_selects,
    ]

    where_parts = [
        event_name_condition(normalized_events),
        *compile_filters(filters),
        *table_suffix_clauses(table_id, start_ts, end_ts),
        timestamp_condition(start_ts, end_ts),
    ]

    group_cols = [interval_alias, "event_name", *custom_aliases]

    sql = f"""
SELECT {', '.join(select_cols)}
FROM `{table_id}`
WHERE {join_where_clauses(where_parts)}
GROUP BY {', '.join(group_cols)}
ORDER BY {order_col} ASC
"""

    df = client.query(sql).result().to_dataframe()
    df = prepare_result_dataframe(df, interval_alias)
    return pivot_events_dataframe(
        df=df,
        interval_alias=interval_alias,
        custom_aliases=custom_aliases,
        events=normalized_events,
    )


def metric_expression(
    measure: Literal["totals", "uniques"], user_id_col: str, formula: str | None
) -> str:
    if formula is not None:
        return formula
    if measure == "uniques":
        return f"COUNT(DISTINCT {user_id_col})"
    return "COUNT(*)"


def pivot_events_dataframe(
    *,
    df: pd.DataFrame,
    interval_alias: str,
    custom_aliases: Sequence[str],
    events: Sequence[str],
) -> pd.DataFrame:
    columns = list(custom_aliases)
    if len(events) > 1:
        columns.append("event_name")

    if columns:
        pivot = df.pivot_table(
            values="value",
            index=interval_alias,
            columns=columns,
            fill_value=0,
        )
        return pivot.sort_index(axis=1)

    out = df[[interval_alias, "value"]].set_index(interval_alias).sort_index()
    out.index.name = interval_alias
    return out


__all__ = [
    "metric_expression",
    "pivot_events_dataframe",
    "request_events",
]
