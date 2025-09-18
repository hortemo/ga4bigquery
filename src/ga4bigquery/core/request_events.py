"""Standalone arguments function for GA4 event metrics."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from typing import Literal, NotRequired, TypedDict, Unpack

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


class RequestEventsSharedArguments(TypedDict):
    client: bigquery.Client
    table_id: str
    tz: str
    user_id_col: str


class RequestEventsSpecificArguments(TypedDict):
    events: str | Sequence[str]
    start: date
    end: date
    measure: NotRequired[Literal["totals", "uniques"]]
    formula: NotRequired[str | None]
    filters: NotRequired[Sequence[EventFilter] | None]
    group_by: NotRequired[str | Sequence[str] | None]
    interval: NotRequired[Literal["day", "hour", "week", "month"]]


class RequestEventsArguments(
    RequestEventsSharedArguments, RequestEventsSpecificArguments
):
    pass


def request_events(**arguments: Unpack[RequestEventsArguments]) -> pd.DataFrame:
    """Return a time series of event metrics."""

    client = arguments["client"]
    table_id = arguments["table_id"]
    tz = arguments["tz"]
    user_id_col = arguments["user_id_col"]

    events = normalize_events(arguments["events"])
    start, end = _parse_date_range(arguments["start"], arguments["end"], tz)

    group_by = normalize_group_by(arguments.get("group_by"))
    group_by_selects, group_by_aliases = _parse_group_by(group_by)

    interval = arguments.get("interval", "day")
    interval_select, interval_alias, order_col = _build_interval_columns(interval, tz)

    metric = metric_expression(
        arguments.get("measure", "totals"),
        user_id_col,
        arguments.get("formula"),
    )

    selects = [
        interval_select,
        "event_name",
        f"{metric} AS value",
        *group_by_selects,
    ]

    wheres = [
        event_name_condition(events),
        *compile_filters(arguments.get("filters")),
        *table_suffix_clauses(table_id, start, end),
        timestamp_condition(start, end),
    ]

    group_bys = [interval_alias, "event_name", *group_by_aliases]

    sql = f"""
SELECT {', '.join(selects)}
FROM `{table_id}`
WHERE {join_where_clauses(wheres)}
GROUP BY {', '.join(group_bys)}
ORDER BY {order_col} ASC
"""

    df = client.query(sql).result().to_dataframe()
    df = prepare_result_dataframe(df, interval_alias)
    return pivot_events_dataframe(
        df=df,
        interval_alias=interval_alias,
        group_by_aliases=group_by_aliases,
        events=events,
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
    group_by_aliases: Sequence[str],
    events: Sequence[str],
) -> pd.DataFrame:
    columns = list(group_by_aliases)
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
    "RequestEventsArguments",
    "RequestEventsSpecificArguments",
    "metric_expression",
    "pivot_events_dataframe",
    "request_events",
]
