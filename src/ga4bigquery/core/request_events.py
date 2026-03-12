"""Standalone arguments function for GA4 event metrics."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from typing import Literal

import pandas as pd
from google.cloud import bigquery

from .helpers import prepare_result_dataframe
from .query_builders import EventQueryBuilder, metric_expression
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
    """Return a time series of event metrics assembled from GA4 exports.

    Args:
        client: BigQuery client used to execute the query.
        table_id: Fully-qualified BigQuery table containing GA4 export data.
        tz: IANA timezone identifier used when bucketing timestamps.
        user_id_col: Column containing the user identifier used for uniques.
        events: Single event name or sequence of event names to aggregate.
        start: Inclusive start date for the query window in ``tz``.
        end: Inclusive end date for the query window in ``tz``.
        measure: Metric to calculate, either ``"totals"`` or ``"uniques"``.
        formula: Custom SQL expression overriding ``measure`` when provided.
        filters: Additional predicates applied to matching events.
        group_by: Dimensions to group by in addition to the interval.
        interval: Time bucketing granularity for returned rows.

    Returns:
        DataFrame containing one row per interval with event metrics pivoted by
        event name when multiple events are supplied.
    """

    builder = EventQueryBuilder(
        table_id=table_id,
        tz=tz,
        user_id_col=user_id_col,
        events=events,
        start=start,
        end=end,
        measure=measure,
        formula=formula,
        filters=filters,
        group_by=group_by,
        interval=interval,
    )
    rendered = builder.build()

    df = client.query(rendered.sql).result().to_dataframe()
    df = prepare_result_dataframe(df, rendered.interval_alias)
    return pivot_events_dataframe(
        df=df,
        interval_alias=rendered.interval_alias,
        group_by_aliases=rendered.group_by_aliases,
        events=rendered.events,
    )


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
    "metric_expression",
    "pivot_events_dataframe",
    "request_events",
]
