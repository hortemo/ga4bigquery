"""Standalone arguments function for GA4 funnel queries."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from typing import Literal

import pandas as pd
from google.cloud import bigquery

from .helpers import prepare_result_dataframe
from .query_builders import FunnelQueryBuilder
from .types import FunnelStep


def request_funnel(
    *,
    client: bigquery.Client,
    table_id: str,
    tz: str,
    user_id_col: str,
    steps: Sequence[FunnelStep],
    start: date,
    end: date,
    group_by: str | Sequence[str] | None = None,
    interval: Literal["day", "hour", "week", "month"] = "day",
) -> pd.DataFrame:
    """Return conversion counts for ``steps`` executed against ``table_id``.

    Args:
        client: BigQuery client used to execute the generated SQL.
        table_id: Fully-qualified BigQuery table containing GA4 export data.
        tz: IANA timezone identifier used for date boundaries.
        user_id_col: Column containing the user identifier used for deduping.
        steps: Ordered collection of funnel steps that define the sequence.
        start: Inclusive start date for the funnel window in ``tz``.
        end: Inclusive end date for the funnel window in ``tz``.
        group_by: Dimensions used to split funnel results (optional).
        interval: Time bucketing granularity for the aggregated counts.

    Returns:
        DataFrame containing one row per interval with funnel step counts. The
        DataFrame is pivoted by the requested grouping dimensions when present.

    Raises:
        ValueError: If ``steps`` is empty.
    """

    builder = FunnelQueryBuilder(
        table_id=table_id,
        tz=tz,
        user_id_col=user_id_col,
        steps=steps,
        start=start,
        end=end,
        group_by=group_by,
        interval=interval,
    )
    rendered = builder.build()

    df = client.query(rendered.sql).result().to_dataframe()
    df = prepare_result_dataframe(df, rendered.interval_alias)
    return pivot_funnel_dataframe(
        df=df,
        interval_alias=rendered.interval_alias,
        group_by_aliases=rendered.group_by_aliases,
        step_count=rendered.step_count,
    )


def pivot_funnel_dataframe(
    *,
    df: pd.DataFrame,
    interval_alias: str,
    group_by_aliases: Sequence[str],
    step_count: int,
) -> pd.DataFrame:
    values_cols = [str(i) for i in range(1, step_count + 1)]

    if group_by_aliases:
        pivot = df.pivot_table(
            index=interval_alias,
            columns=group_by_aliases,
            values=values_cols,
            fill_value=0,
        )
        return pivot.sort_index(axis=1)

    return df.set_index(interval_alias)[values_cols].sort_index()


__all__ = [
    "pivot_funnel_dataframe",
    "request_funnel",
]
