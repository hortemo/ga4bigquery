"""Standalone arguments function for GA4 funnel queries."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, timedelta
from typing import Literal

import pandas as pd
from google.cloud import bigquery

from .dates import _build_interval_columns, _parse_date_range
from .group_by import _parse_group_by
from .query_helpers import (
    compile_filters,
    event_name_condition,
    normalize_group_by,
    prepare_result_dataframe,
    table_suffix_clauses,
    timestamp_condition,
)
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

    if not steps:
        raise ValueError("steps must contain at least one funnel step")

    start, end = _parse_date_range(start, end, tz)

    group_by = normalize_group_by(group_by)
    group_by_selects, group_by_aliases = _parse_group_by(group_by)

    interval_select, interval_alias, interval_order_by = _build_interval_columns(
        interval, tz
    )

    ctes: list[str] = []
    cumulative_gt = timedelta(seconds=0)
    cumulative_lt = timedelta(seconds=0)

    for idx, step in enumerate(steps, start=1):
        if idx == 1:
            step_start, step_end = start, end
        else:
            cumulative_gt += step.conversion_window_gt
            cumulative_lt += step.conversion_window_lt
            step_start = start + cumulative_gt
            step_end = end + cumulative_lt

        step_selects = [user_id_col, "event_timestamp"]
        if idx == 1:
            step_selects.append(interval_select)
            if group_by_selects:
                step_selects.extend(group_by_selects)

        step_wheres = [
            event_name_condition(step.event_name),
            *compile_filters(step.filters),
            *table_suffix_clauses(table_id, step_start, step_end),
            timestamp_condition(step_start, step_end),
        ]

        ctes.append(
            f"""step{idx} AS (
  SELECT {', '.join(step_selects)}
  FROM `{table_id}`
  WHERE {' AND '.join(step_wheres)}
)"""
        )

    joins = []
    for idx in range(2, len(steps) + 1):
        step = steps[idx - 1]
        gt_us = int(step.conversion_window_gt.total_seconds() * 1_000_000)
        lt_us = int(step.conversion_window_lt.total_seconds() * 1_000_000)
        joins.append(
            f"""LEFT JOIN step{idx}
            ON step{idx}.{user_id_col} = step{idx-1}.{user_id_col}
            AND step{idx}.event_timestamp - step{idx-1}.event_timestamp > {gt_us}
            AND step{idx}.event_timestamp - step{idx-1}.event_timestamp < {lt_us}"""
        )

    step_cols = [
        f"COUNT(DISTINCT step{idx}.{user_id_col}) AS `{idx}`"
        for idx in range(1, len(steps) + 1)
    ]

    selects = [interval_alias, *group_by_aliases, *step_cols]
    group_bys = [interval_alias, *group_by_aliases]

    sql = f"""WITH
    {',\n'.join(ctes)}

    SELECT {', '.join(selects)}
    FROM step1
    {'\n'.join(joins)}
    GROUP BY {', '.join(group_bys)}
    ORDER BY {interval_order_by} ASC
    """.strip()

    df = client.query(sql).result().to_dataframe()
    df = prepare_result_dataframe(df, interval_alias)
    return pivot_funnel_dataframe(
        df=df,
        interval_alias=interval_alias,
        group_by_aliases=group_by_aliases,
        step_count=len(steps),
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
