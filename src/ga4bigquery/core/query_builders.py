"""Helpers that compile GA4 queries into executable SQL statements."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from collections.abc import Sequence
from typing import Literal

import pandas as pd

from .helpers import (
    _build_interval_columns,
    _parse_date_range,
    _parse_group_by,
    compile_filters,
    event_name_condition,
    join_where_clauses,
    normalize_events,
    normalize_group_by,
    table_suffix_clauses,
    timestamp_condition,
)
from .types import EventFilter, FunnelStep


@dataclass(frozen=True)
class RenderedEventsQuery:
    """SQL statement plus metadata required for post-processing."""

    sql: str
    interval_alias: str
    group_by_aliases: tuple[str, ...]
    events: tuple[str, ...]
    pivot_by_event_name: bool


@dataclass(frozen=True)
class RenderedFunnelQuery:
    """SQL statement plus metadata required for post-processing."""

    sql: str
    interval_alias: str
    group_by_aliases: tuple[str, ...]
    step_count: int


def metric_expression(
    measure: Literal["totals", "uniques"],
    user_id_col: str,
    formula: str | None,
) -> str:
    """Return SQL expression representing the requested metric."""

    if formula is not None:
        return formula
    if measure == "uniques":
        return f"COUNT(DISTINCT {user_id_col})"
    return "COUNT(*)"


class EventQueryBuilder:
    """Compile ``request_events`` arguments into a BigQuery SQL statement."""

    def __init__(
        self,
        *,
        table_id: str,
        tz: str,
        user_id_col: str,
        events: str | Sequence[str],
        start: date,
        end: date,
        measure: Literal["totals", "uniques"],
        formula: str | None,
        filters: Sequence[EventFilter] | None,
        group_by: str | Sequence[str] | None,
        interval: Literal["day", "hour", "week", "month"],
    ) -> None:
        self.table_id = table_id
        self.tz = tz
        self.user_id_col = user_id_col
        self.events = events
        self.start = start
        self.end = end
        self.measure = measure
        self.formula = formula
        self.filters = filters
        self.group_by = group_by
        self.interval = interval

    def build(self) -> RenderedEventsQuery:
        events = tuple(normalize_events(self.events))
        start_ts, end_ts = _parse_date_range(self.start, self.end, self.tz)

        group_by = normalize_group_by(self.group_by)
        group_by_selects, group_by_aliases = _parse_group_by(group_by)
        pivot_by_event_name = self.formula is None

        interval_select, interval_alias, order_col = _build_interval_columns(
            self.interval, self.tz
        )

        metric = metric_expression(self.measure, self.user_id_col, self.formula)
        selects = [interval_select, f"{metric} AS value", *group_by_selects]
        if pivot_by_event_name:
            selects.insert(1, "event_name")

        wheres = [
            event_name_condition(events),
            *compile_filters(self.filters),
            *table_suffix_clauses(self.table_id, start_ts, end_ts),
            timestamp_condition(start_ts, end_ts),
        ]

        group_bys = [interval_alias, *group_by_aliases]
        if pivot_by_event_name:
            group_bys.insert(1, "event_name")

        sql = f"""
        SELECT {', '.join(selects)}
        FROM `{self.table_id}`
        WHERE {join_where_clauses(wheres)}
        GROUP BY {', '.join(group_bys)}
        ORDER BY {order_col} ASC
        """

        return RenderedEventsQuery(
            sql=sql,
            interval_alias=interval_alias,
            group_by_aliases=tuple(group_by_aliases),
            events=events,
            pivot_by_event_name=pivot_by_event_name,
        )


class FunnelQueryBuilder:
    """Compile ``request_funnel`` arguments into a BigQuery SQL statement."""

    def __init__(
        self,
        *,
        table_id: str,
        tz: str,
        user_id_col: str,
        steps: Sequence[FunnelStep],
        start: date,
        end: date,
        group_by: str | Sequence[str] | None,
        interval: Literal["day", "hour", "week", "month"],
    ) -> None:
        self.table_id = table_id
        self.tz = tz
        self.user_id_col = user_id_col
        self.steps = steps
        self.start = start
        self.end = end
        self.group_by = group_by
        self.interval = interval

    def build(self) -> RenderedFunnelQuery:
        if not self.steps:
            raise ValueError("steps must contain at least one funnel step")

        start_ts, end_ts = _parse_date_range(self.start, self.end, self.tz)

        group_by = normalize_group_by(self.group_by)
        group_by_selects, group_by_aliases = _parse_group_by(group_by)

        interval_select, interval_alias, interval_order_by = _build_interval_columns(
            self.interval, self.tz
        )

        ctes = self._build_step_ctes(
            group_by_selects=group_by_selects,
            interval_select=interval_select,
            start_ts=start_ts,
            end_ts=end_ts,
        )

        joins = self._build_step_joins()
        step_cols = [
            f"COUNT(DISTINCT step{idx}.{self.user_id_col}) AS `{idx}`"
            for idx in range(1, len(self.steps) + 1)
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

        return RenderedFunnelQuery(
            sql=sql,
            interval_alias=interval_alias,
            group_by_aliases=tuple(group_by_aliases),
            step_count=len(self.steps),
        )

    def _build_step_ctes(
        self,
        *,
        group_by_selects: Sequence[str],
        interval_select: str,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
    ) -> list[str]:
        ctes: list[str] = []
        cumulative_gt = timedelta(seconds=0)
        cumulative_lt = timedelta(seconds=0)

        for idx, step in enumerate(self.steps, start=1):
            if idx == 1:
                step_start, step_end = start_ts, end_ts
            else:
                cumulative_gt += step.conversion_window_gt
                cumulative_lt += step.conversion_window_lt
                step_start = start_ts + cumulative_gt
                step_end = end_ts + cumulative_lt

            step_selects = [self.user_id_col, "event_timestamp"]
            if idx == 1:
                step_selects.append(interval_select)
                if group_by_selects:
                    step_selects.extend(group_by_selects)

            step_wheres = [
                event_name_condition(step.event_name),
                *compile_filters(step.filters),
                *table_suffix_clauses(self.table_id, step_start, step_end),
                timestamp_condition(step_start, step_end),
            ]

            ctes.append(
                f"""step{idx} AS (
  SELECT {', '.join(step_selects)}
  FROM `{self.table_id}`
  WHERE {' AND '.join(step_wheres)}
)"""
            )

        return ctes

    def _build_step_joins(self) -> list[str]:
        joins: list[str] = []
        for idx in range(2, len(self.steps) + 1):
            step = self.steps[idx - 1]
            gt_us = int(step.conversion_window_gt.total_seconds() * 1_000_000)
            lt_us = int(step.conversion_window_lt.total_seconds() * 1_000_000)
            joins.append(
                f"""LEFT JOIN step{idx}
            ON step{idx}.{self.user_id_col} = step{idx-1}.{self.user_id_col}
            AND step{idx}.event_timestamp - step{idx-1}.event_timestamp > {gt_us}
            AND step{idx}.event_timestamp - step{idx-1}.event_timestamp < {lt_us}"""
            )
        return joins


__all__ = [
    "EventQueryBuilder",
    "FunnelQueryBuilder",
    "RenderedEventsQuery",
    "RenderedFunnelQuery",
    "metric_expression",
]
