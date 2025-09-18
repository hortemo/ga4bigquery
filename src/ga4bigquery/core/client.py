"""Primary client for issuing GA4 queries against BigQuery."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, timedelta
from typing import Literal

import pandas as pd
from google.cloud import bigquery

from .dates import _build_interval_columns, _parse_date_range, _table_suffix_condition
from .filters import _parse_filters
from .group_by import _parse_group_by
from .sql import format_literal, format_literal_list
from .types import EventFilter, FunnelStep


class GA4BigQuery:
    """Minimal GA4-on-BigQuery client focused on event and funnel requests."""

    def __init__(
        self,
        table_id: str,
        *,
        tz: str = "UTC",
        user_id_col: str = "user_pseudo_id",
        client: bigquery.Client | None = None,
    ) -> None:
        self.table_id = table_id
        self.tz = tz
        self.user_id_col = user_id_col
        self.client = client or bigquery.Client()

    def _query(self, sql: str) -> pd.DataFrame:
        """Execute ``sql`` and return the resulting dataframe."""

        return self.client.query(sql).result().to_dataframe()

    def request_events(
        self,
        *,
        events: str | Sequence[str],
        start: date,
        end: date,
        measure: Literal["totals", "uniques"] = "totals",
        formula: str | None = None,
        filters: Sequence[EventFilter] | None = None,
        group_by: str | Sequence[str] | None = None,
        interval: Literal["day", "hour", "week", "month"] = "day",
    ) -> pd.DataFrame:
        """Return a time series of event metrics."""

        normalized_events = self._normalize_events(events)
        start_ts, end_ts = _parse_date_range(start, end, self.tz)
        group_by_list = self._normalize_group_by(group_by)
        custom_selects, custom_aliases = _parse_group_by(group_by_list)
        interval_select, interval_alias, order_col = _build_interval_columns(
            interval, self.tz
        )
        metric = self._metric_expression(measure, self.user_id_col, formula)

        select_cols = [
            interval_select,
            "event_name",
            f"{metric} AS value",
            *custom_selects,
        ]

        where_parts = [
            self._event_name_condition(normalized_events),
            *self._compile_filters(filters),
            *self._table_suffix_clauses(start_ts, end_ts),
            self._timestamp_condition(start_ts, end_ts),
        ]
        where_clause = " AND ".join(f"({clause})" for clause in where_parts)
        group_cols = [interval_alias, "event_name", *custom_aliases]
        sql = f"""
        SELECT {', '.join(select_cols)}
        FROM `{self.table_id}`
        WHERE {where_clause}
        GROUP BY {', '.join(group_cols)}
        ORDER BY {order_col} ASC
        """

        df = self._prepare_result_dataframe(self._query(sql), interval_alias)
        return self._pivot_events_dataframe(
            df=df,
            interval_alias=interval_alias,
            custom_aliases=custom_aliases,
            events=normalized_events,
        )

    def request_funnel(
        self,
        *,
        steps: Sequence[FunnelStep],
        start: date,
        end: date,
        group_by: str | Sequence[str] | None = None,
        interval: Literal["day", "hour", "week", "month"] = "day",
    ) -> pd.DataFrame:
        """Return conversion counts for a funnel across time."""

        if not steps:
            raise ValueError("steps must contain at least one funnel step")

        start_ts, end_ts = _parse_date_range(start, end, self.tz)
        group_by_list = self._normalize_group_by(group_by)
        custom_selects, custom_aliases = _parse_group_by(group_by_list)
        interval_select, interval_alias, order_col = _build_interval_columns(
            interval, self.tz
        )

        ctes: list[str] = []
        cumulative_gt = timedelta(seconds=0)
        cumulative_lt = timedelta(seconds=0)

        for idx, step in enumerate(steps, start=1):
            if idx == 1:
                step_start, step_end = start_ts, end_ts
            else:
                cumulative_gt += step.conversion_window_gt
                cumulative_lt += step.conversion_window_lt
                step_start = start_ts + cumulative_gt
                step_end = end_ts + cumulative_lt

            select_fields = [self.user_id_col, "event_timestamp"]
            if idx == 1:
                select_fields.append(interval_select)
                if custom_selects:
                    select_fields.extend(custom_selects)

            wheres = [
                self._event_name_condition(step.event_name),
                *self._compile_filters(step.filters),
                *self._table_suffix_clauses(step_start, step_end),
                self._timestamp_condition(step_start, step_end),
            ]

            ctes.append(
                "\n".join(
                    [
                        f"step{idx} AS (",
                        f"  SELECT {', '.join(select_fields)}",
                        f"  FROM `{self.table_id}`",
                        f"  WHERE {' AND '.join(wheres)}",
                        ")",
                    ]
                )
            )

        joins = []
        for idx in range(2, len(steps) + 1):
            step = steps[idx - 1]
            gt_us = int(step.conversion_window_gt.total_seconds() * 1_000_000)
            lt_us = int(step.conversion_window_lt.total_seconds() * 1_000_000)
            joins.append(
                "\n".join(
                    [
                        f"LEFT JOIN step{idx}",
                        f"       ON step{idx}.{self.user_id_col} = step{idx-1}.{self.user_id_col}",
                        f"      AND step{idx}.event_timestamp - step{idx-1}.event_timestamp > {gt_us}",
                        f"      AND step{idx}.event_timestamp - step{idx-1}.event_timestamp < {lt_us}",
                    ]
                )
            )
        step_cols = [
            f"COUNT(DISTINCT step{idx}.{self.user_id_col}) AS `{idx}`"
            for idx in range(1, len(steps) + 1)
        ]

        if custom_aliases:
            select_suffix = ", " + ", ".join(custom_aliases)
            group_suffix = ", " + ", ".join(custom_aliases)
        else:
            select_suffix = ""
            group_suffix = ""

        sql = "\n".join(
            [
                "WITH",
                ",\n".join(ctes),
                "",
                "SELECT",
                f"  {interval_alias}{select_suffix},",
                f"  {', '.join(step_cols)}",
                "FROM step1",
                "\n".join(joins),
                f"GROUP BY {interval_alias}{group_suffix}",
                f"ORDER BY {order_col} ASC",
            ]
        ).strip()

        df = self._prepare_result_dataframe(self._query(sql), interval_alias)
        return self._pivot_funnel_dataframe(
            df=df,
            interval_alias=interval_alias,
            custom_aliases=custom_aliases,
            step_count=len(steps),
        )

    @staticmethod
    def _compile_filters(filters: Sequence[EventFilter] | None) -> list[str]:
        return _parse_filters(filters)

    @staticmethod
    def _prepare_result_dataframe(
        df: pd.DataFrame, interval_alias: str
    ) -> pd.DataFrame:
        df[interval_alias] = pd.to_datetime(df[interval_alias])
        return df

    @staticmethod
    def _normalize_group_by(group_by: str | Sequence[str] | None) -> list[str]:
        if group_by is None:
            return []
        if isinstance(group_by, str):
            return [group_by]
        return list(group_by)

    @staticmethod
    def _normalize_events(events: str | Sequence[str]) -> list[str]:
        if isinstance(events, str):
            return [events]
        return list(events)

    @staticmethod
    def _event_name_condition(events: str | Sequence[str]) -> str:
        normalized_events = GA4BigQuery._normalize_events(events)
        return f"event_name IN {format_literal_list(normalized_events)}"

    @staticmethod
    def _metric_expression(
        measure: Literal["totals", "uniques"], user_id_col: str, formula: str | None
    ) -> str:
        if formula is not None:
            return formula
        if measure == "uniques":
            return f"COUNT(DISTINCT {user_id_col})"
        return "COUNT(*)"

    @staticmethod
    def _timestamp_condition(start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> str:
        return (
            "TIMESTAMP_MICROS(event_timestamp) BETWEEN TIMESTAMP('{start}') AND TIMESTAMP('{end}')"
        ).format(start=start_ts.isoformat(), end=end_ts.isoformat())

    @staticmethod
    def _pivot_events_dataframe(
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

    @staticmethod
    def _pivot_funnel_dataframe(
        df: pd.DataFrame,
        interval_alias: str,
        custom_aliases: Sequence[str],
        step_count: int,
    ) -> pd.DataFrame:
        values_cols = [str(i) for i in range(1, step_count + 1)]

        if custom_aliases:
            pivot = df.pivot_table(
                index=interval_alias,
                columns=custom_aliases,
                values=values_cols,
                fill_value=0,
            )
            return pivot.sort_index(axis=1)

        return df.set_index(interval_alias)[values_cols].sort_index()

    def _table_suffix_clauses(
        self, start_ts: pd.Timestamp, end_ts: pd.Timestamp
    ) -> tuple[str, ...]:
        clause = _table_suffix_condition(self.table_id, start_ts, end_ts)
        return (clause,) if clause else tuple()
