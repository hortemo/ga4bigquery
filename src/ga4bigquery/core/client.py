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
        events: Sequence[str],
        start: date,
        end: date,
        measure: Literal["totals", "uniques"] = "totals",
        formula: str | None = None,
        filters: Sequence[EventFilter] | None = None,
        group_by: str | Sequence[str] | None = None,
        interval: Literal["day", "hour", "week", "month"] = "day",
    ) -> pd.DataFrame:
        """Return a time series of event metrics."""

        if not events:
            raise ValueError("events must contain at least one event name")

        start_ts, end_ts = _parse_date_range(start, end, self.tz)
        (
            interval_select,
            interval_alias,
            order_col,
            custom_selects,
            custom_aliases,
        ) = self._prepare_dimensions(group_by, interval)

        select_cols = self._build_event_select_columns(interval_select, measure, formula, custom_selects)

        sql = self._render_events_sql(
            select_cols=select_cols,
            from_clause=f"`{self.table_id}`",
            where_parts=self._build_event_where_parts(events, filters, start_ts, end_ts),
            group_cols=[interval_alias, "event_name"] + custom_aliases,
            order_col=order_col,
        )

        df = self._prepare_result_dataframe(self._query(sql), interval_alias)
        return self._pivot_events_dataframe(df, interval_alias, custom_aliases, events)

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
        (
            interval_select,
            interval_alias,
            order_col,
            custom_selects,
            custom_aliases,
        ) = self._prepare_dimensions(group_by, interval)

        ctes = self._build_funnel_ctes(
            steps=steps,
            start_ts=start_ts,
            end_ts=end_ts,
            interval_select=interval_select,
            custom_selects=custom_selects,
        )
        joins = [
            self._build_funnel_join_clause(idx, steps[idx - 1])
            for idx in range(2, len(steps) + 1)
        ]
        step_cols = [
            f"COUNT(DISTINCT step{idx}.{self.user_id_col}) AS `{idx}`"
            for idx in range(1, len(steps) + 1)
        ]

        sql = self._render_funnel_sql(
            ctes=ctes,
            interval_alias=interval_alias,
            custom_aliases=custom_aliases,
            step_cols=step_cols,
            joins=joins,
            order_col=order_col,
        )

        df = self._prepare_result_dataframe(self._query(sql), interval_alias)
        return self._pivot_funnel_dataframe(df, interval_alias, custom_aliases, len(steps))

    def _prepare_dimensions(
        self,
        group_by: str | Sequence[str] | None,
        interval: Literal["day", "hour", "week", "month"],
    ) -> tuple[str, str, str, list[str], list[str]]:
        group_by_list = self._normalize_group_by(group_by)
        custom_selects, custom_aliases = _parse_group_by(group_by_list)
        interval_select, interval_alias, order_col = _build_interval_columns(interval, self.tz)
        return interval_select, interval_alias, order_col, custom_selects, custom_aliases

    def _build_event_select_columns(
        self,
        interval_select: str,
        measure: Literal["totals", "uniques"],
        formula: str | None,
        custom_selects: Sequence[str],
    ) -> list[str]:
        metric = formula or self._metric_expression(measure)
        return [interval_select, "event_name", f"{metric} AS value", *custom_selects]

    def _build_event_where_parts(
        self,
        events: Sequence[str],
        filters: Sequence[EventFilter] | None,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
    ) -> list[str]:
        where_parts = [self._events_condition(events)]
        where_parts += self._compile_filters(filters)

        suffix_condition = _table_suffix_condition(self.table_id, start_ts, end_ts)
        if suffix_condition:
            where_parts.append(suffix_condition)

        where_parts.append(self._timestamp_condition(start_ts, end_ts))
        return where_parts

    def _build_funnel_ctes(
        self,
        *,
        steps: Sequence[FunnelStep],
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
        interval_select: str,
        custom_selects: Sequence[str],
    ) -> list[str]:
        ctes: list[str] = []
        cumulative_gt = timedelta(seconds=0)
        cumulative_lt = timedelta(seconds=0)

        for idx, step in enumerate(steps, start=1):
            if idx == 1:
                step_start = start_ts
                step_end = end_ts
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

            wheres = [f"event_name = {format_literal(step.event_name)}"]
            wheres += self._compile_filters(step.filters)

            suffix_condition = _table_suffix_condition(self.table_id, step_start, step_end)
            if suffix_condition:
                wheres.append(suffix_condition)

            wheres.append(self._timestamp_condition(step_start, step_end))

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

        return ctes

    def _build_funnel_join_clause(self, idx: int, step: FunnelStep) -> str:
        gt_us = self._timedelta_to_microseconds(step.conversion_window_gt)
        lt_us = self._timedelta_to_microseconds(step.conversion_window_lt)
        return "\n".join(
            [
                f"LEFT JOIN step{idx}",
                f"       ON step{idx}.{self.user_id_col} = step{idx-1}.{self.user_id_col}",
                f"      AND step{idx}.event_timestamp - step{idx-1}.event_timestamp > {gt_us}",
                f"      AND step{idx}.event_timestamp - step{idx-1}.event_timestamp < {lt_us}",
            ]
        )

    @staticmethod
    def _timedelta_to_microseconds(delta: timedelta) -> int:
        return int(delta.total_seconds() * 1_000_000)

    @staticmethod
    def _render_events_sql(
        *,
        select_cols: Sequence[str],
        from_clause: str,
        where_parts: Sequence[str],
        group_cols: Sequence[str],
        order_col: str,
    ) -> str:
        where_clause = " AND ".join(f"({clause})" for clause in where_parts)
        lines = [
            "",
            f"        SELECT {', '.join(select_cols)}",
            f"        FROM {from_clause}",
            f"        WHERE {where_clause}",
            f"        GROUP BY {', '.join(group_cols)}",
            f"        ORDER BY {order_col} ASC",
            "        ",
        ]
        return "\n".join(lines)

    @staticmethod
    def _render_funnel_sql(
        *,
        ctes: Sequence[str],
        interval_alias: str,
        custom_aliases: Sequence[str],
        step_cols: Sequence[str],
        joins: Sequence[str],
        order_col: str,
    ) -> str:
        select_group = (", " + ", ".join(custom_aliases)) if custom_aliases else ""
        group_suffix = ("," + ", ".join(custom_aliases)) if custom_aliases else ""
        parts = [
            "WITH",
            ",\n".join(ctes),
            "",
            "SELECT",
            f"  {interval_alias}{select_group},",
            f"  {', '.join(step_cols)}",
            "FROM step1",
            "\n".join(joins),
            f"GROUP BY {interval_alias}{group_suffix}",
            f"ORDER BY {order_col} ASC",
        ]
        return "\n".join(parts).strip()

    @staticmethod
    def _compile_filters(filters: Sequence[EventFilter] | None) -> list[str]:
        return _parse_filters(list(filters or []))

    @staticmethod
    def _prepare_result_dataframe(df: pd.DataFrame, interval_alias: str) -> pd.DataFrame:
        df[interval_alias] = pd.to_datetime(df[interval_alias])
        return df

    def _metric_expression(self, measure: Literal["totals", "uniques"]) -> str:
        if measure == "uniques":
            return f"COUNT(DISTINCT {self.user_id_col})"
        return "COUNT(*)"

    @staticmethod
    def _normalize_group_by(group_by: str | Sequence[str] | None) -> list[str]:
        if group_by is None:
            return []
        if isinstance(group_by, str):
            return [group_by]
        return list(group_by)

    @staticmethod
    def _events_condition(events: Sequence[str]) -> str:
        return "event_name IN {}".format(format_literal_list(events))

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
            pivot = df.pivot_table(values="value", index=interval_alias, columns=columns, fill_value=0)
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
            pivot = df.pivot_table(index=interval_alias, columns=custom_aliases, values=values_cols, fill_value=0)
            return pivot.sort_index(axis=1)

        return df.set_index(interval_alias)[values_cols].sort_index()
