"""Primary client for issuing GA4 queries against BigQuery."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Literal

import pandas as pd
from google.cloud import bigquery

from .dates import _build_interval_columns, _parse_date_range, _table_suffix_condition
from .filters import _parse_filters
from .group_by import _parse_group_by
from .sql import format_literal, format_literal_list
from .types import EventFilter, FunnelStep


@dataclass(frozen=True)
class _DimensionContext:
    """Container describing the interval and custom dimensions for a query."""

    interval_select: str
    interval_alias: str
    order_column: str
    custom_selects: tuple[str, ...]
    custom_aliases: tuple[str, ...]

    def event_select_columns(self, metric: str) -> list[str]:
        """Return the select columns used for event style queries."""

        return [self.interval_select, "event_name", f"{metric} AS value", *self.custom_selects]

    def event_group_columns(self) -> list[str]:
        """Return the group by clause for event style queries."""

        return [self.interval_alias, "event_name", *self.custom_aliases]

    def select_alias_suffix(self) -> str:
        """Return the suffix appended to the ``SELECT`` clause for funnels."""

        return (", " + ", ".join(self.custom_aliases)) if self.custom_aliases else ""

    def group_alias_suffix(self) -> str:
        """Return the suffix appended to the ``GROUP BY`` clause for funnels."""

        return (", " + ", ".join(self.custom_aliases)) if self.custom_aliases else ""


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
        dimensions = self._prepare_dimensions(group_by, interval)

        select_cols = self._build_event_select_columns(dimensions, measure, formula)

        sql = self._render_events_sql(
            select_cols=select_cols,
            from_clause=f"`{self.table_id}`",
            where_parts=self._build_event_where_parts(events, filters, start_ts, end_ts),
            group_cols=dimensions.event_group_columns(),
            order_col=dimensions.order_column,
        )

        df = self._prepare_result_dataframe(self._query(sql), dimensions.interval_alias)
        return self._pivot_events_dataframe(
            df, dimensions.interval_alias, dimensions.custom_aliases, events
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
        dimensions = self._prepare_dimensions(group_by, interval)

        ctes = self._build_funnel_ctes(
            steps=steps,
            start_ts=start_ts,
            end_ts=end_ts,
            dimensions=dimensions,
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
            dimensions=dimensions,
            step_cols=step_cols,
            joins=joins,
        )

        df = self._prepare_result_dataframe(self._query(sql), dimensions.interval_alias)
        return self._pivot_funnel_dataframe(
            df, dimensions.interval_alias, dimensions.custom_aliases, len(steps)
        )

    def _prepare_dimensions(
        self,
        group_by: str | Sequence[str] | None,
        interval: Literal["day", "hour", "week", "month"],
    ) -> _DimensionContext:
        group_by_list = self._normalize_group_by(group_by)
        custom_selects, custom_aliases = _parse_group_by(group_by_list)
        interval_select, interval_alias, order_col = _build_interval_columns(interval, self.tz)
        return _DimensionContext(
            interval_select=interval_select,
            interval_alias=interval_alias,
            order_column=order_col,
            custom_selects=tuple(custom_selects),
            custom_aliases=tuple(custom_aliases),
        )

    def _build_event_select_columns(
        self,
        dimensions: _DimensionContext,
        measure: Literal["totals", "uniques"],
        formula: str | None,
    ) -> list[str]:
        metric = formula or self._metric_expression(measure)
        return dimensions.event_select_columns(metric)

    def _build_event_where_parts(
        self,
        events: Sequence[str],
        filters: Sequence[EventFilter] | None,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
    ) -> list[str]:
        return [
            self._events_condition(events),
            *self._compile_filters(filters),
            *self._table_suffix_clauses(start_ts, end_ts),
            self._timestamp_condition(start_ts, end_ts),
        ]

    def _build_funnel_ctes(
        self,
        *,
        steps: Sequence[FunnelStep],
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
        dimensions: _DimensionContext,
    ) -> list[str]:
        return [
            self._render_funnel_cte(
                idx=idx,
                step=step,
                step_start=step_start,
                step_end=step_end,
                include_dimensions=(idx == 1),
                dimensions=dimensions,
            )
            for idx, step, step_start, step_end in self._iter_funnel_steps_within_range(
                steps, start_ts, end_ts
            )
        ]

    def _iter_funnel_steps_within_range(
        self,
        steps: Sequence[FunnelStep],
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
    ) -> Iterator[tuple[int, FunnelStep, pd.Timestamp, pd.Timestamp]]:
        """Yield each funnel step along with its timestamp bounds."""

        cumulative_gt = timedelta(seconds=0)
        cumulative_lt = timedelta(seconds=0)

        for idx, step in enumerate(steps, start=1):
            if idx == 1:
                yield idx, step, start_ts, end_ts
                continue

            cumulative_gt += step.conversion_window_gt
            cumulative_lt += step.conversion_window_lt
            yield idx, step, start_ts + cumulative_gt, end_ts + cumulative_lt

    def _render_funnel_cte(
        self,
        *,
        idx: int,
        step: FunnelStep,
        step_start: pd.Timestamp,
        step_end: pd.Timestamp,
        include_dimensions: bool,
        dimensions: _DimensionContext,
    ) -> str:
        """Return the SQL ``WITH`` clause for a single funnel step."""

        select_fields = self._funnel_select_fields(include_dimensions, dimensions)
        wheres = [
            f"event_name = {format_literal(step.event_name)}",
            *self._compile_filters(step.filters),
            *self._table_suffix_clauses(step_start, step_end),
            self._timestamp_condition(step_start, step_end),
        ]

        return "\n".join(
            [
                f"step{idx} AS (",
                f"  SELECT {', '.join(select_fields)}",
                f"  FROM `{self.table_id}`",
                f"  WHERE {' AND '.join(wheres)}",
                ")",
            ]
        )

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
        dimensions: _DimensionContext,
        step_cols: Sequence[str],
        joins: Sequence[str],
    ) -> str:
        select_suffix = dimensions.select_alias_suffix()
        group_suffix = dimensions.group_alias_suffix()
        parts = [
            "WITH",
            ",\n".join(ctes),
            "",
            "SELECT",
            f"  {dimensions.interval_alias}{select_suffix},",
            f"  {', '.join(step_cols)}",
            "FROM step1",
            "\n".join(joins),
            f"GROUP BY {dimensions.interval_alias}{group_suffix}",
            f"ORDER BY {dimensions.order_column} ASC",
        ]
        return "\n".join(parts).strip()

    @staticmethod
    def _compile_filters(filters: Sequence[EventFilter] | None) -> list[str]:
        return _parse_filters(filters)

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

    def _funnel_select_fields(
        self, include_dimensions: bool, dimensions: _DimensionContext
    ) -> list[str]:
        fields = [self.user_id_col, "event_timestamp"]
        if include_dimensions:
            fields.append(dimensions.interval_select)
            if dimensions.custom_selects:
                fields.extend(dimensions.custom_selects)
        return fields

    def _table_suffix_clauses(
        self, start_ts: pd.Timestamp, end_ts: pd.Timestamp
    ) -> tuple[str, ...]:
        clause = _table_suffix_condition(self.table_id, start_ts, end_ts)
        return (clause,) if clause else tuple()
