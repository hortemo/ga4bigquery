"""Primary client for issuing GA4 queries against BigQuery."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
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

        group_by_list = self._normalize_group_by(group_by)
        custom_selects, custom_aliases = _parse_group_by(group_by_list)

        interval_select, interval_alias, order_col = _build_interval_columns(interval, self.tz)

        select_cols: list[str] = [interval_select, "event_name"]
        default_metric = f"COUNT(DISTINCT {self.user_id_col})" if measure == "uniques" else "COUNT(*)"
        select_cols.append(f"{formula or default_metric} AS value")
        select_cols.extend(custom_selects)

        from_clause = f"`{self.table_id}`"

        where_parts: list[str] = [self._events_condition(events)]
        where_parts += _parse_filters(list(filters or []))

        suffix_condition = _table_suffix_condition(self.table_id, start_ts, end_ts)
        if suffix_condition:
            where_parts.append(suffix_condition)

        where_parts.append(self._timestamp_condition(start_ts, end_ts))

        group_cols: list[str] = [interval_alias, "event_name"] + custom_aliases

        sql = f"""
        SELECT {', '.join(select_cols)}
        FROM {from_clause}
        WHERE {' AND '.join(f'({clause})' for clause in where_parts)}
        GROUP BY {', '.join(group_cols)}
        ORDER BY {order_col} ASC
        """

        df = self._query(sql)
        df.replace(["IOS", "ANDROID"], ["iOS", "Android"], inplace=True)
        df[interval_alias] = pd.to_datetime(df[interval_alias])

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

        group_by_list = self._normalize_group_by(group_by)
        custom_selects, custom_aliases = _parse_group_by(group_by_list)

        interval_select, interval_alias, order_col = _build_interval_columns(interval, self.tz)

        ctes: list[str] = []
        ts_min = start_ts
        ts_max = end_ts

        for idx, step in enumerate(steps, start=1):
            select_fields = [self.user_id_col, "event_timestamp"]
            if idx == 1:
                select_fields.append(interval_select)
                if custom_selects:
                    select_fields.extend(custom_selects)

            if idx > 1:
                ts_min += step.conversion_window_gt
                ts_max += step.conversion_window_lt

            wheres = [f"event_name = {format_literal(step.event_name)}"]
            wheres += _parse_filters(list(step.filters or []))

            suffix_condition = _table_suffix_condition(self.table_id, ts_min, ts_max)
            if suffix_condition:
                wheres.append(suffix_condition)

            wheres.append(self._timestamp_condition(ts_min, ts_max))

            ctes.append(
                f"""
step{idx} AS (
  SELECT {', '.join(select_fields)}
  FROM `{self.table_id}`
  WHERE {' AND '.join(wheres)}
)
                """.strip()
            )

        joins: list[str] = []
        for i in range(2, len(steps) + 1):
            gt_us = int(steps[i - 1].conversion_window_gt.total_seconds() * 1_000_000)
            lt_us = int(steps[i - 1].conversion_window_lt.total_seconds() * 1_000_000)
            joins.append(
                f"""
LEFT JOIN step{i}
       ON step{i}.{self.user_id_col} = step{i-1}.{self.user_id_col}
      AND step{i}.event_timestamp - step{i-1}.event_timestamp > {gt_us}
      AND step{i}.event_timestamp - step{i-1}.event_timestamp < {lt_us}
                """.strip()
            )

        step_cols = [f"COUNT(DISTINCT step{idx}.{self.user_id_col}) AS `{idx}`" for idx in range(1, len(steps) + 1)]
        select_group = (", " + ", ".join(custom_aliases)) if custom_aliases else ""

        sql = f"""
WITH
{',\n'.join(ctes)}

SELECT
  {interval_alias}{select_group},
  {', '.join(step_cols)}
FROM step1
{'\n'.join(joins)}
GROUP BY {interval_alias}{(',' + ', '.join(custom_aliases)) if custom_aliases else ''}
ORDER BY {order_col} ASC
        """.strip()

        df = self._query(sql)
        df.replace(["IOS", "ANDROID"], ["iOS", "Android"], inplace=True)
        df[interval_alias] = pd.to_datetime(df[interval_alias])

        return self._pivot_funnel_dataframe(df, interval_alias, custom_aliases, len(steps))

    @staticmethod
    def _normalize_group_by(group_by: str | Sequence[str] | None) -> list[str]:
        if group_by is None:
            return []
        if isinstance(group_by, str):
            values = [group_by]
        else:
            values = list(group_by)
        return ["geo.country" if column == "country" else column for column in values]

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
