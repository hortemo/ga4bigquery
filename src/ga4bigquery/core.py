
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import List, TypedDict, Tuple, Union, Literal, Optional

import pandas as pd
import re
from google.cloud import bigquery


EventOperator = Literal["IN", "NOT IN", "=", "!=", ">", "<", ">=", "<="]


class EventFilter(TypedDict):
    prop: str
    op: EventOperator
    values: List[str]


@dataclass
class FunnelStep:
    event_name: str
    conversion_window_gt: timedelta = timedelta(seconds=0)
    conversion_window_lt: timedelta = timedelta(days=30)
    filters: List[EventFilter] = field(default_factory=list)


def _parse_date_range(start: date, end: date, tz: str) -> Tuple[pd.Timestamp, pd.Timestamp]:
    start_ts = (
        pd.Timestamp(start)
        .tz_localize(tz)
        .replace(hour=0, minute=0, second=0, microsecond=0)
    )
    end_ts = (
        pd.Timestamp(end)
        .tz_localize(tz)
        .replace(hour=23, minute=59, second=59, microsecond=999_999)
    )
    return start_ts, end_ts


def _build_interval_columns(interval: Literal["day", "hour", "week", "month"], tz: str):
    interval = interval.lower()

    if interval in {"day", "date"}:
        expr = (
            "FORMAT_DATE('%Y-%m-%d', DATE(TIMESTAMP_MICROS(event_timestamp), '{tz}')) AS event_date"
        ).format(tz=tz)
        return expr, "event_date", "event_date"

    if interval == "hour":
        expr = (
            "FORMAT_TIMESTAMP('%Y-%m-%d %H:00:00', "
            "TIMESTAMP_TRUNC(TIMESTAMP_MICROS(event_timestamp), HOUR, '{tz}'), '{tz}') AS event_hour"
        ).format(tz=tz)
        return expr, "event_hour", "event_hour"

    if interval == "week":
        expr = (
            "FORMAT_DATE('%Y-%m-%d', "
            "DATE_TRUNC(DATE(TIMESTAMP_MICROS(event_timestamp), '{tz}'), WEEK(MONDAY))) AS event_week"
        ).format(tz=tz)
        return expr, "event_week", "event_week"

    if interval == "month":
        expr = (
            "FORMAT_DATE('%Y-%m', "
            "DATE_TRUNC(DATE(TIMESTAMP_MICROS(event_timestamp), '{tz}'), MONTH)) AS event_month"
        ).format(tz=tz)
        return expr, "event_month", "event_month"

    raise ValueError("interval must be one of: 'day', 'hour', 'week', 'month'")


def _parse_filters(filters: List[EventFilter]) -> List[str]:
    return [_parse_filter(f) for f in (filters or [])]


def _parse_filter(filter: EventFilter) -> str:
    prop_with_prefix = filter["prop"]
    parts = prop_with_prefix.split(".")
    prefix = parts[0] if len(parts) > 1 else None
    prop_without_prefix = parts[-1]
    op = filter["op"]

    values = filter["values"]
    values_sql = "({})".format(", ".join("'{}'".format(str(x).replace("'", "\\'")) for x in values))

    if prefix in {"event_params", "user_properties"}:
        values_are_numeric = all(re.fullmatch(r"-?\d+(\.\d+)?", str(v)) is not None for v in values)
        value_expr = "CAST(value.string_value AS INT64)" if values_are_numeric else "value.string_value"
        return (
            "EXISTS (SELECT * FROM UNNEST({prefix}) WHERE key = '{key}' "
            "AND {value_expr} {op} {values})"
        ).format(prefix=prefix, key=prop_without_prefix, value_expr=value_expr, op=op, values=values_sql)

    return f"{prop_with_prefix} {op} {values_sql}"


def _parse_group_by(group_by: List[str]):
    statements: List[str] = []
    aliases: List[str] = []

    for prop_with_prefix in (group_by or []):
        parts = prop_with_prefix.split(".")
        prefix = parts[0] if len(parts) > 1 else None
        prop_without_prefix = parts[-1]

        if prefix in {"event_params", "user_properties"}:
            statements.append(
                "(SELECT props.value.string_value FROM UNNEST({prefix}) props WHERE props.key = '{key}') "
                "AS {alias}".format(prefix=prefix, key=prop_without_prefix, alias=prop_without_prefix)
            )
        else:
            statements.append(f"{prop_with_prefix} AS {prop_without_prefix}")
        aliases.append(prop_without_prefix)

    return statements, aliases


class GA4BigQuery:
    """
    Minimal GA4-on-BigQuery client focused on two operations:
      - request_events
      - request_funnel

    Project-agnostic: pass the project, dataset, and table identifiers (table may end
    with *), timezone for bucketing, and the user identifier column (default
    'user_pseudo_id').
    """

    def __init__(
        self,
        project_id: str,
        dataset_id: str,
        table_id: str,
        *,
        tz: str = "UTC",
        user_id_col: str = "user_pseudo_id",
        client: Optional[bigquery.Client] = None,
    ):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self._table_fqn = f"{project_id}.{dataset_id}.{table_id}"
        self.tz = tz
        self.user_id_col = user_id_col
        self.client = client or bigquery.Client()

    def _query(self, sql: str) -> pd.DataFrame:
        return self.client.query(sql).result().to_dataframe()

    def request_events(
        self,
        *,
        events: List[str],
        start: date,
        end: date,
        measure: Union[Literal["totals"], Literal["uniques"]] = "totals",
        formula: Optional[str] = None,
        filters: Optional[List[EventFilter]] = None,
        group_by: Union[str, List[str], None] = None,
        interval: Literal["day", "hour", "week", "month"] = "day",
    ) -> pd.DataFrame:
        start_ts, end_ts = _parse_date_range(start, end, self.tz)

        group_by_list = [group_by] if isinstance(group_by, str) else (group_by or [])
        group_by_list = [re.sub(r"^country$", "geo.country", g) for g in group_by_list]
        custom_selects, custom_aliases = _parse_group_by(group_by_list)

        interval_select, interval_alias, order_col = _build_interval_columns(interval, self.tz)

        select_cols: List[str] = [interval_select, "event_name"]
        default_metric = f"COUNT(DISTINCT {self.user_id_col})" if measure == "uniques" else "COUNT(*)"
        select_cols.append(f"{formula or default_metric} AS value")
        select_cols += custom_selects

        from_clause = f"`{self._table_fqn}`"

        where_parts: List[str] = [
            "event_name IN ({})".format(", ".join("'{}'".format(e.replace("'", "\\'")) for e in events))
        ]
        where_parts += _parse_filters(filters or [])

        if self.table_id.endswith("*"):
            lo = start_ts.tz_convert("UTC").date().strftime("%Y%m%d")
            hi = end_ts.tz_convert("UTC").date().strftime("%Y%m%d")
            where_parts.append(
                "REGEXP_EXTRACT(_TABLE_SUFFIX, r'(\\d+)$') BETWEEN '{lo}' AND '{hi}'".format(lo=lo, hi=hi)
            )

        where_parts.append(
            "TIMESTAMP_MICROS(event_timestamp) BETWEEN TIMESTAMP('{start}') AND TIMESTAMP('{end}')".format(
                start=start_ts.isoformat(), end=end_ts.isoformat()
            )
        )

        group_cols: List[str] = [interval_alias, "event_name"] + custom_aliases

        sql = f"""
        SELECT {', '.join(select_cols)}
        FROM {from_clause}
        WHERE {' AND '.join(f'({w})' for w in where_parts)}
        GROUP BY {', '.join(group_cols)}
        ORDER BY {order_col} ASC
        """

        df = self._query(sql)
        df.replace(["IOS", "ANDROID"], ["iOS", "Android"], inplace=True)
        df[interval_alias] = pd.to_datetime(df[interval_alias])

        if custom_aliases:
            cols = custom_aliases + (["event_name"] if len(events) > 1 else [])
            pivot = df.pivot_table(values="value", index=interval_alias, columns=cols, fill_value=0)
            return pivot.sort_index(axis=1)
        else:
            if len(events) > 1:
                pivot = df.pivot_table(values="value", index=interval_alias, columns=["event_name"], fill_value=0)
                return pivot.sort_index(axis=1)
            else:
                out = df[[interval_alias, "value"]].set_index(interval_alias).sort_index()
                out.index.name = interval_alias
                return out

    def request_funnel(
        self,
        *,
        steps: List[FunnelStep],
        start: date,
        end: date,
        group_by: Union[str, List[str], None] = None,
        interval: Literal["day", "hour", "week", "month"] = "day",
    ) -> pd.DataFrame:
        start_ts, end_ts = _parse_date_range(start, end, self.tz)

        group_by_list = [group_by] if isinstance(group_by, str) else (group_by or [])
        group_by_list = [re.sub(r"^country$", "geo.country", g) for g in group_by_list]
        custom_selects, custom_aliases = _parse_group_by(group_by_list)

        interval_select, interval_alias, order_col = _build_interval_columns(interval, self.tz)

        ctes: List[str] = []
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

            wheres = [f"event_name = '{step.event_name.replace("'", "\\'")}'"]
            wheres += _parse_filters(step.filters or [])

            if self.table_id.endswith("*"):
                lo = ts_min.tz_convert("UTC").date().strftime("%Y%m%d")
                hi = ts_max.tz_convert("UTC").date().strftime("%Y%m%d")
                wheres.append("REGEXP_EXTRACT(_TABLE_SUFFIX, r'(\\d+)$') BETWEEN '{lo}' AND '{hi}'".format(lo=lo, hi=hi))

            wheres.append("TIMESTAMP_MICROS(event_timestamp) BETWEEN TIMESTAMP('{start}') AND TIMESTAMP('{end}')".format(
                start=ts_min.isoformat(), end=ts_max.isoformat()
            ))

            ctes.append(
                f"""
step{idx} AS (
  SELECT {', '.join(select_fields)}
  FROM `{self._table_fqn}`
  WHERE {' AND '.join(wheres)}
)
                """.strip()
            )

        joins: List[str] = []
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

        df = self._query(sql).replace(["IOS", "ANDROID"], ["iOS", "Android"])
        df[interval_alias] = pd.to_datetime(df[interval_alias])

        if custom_aliases:
            values_cols = [str(i) for i in range(1, len(steps) + 1)]
            pivot = df.pivot_table(index=interval_alias, columns=custom_aliases, values=values_cols, fill_value=0)
            return pivot.sort_index(axis=1)
        else:
            return df.set_index(interval_alias)[[str(i) for i in range(1, len(steps) + 1)]].sort_index()
