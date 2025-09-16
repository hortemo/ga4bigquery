from __future__ import annotations

import re
from datetime import date
from typing import List, Optional, Union, Literal

import pandas as pd
from google.cloud import bigquery

from .dates import _parse_date_range, _build_interval_columns
from .filters import _parse_filters
from .group_by import _parse_group_by
from .types import EventFilter, FunnelStep


class GA4BigQuery:
    """
    Minimal GA4-on-BigQuery client focused on two operations:
      - request_events
      - request_funnel

    Project-agnostic: pass the table_id (may end with *), timezone for bucketing,
    and the user identifier column (default 'user_pseudo_id').
    """

    def __init__(self, table_id: str, *, tz: str = "UTC", user_id_col: str = "user_pseudo_id", client: Optional[bigquery.Client] = None):
        self.table_id = table_id
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

        from_clause = f"`{self.table_id}`"

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
  FROM `{self.table_id}`
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
