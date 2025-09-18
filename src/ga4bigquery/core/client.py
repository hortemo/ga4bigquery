"""Primary client for issuing GA4 queries against BigQuery."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from typing import Literal

import pandas as pd
from google.cloud import bigquery

from .request_events import request_events as _request_events_impl
from .request_funnel import request_funnel as _request_funnel_impl
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

        return _request_events_impl(
            client=self.client,
            table_id=self.table_id,
            tz=self.tz,
            user_id_col=self.user_id_col,
            events=events,
            start=start,
            end=end,
            measure=measure,
            formula=formula,
            filters=filters,
            group_by=group_by,
            interval=interval,
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

        return _request_funnel_impl(
            client=self.client,
            table_id=self.table_id,
            tz=self.tz,
            user_id_col=self.user_id_col,
            steps=steps,
            start=start,
            end=end,
            group_by=group_by,
            interval=interval,
        )
