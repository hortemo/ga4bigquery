"""Primary client for issuing GA4 queries against BigQuery."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from typing import Literal

import pandas as pd
from google.cloud import bigquery

from .request_events import request_events as _request_events
from .request_funnel import request_funnel as _request_funnel
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
        """Instantiate the client with shared BigQuery context.

        Args:
            table_id: Fully-qualified BigQuery table containing GA4 export data.
            tz: IANA timezone identifier applied when parsing date ranges.
            user_id_col: Column used for unique user calculations.
            client: Optional pre-configured BigQuery client instance.
        """
        self.table_id = table_id
        self.tz = tz
        self.user_id_col = user_id_col
        self.client = client or bigquery.Client()

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
        """Return a time series of event metrics for the specified events.

        Args:
            events: Single event name or sequence of event names to aggregate.
            start: Inclusive start date for the query window.
            end: Inclusive end date for the query window.
            measure: Metric to calculate, either ``"totals"`` or ``"uniques"``.
            formula: Custom SQL expression overriding ``measure`` when provided.
            filters: Additional predicates applied to matching events.
            group_by: Dimensions to group by in addition to the interval.
            interval: Time bucketing granularity for returned rows.

        Returns:
            DataFrame with event metrics indexed by the requested interval.
        """

        return _request_events(
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
        """Return conversion counts for a funnel across time.

        Args:
            steps: Ordered collection of funnel steps that define the sequence.
            start: Inclusive start date for the funnel window.
            end: Inclusive end date for the funnel window.
            group_by: Dimensions used to split funnel results (optional).
            interval: Time bucketing granularity for the aggregated counts.

        Returns:
            DataFrame containing funnel step counts indexed by interval.
        """

        return _request_funnel(
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
