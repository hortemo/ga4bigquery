"""Primary client for issuing GA4 queries against BigQuery."""

from __future__ import annotations

from typing import Unpack

import pandas as pd
from google.cloud import bigquery

from .request_events import (
    RequestEventsSpecificArguments,
    request_events as _request_events,
)
from .request_funnel import (
    RequestFunnelSpecificArguments,
    request_funnel as _request_funnel,
)


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

    def shared_arguments(self) -> dict:
        return dict(
            client=self.client,
            table_id=self.table_id,
            tz=self.tz,
            user_id_col=self.user_id_col,
        )

    def request_events(
        self,
        **arguments: Unpack[RequestEventsSpecificArguments],
    ) -> pd.DataFrame:
        """Return a time series of event metrics."""

        return _request_events(
            **self.shared_arguments(),
            **arguments,
        )

    def request_funnel(
        self,
        **arguments: Unpack[RequestFunnelSpecificArguments],
    ) -> pd.DataFrame:
        """Return conversion counts for a funnel across time."""

        return _request_funnel(
            **self.shared_arguments(),
            **arguments,
        )
