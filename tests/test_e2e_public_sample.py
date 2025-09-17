from __future__ import annotations

from datetime import date

import pandas as pd
import pytest
from google.cloud import bigquery
from google.api_core import exceptions as gcore_exc
from google.auth import exceptions as auth_exc

from ga4bigquery import GA4BigQuery, FunnelStep


def _client() -> GA4BigQuery:
    try:
        client = bigquery.Client()
    except (
        auth_exc.DefaultCredentialsError,
        auth_exc.RefreshError,
        gcore_exc.Unauthenticated,
        gcore_exc.PermissionDenied,
        gcore_exc.Forbidden,
    ) as e:
        pytest.skip(f"Skipping E2E due to Google auth error: {e}")

    return GA4BigQuery(
        table_id="bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*",
        tz="America/Los_Angeles",
        client=client,
    )


def test_public_sample_page_views_matches_snapshot() -> None:
    ga = _client()

    df = ga.request_events(
        events=["page_view"],
        start=date(2020, 11, 1),
        end=date(2020, 11, 2),
        measure="totals",
        interval="day",
    )

    expected = pd.DataFrame(
        {"value": [11308, 17698]},
        index=pd.to_datetime(["2020-11-01", "2020-11-02"]),
    )
    expected.index.name = "event_date"

    pd.testing.assert_frame_equal(df, expected, check_dtype=False)


def test_public_sample_purchase_funnel_matches_snapshot() -> None:
    ga = _client()

    df = ga.request_funnel(
        steps=[
            FunnelStep(event_name="view_item"),
            FunnelStep(event_name="add_to_cart"),
            FunnelStep(event_name="purchase"),
        ],
        start=date(2020, 11, 1),
        end=date(2020, 11, 2),
        group_by="platform",
        interval="day",
    )

    expected = pd.DataFrame(
        [[607.0, 4.0, 3.0], [897.0, 19.0, 9.0]],
        index=pd.to_datetime(["2020-11-01", "2020-11-02"]),
        columns=pd.MultiIndex.from_tuples(
            [("view_item", "WEB"), ("add_to_cart", "WEB"), ("purchase", "WEB")],
            names=[None, "platform"],
        ),
    )
    expected.index.name = "event_date"

    pd.testing.assert_frame_equal(df, expected, check_dtype=False)
