"""Unit tests for the funnel pivot helper."""

from __future__ import annotations

import pandas as pd
from pandas.testing import assert_frame_equal

from ga4bigquery import GA4BigQuery, FunnelStep


def test_pivot_funnel_dataframe_without_grouping():
    step_columns = ("view_item", "add_to_cart", "purchase")
    df = pd.DataFrame(
        {
            "event_date": pd.to_datetime(["2024-01-02", "2024-01-01"]),
            "view_item": [30, 10],
            "add_to_cart": [20, 5],
            "purchase": [7, 2],
        }
    )

    result = GA4BigQuery._pivot_funnel_dataframe(df, "event_date", (), step_columns)

    expected = pd.DataFrame(
        {
            "view_item": [10, 30],
            "add_to_cart": [5, 20],
            "purchase": [2, 7],
        },
        index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
    )
    expected.index.name = "event_date"

    assert_frame_equal(result, expected)


def test_pivot_funnel_dataframe_with_custom_aliases_creates_sorted_multiindex():
    step_columns = ("view_item", "add_to_cart")
    df = pd.DataFrame(
        {
            "event_date": pd.to_datetime(
                [
                    "2024-01-02",
                    "2024-01-01",
                    "2024-01-01",
                    "2024-01-02",
                ]
            ),
            "platform": ["Android", "Android", "iOS", "Android"],
            "country": ["NO", "NO", "NO", "SE"],
            "view_item": [4, 2, 1, 3],
            "add_to_cart": [8, 6, 5, 7],
        }
    )

    result = GA4BigQuery._pivot_funnel_dataframe(
        df,
        interval_alias="event_date",
        custom_aliases=("platform", "country"),
        step_columns=step_columns,
    )

    columns = pd.MultiIndex.from_tuples(
        [
            ("view_item", "Android", "NO"),
            ("view_item", "Android", "SE"),
            ("view_item", "iOS", "NO"),
            ("add_to_cart", "Android", "NO"),
            ("add_to_cart", "Android", "SE"),
            ("add_to_cart", "iOS", "NO"),
        ],
        names=[None, "platform", "country"],
    )
    expected = pd.DataFrame(
        [
            [2.0, 0.0, 1.0, 6.0, 0.0, 5.0],
            [4.0, 3.0, 0.0, 8.0, 7.0, 0.0],
        ],
        index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
        columns=columns,
    )
    expected.index.name = "event_date"

    assert_frame_equal(result, expected)


def test_funnel_step_column_names_handle_duplicates() -> None:
    steps = (
        FunnelStep(event_name="purchase"),
        FunnelStep(event_name="purchase"),
        FunnelStep(event_name="refund"),
        FunnelStep(event_name="purchase"),
    )

    names = GA4BigQuery._funnel_step_column_names(steps)

    assert names == ("purchase", "purchase (2)", "refund", "purchase (3)")
