"""Unit tests for the funnel pivot helper."""

from __future__ import annotations

import pandas as pd
from pandas.testing import assert_frame_equal

from ga4bigquery import GA4BigQuery


def test_pivot_funnel_dataframe_without_grouping():
    df = pd.DataFrame(
        {
            "event_date": pd.to_datetime(["2024-01-02", "2024-01-01"]),
            "1": [30, 10],
            "2": [20, 5],
            "3": [7, 2],
        }
    )

    result = GA4BigQuery._pivot_funnel_dataframe(df, "event_date", (), 3)

    expected = pd.DataFrame(
        {
            "1": [10, 30],
            "2": [5, 20],
            "3": [2, 7],
        },
        index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
    )
    expected.index.name = "event_date"

    assert_frame_equal(result, expected)


def test_pivot_funnel_dataframe_with_custom_aliases_creates_sorted_multiindex():
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
            "1": [4, 2, 1, 3],
            "2": [8, 6, 5, 7],
        }
    )

    result = GA4BigQuery._pivot_funnel_dataframe(
        df,
        interval_alias="event_date",
        custom_aliases=("platform", "country"),
        step_count=2,
    )

    columns = pd.MultiIndex.from_tuples(
        [
            ("1", "Android", "NO"),
            ("1", "Android", "SE"),
            ("1", "iOS", "NO"),
            ("2", "Android", "NO"),
            ("2", "Android", "SE"),
            ("2", "iOS", "NO"),
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
