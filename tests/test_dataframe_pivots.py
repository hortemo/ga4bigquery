from __future__ import annotations

import pandas as pd
from pandas.testing import assert_frame_equal

from ga4bigquery.core.request_events import pivot_events_dataframe
from ga4bigquery.core.request_funnel import pivot_funnel_dataframe


# Event pivot tests ----------------------------------------------------------

def test_pivot_events_dataframe_without_grouping_single_event() -> None:
    df = pd.DataFrame(
        {
            "interval": [pd.Timestamp("2023-01-02"), pd.Timestamp("2023-01-01")],
            "event_name": ["sign_up", "sign_up"],
            "value": [5, 3],
        }
    )

    result = pivot_events_dataframe(
        df=df, interval_alias="interval", group_by_aliases=[], events=["sign_up"]
    )

    expected = pd.DataFrame(
        {"value": [3, 5]},
        index=pd.DatetimeIndex(
            [pd.Timestamp("2023-01-01"), pd.Timestamp("2023-01-02")],
            name="interval",
        ),
    )

    assert_frame_equal(result, expected)


def test_pivot_events_dataframe_multiple_events() -> None:
    df = pd.DataFrame(
        {
            "interval": [
                pd.Timestamp("2023-01-01"),
                pd.Timestamp("2023-01-01"),
                pd.Timestamp("2023-01-02"),
            ],
            "event_name": ["purchase", "sign_up", "purchase"],
            "value": [2, 4, 1],
        }
    )

    result = pivot_events_dataframe(
        df=df,
        interval_alias="interval",
        group_by_aliases=[],
        events=["sign_up", "purchase"],
    )

    expected = pd.DataFrame(
        {
            "purchase": [2.0, 1.0],
            "sign_up": [4.0, 0.0],
        },
        index=pd.DatetimeIndex(
            [pd.Timestamp("2023-01-01"), pd.Timestamp("2023-01-02")],
            name="interval",
        ),
        columns=pd.Index(["purchase", "sign_up"], name="event_name"),
    )

    assert_frame_equal(result, expected)


def test_pivot_events_dataframe_with_custom_dimensions() -> None:
    df = pd.DataFrame(
        {
            "interval": [
                pd.Timestamp("2023-01-01"),
                pd.Timestamp("2023-01-01"),
                pd.Timestamp("2023-01-01"),
                pd.Timestamp("2023-01-02"),
                pd.Timestamp("2023-01-02"),
            ],
            "platform": ["iOS", "Android", "iOS", "iOS", "Android"],
            "event_name": ["purchase", "purchase", "sign_up", "purchase", "sign_up"],
            "value": [2, 3, 5, 4, 1],
        }
    )

    result = pivot_events_dataframe(
        df=df,
        interval_alias="interval",
        group_by_aliases=["platform"],
        events=["sign_up", "purchase"],
    )

    expected = pd.DataFrame(
        data=[[3.0, 0.0, 2.0, 5.0], [0.0, 1.0, 4.0, 0.0]],
        index=pd.DatetimeIndex(
            [pd.Timestamp("2023-01-01"), pd.Timestamp("2023-01-02")],
            name="interval",
        ),
        columns=pd.MultiIndex.from_product(
            [["Android", "iOS"], ["purchase", "sign_up"]],
            names=["platform", "event_name"],
        ),
    )

    assert_frame_equal(result, expected)


# Funnel pivot tests ---------------------------------------------------------

def test_pivot_funnel_dataframe_without_grouping() -> None:
    df = pd.DataFrame(
        {
            "event_date": pd.to_datetime(["2024-01-02", "2024-01-01"]),
            "1": [30, 10],
            "2": [20, 5],
            "3": [7, 2],
        }
    )

    result = pivot_funnel_dataframe(
        df=df,
        interval_alias="event_date",
        group_by_aliases=(),
        step_count=3,
    )

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


def test_pivot_funnel_dataframe_with_group_by_aliases_creates_sorted_multiindex() -> None:
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

    result = pivot_funnel_dataframe(
        df=df,
        interval_alias="event_date",
        group_by_aliases=("platform", "country"),
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
