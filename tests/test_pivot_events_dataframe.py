from __future__ import annotations

import pandas as pd

from ga4bigquery.core.request_events import pivot_events_dataframe


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

    pd.testing.assert_frame_equal(result, expected)


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

    pd.testing.assert_frame_equal(result, expected)


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

    pd.testing.assert_frame_equal(result, expected)
