"""Snapshot-style tests that assert complex SQL generation remains stable."""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from ga4bigquery import GA4BigQuery, FunnelStep


class _CaptureQuery(Exception):
    """Internal helper used to intercept the generated SQL without running it."""

    def __init__(self, sql: str) -> None:
        super().__init__(sql)
        self.sql = sql


class _CaptureGA4(GA4BigQuery):
    """GA4 client that raises instead of querying so SQL can be inspected."""

    def _query(self, sql: str):  # type: ignore[override]
        raise _CaptureQuery(sql)


@pytest.fixture
def capture_ga4() -> type[_CaptureGA4]:
    """Fixture returning a GA4 client class that records the generated SQL."""

    return _CaptureGA4


def test_request_events_sql_simple_snapshot(capture_ga4: type[_CaptureGA4]):
    ga = capture_ga4(table_id="proj.dataset.events_2024", tz="UTC", user_id_col="user_id", client=object())

    with pytest.raises(_CaptureQuery) as captured:
        ga.request_events(events=["purchase"], start=date(2024, 3, 1), end=date(2024, 3, 2))

    expected_sql = """
        SELECT FORMAT_DATE('%Y-%m-%d', DATE(TIMESTAMP_MICROS(event_timestamp), 'UTC')) AS event_date, event_name, COUNT(*) AS value
        FROM `proj.dataset.events_2024`
        WHERE (event_name IN ('purchase')) AND (TIMESTAMP_MICROS(event_timestamp) BETWEEN TIMESTAMP('2024-03-01T00:00:00+00:00') AND TIMESTAMP('2024-03-02T23:59:59.999999+00:00'))
        GROUP BY event_date, event_name
        ORDER BY event_date ASC
        """

    assert captured.value.sql == expected_sql


def test_request_events_sql_uniques_snapshot(capture_ga4: type[_CaptureGA4]):
    ga = capture_ga4(table_id="proj.dataset.events", tz="UTC", user_id_col="user_id", client=object())

    with pytest.raises(_CaptureQuery) as captured:
        ga.request_events(
            events=["page_view"],
            start=date(2024, 4, 1),
            end=date(2024, 4, 7),
            measure="uniques",
            interval="day",
        )

    expected_sql = """
        SELECT FORMAT_DATE('%Y-%m-%d', DATE(TIMESTAMP_MICROS(event_timestamp), 'UTC')) AS event_date, event_name, COUNT(DISTINCT user_id) AS value
        FROM `proj.dataset.events`
        WHERE (event_name IN ('page_view')) AND (TIMESTAMP_MICROS(event_timestamp) BETWEEN TIMESTAMP('2024-04-01T00:00:00+00:00') AND TIMESTAMP('2024-04-07T23:59:59.999999+00:00'))
        GROUP BY event_date, event_name
        ORDER BY event_date ASC
        """

    assert captured.value.sql == expected_sql


def test_request_events_sql_monthly_group_snapshot(capture_ga4: type[_CaptureGA4]):
    ga = capture_ga4(
        table_id="proj.dataset.events_*",
        tz="America/Los_Angeles",
        user_id_col="user_pseudo_id",
        client=object(),
    )

    with pytest.raises(_CaptureQuery) as captured:
        ga.request_events(
            events=["login", "purchase"],
            start=date(2023, 12, 15),
            end=date(2024, 1, 10),
            filters=[{"prop": "platform", "op": "IN", "values": ["ANDROID", "IOS"]}],
            group_by="geo.country",
            interval="month",
            measure="totals",
        )

    expected_sql = """
        SELECT FORMAT_DATE('%Y-%m', DATE_TRUNC(DATE(TIMESTAMP_MICROS(event_timestamp), 'America/Los_Angeles'), MONTH)) AS event_month, event_name, COUNT(*) AS value, geo.country AS country
        FROM `proj.dataset.events_*`
        WHERE (event_name IN ('login', 'purchase')) AND (platform IN ('ANDROID', 'IOS')) AND (REGEXP_EXTRACT(_TABLE_SUFFIX, r'(\\d+)$') BETWEEN '20231215' AND '20240111') AND (TIMESTAMP_MICROS(event_timestamp) BETWEEN TIMESTAMP('2023-12-15T00:00:00-08:00') AND TIMESTAMP('2024-01-10T23:59:59.999999-08:00'))
        GROUP BY event_month, event_name, country
        ORDER BY event_month ASC
        """

    assert captured.value.sql == expected_sql


def test_request_events_sql_numeric_filters_snapshot(capture_ga4: type[_CaptureGA4]):
    ga = capture_ga4(table_id="proj.dataset.events_*", tz="UTC", user_id_col="user_id", client=object())

    filters = [
        {"prop": "event_params.quantity", "op": ">=", "values": [10]},
        {"prop": "user_properties.score", "op": "<", "values": [5]},
    ]

    with pytest.raises(_CaptureQuery) as captured:
        ga.request_events(
            events=["purchase"],
            start=date(2024, 3, 1),
            end=date(2024, 3, 3),
            filters=filters,
            group_by=["event_params.quantity", "user_properties.level", "platform"],
            interval="hour",
        )

    expected_sql = """
        SELECT FORMAT_TIMESTAMP('%Y-%m-%d %H:00:00', TIMESTAMP_TRUNC(TIMESTAMP_MICROS(event_timestamp), HOUR, 'UTC'), 'UTC') AS event_hour, event_name, COUNT(*) AS value, (SELECT props.value.string_value FROM UNNEST(event_params) props WHERE props.key = 'quantity') AS quantity, (SELECT props.value.string_value FROM UNNEST(user_properties) props WHERE props.key = 'level') AS level, platform AS platform
        FROM `proj.dataset.events_*`
        WHERE (event_name IN ('purchase')) AND (EXISTS (SELECT * FROM UNNEST(event_params) WHERE key = 'quantity' AND CAST(value.string_value AS NUMERIC) >= ('10'))) AND (EXISTS (SELECT * FROM UNNEST(user_properties) WHERE key = 'score' AND CAST(value.string_value AS NUMERIC) < ('5'))) AND (REGEXP_EXTRACT(_TABLE_SUFFIX, r'(\\d+)$') BETWEEN '20240301' AND '20240303') AND (TIMESTAMP_MICROS(event_timestamp) BETWEEN TIMESTAMP('2024-03-01T00:00:00+00:00') AND TIMESTAMP('2024-03-03T23:59:59.999999+00:00'))
        GROUP BY event_hour, event_name, quantity, level, platform
        ORDER BY event_hour ASC
        """

    assert captured.value.sql == expected_sql


def test_request_events_sql_snapshot(capture_ga4: type[_CaptureGA4]):
    ga = capture_ga4(table_id="proj.dataset.events_*", tz="UTC", user_id_col="user_id", client=object())

    filters = [
        {"prop": "event_params.currency", "op": "IN", "values": ["USD", "EUR"]},
        {"prop": "user_properties.tier", "op": "=", "values": ["gold"]},
        {"prop": "platform", "op": "!=", "values": ["ANDROID"]},
    ]

    with pytest.raises(_CaptureQuery) as captured:
        ga.request_events(
            events=["purchase", "login"],
            start=date(2024, 1, 1),
            end=date(2024, 1, 7),
            measure="uniques",
            formula="SUM(event_value)",
            filters=filters,
            group_by=["event_params.currency", "geo.country", "user_properties.tier"],
            interval="week",
        )

    expected_sql = """
        SELECT FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(DATE(TIMESTAMP_MICROS(event_timestamp), 'UTC'), WEEK(MONDAY))) AS event_week, event_name, SUM(event_value) AS value, (SELECT props.value.string_value FROM UNNEST(event_params) props WHERE props.key = 'currency') AS currency, geo.country AS country, (SELECT props.value.string_value FROM UNNEST(user_properties) props WHERE props.key = 'tier') AS tier
        FROM `proj.dataset.events_*`
        WHERE (event_name IN ('purchase', 'login')) AND (EXISTS (SELECT * FROM UNNEST(event_params) WHERE key = 'currency' AND value.string_value IN ('USD', 'EUR'))) AND (EXISTS (SELECT * FROM UNNEST(user_properties) WHERE key = 'tier' AND value.string_value = ('gold'))) AND (platform != ('ANDROID')) AND (REGEXP_EXTRACT(_TABLE_SUFFIX, r'(\\d+)$') BETWEEN '20240101' AND '20240107') AND (TIMESTAMP_MICROS(event_timestamp) BETWEEN TIMESTAMP('2024-01-01T00:00:00+00:00') AND TIMESTAMP('2024-01-07T23:59:59.999999+00:00'))
        GROUP BY event_week, event_name, currency, country, tier
        ORDER BY event_week ASC
        """

    assert captured.value.sql == expected_sql


def test_request_funnel_sql_single_step_snapshot(capture_ga4: type[_CaptureGA4]):
    ga = capture_ga4(table_id="proj.dataset.events_*", tz="UTC", user_id_col="user_id", client=object())

    with pytest.raises(_CaptureQuery) as captured:
        ga.request_funnel(
            steps=[FunnelStep(event_name="sign_up")],
            start=date(2024, 1, 1),
            end=date(2024, 1, 31),
            interval="month",
        )

    expected_sql = """WITH
step1 AS (
  SELECT user_id, event_timestamp, FORMAT_DATE('%Y-%m', DATE_TRUNC(DATE(TIMESTAMP_MICROS(event_timestamp), 'UTC'), MONTH)) AS event_month
  FROM `proj.dataset.events_*`
  WHERE event_name = 'sign_up' AND REGEXP_EXTRACT(_TABLE_SUFFIX, r'(\\d+)$') BETWEEN '20240101' AND '20240131' AND TIMESTAMP_MICROS(event_timestamp) BETWEEN TIMESTAMP('2024-01-01T00:00:00+00:00') AND TIMESTAMP('2024-01-31T23:59:59.999999+00:00')
)

SELECT
  event_month,
  COUNT(DISTINCT step1.user_id) AS `1`
FROM step1

GROUP BY event_month
ORDER BY event_month ASC"""

    assert captured.value.sql == expected_sql


def test_request_funnel_sql_snapshot(capture_ga4: type[_CaptureGA4]):
    ga = capture_ga4(
        table_id="proj.dataset.events_*",
        tz="America/New_York",
        user_id_col="user_pseudo_id",
        client=object(),
    )

    steps = [
        FunnelStep(
            event_name="view_item",
            conversion_window_gt=timedelta(seconds=0),
            conversion_window_lt=timedelta(hours=12),
            filters=[{"prop": "event_params.category", "op": "=", "values": ["electronics"]}],
        ),
        FunnelStep(
            event_name="add_to_cart",
            conversion_window_gt=timedelta(minutes=5),
            conversion_window_lt=timedelta(hours=24),
            filters=[{"prop": "user_properties.tier", "op": "IN", "values": ["gold", "silver"]}],
        ),
        FunnelStep(
            event_name="purchase",
            conversion_window_gt=timedelta(minutes=10),
            conversion_window_lt=timedelta(days=2),
        ),
    ]

    with pytest.raises(_CaptureQuery) as captured:
        ga.request_funnel(
            steps=steps,
            start=date(2024, 2, 1),
            end=date(2024, 2, 3),
            group_by=["event_params.device", "geo.country"],
            interval="day",
        )

    expected_sql = """WITH
step1 AS (
  SELECT user_pseudo_id, event_timestamp, FORMAT_DATE('%Y-%m-%d', DATE(TIMESTAMP_MICROS(event_timestamp), 'America/New_York')) AS event_date, (SELECT props.value.string_value FROM UNNEST(event_params) props WHERE props.key = 'device') AS device, geo.country AS country
  FROM `proj.dataset.events_*`
  WHERE event_name = 'view_item' AND EXISTS (SELECT * FROM UNNEST(event_params) WHERE key = 'category' AND value.string_value = ('electronics')) AND REGEXP_EXTRACT(_TABLE_SUFFIX, r'(\\d+)$') BETWEEN '20240201' AND '20240204' AND TIMESTAMP_MICROS(event_timestamp) BETWEEN TIMESTAMP('2024-02-01T00:00:00-05:00') AND TIMESTAMP('2024-02-03T23:59:59.999999-05:00')
),
step2 AS (
  SELECT user_pseudo_id, event_timestamp
  FROM `proj.dataset.events_*`
  WHERE event_name = 'add_to_cart' AND EXISTS (SELECT * FROM UNNEST(user_properties) WHERE key = 'tier' AND value.string_value IN ('gold', 'silver')) AND REGEXP_EXTRACT(_TABLE_SUFFIX, r'(\\d+)$') BETWEEN '20240201' AND '20240205' AND TIMESTAMP_MICROS(event_timestamp) BETWEEN TIMESTAMP('2024-02-01T00:05:00-05:00') AND TIMESTAMP('2024-02-04T23:59:59-05:00')
),
step3 AS (
  SELECT user_pseudo_id, event_timestamp
  FROM `proj.dataset.events_*`
  WHERE event_name = 'purchase' AND REGEXP_EXTRACT(_TABLE_SUFFIX, r'(\\d+)$') BETWEEN '20240201' AND '20240207' AND TIMESTAMP_MICROS(event_timestamp) BETWEEN TIMESTAMP('2024-02-01T00:15:00-05:00') AND TIMESTAMP('2024-02-06T23:59:59-05:00')
)

SELECT
  event_date, device, country,
  COUNT(DISTINCT step1.user_pseudo_id) AS `1`, COUNT(DISTINCT step2.user_pseudo_id) AS `2`, COUNT(DISTINCT step3.user_pseudo_id) AS `3`
FROM step1
LEFT JOIN step2
       ON step2.user_pseudo_id = step1.user_pseudo_id
      AND step2.event_timestamp - step1.event_timestamp > 300000000
      AND step2.event_timestamp - step1.event_timestamp < 86400000000
LEFT JOIN step3
       ON step3.user_pseudo_id = step2.user_pseudo_id
      AND step3.event_timestamp - step2.event_timestamp > 600000000
      AND step3.event_timestamp - step2.event_timestamp < 172800000000
GROUP BY event_date,device, country
ORDER BY event_date ASC"""

    assert captured.value.sql == expected_sql
