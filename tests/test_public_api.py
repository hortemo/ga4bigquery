"""Tests for the public package API exports."""

from typing import get_args

from ga4bigquery import EventOperator


def test_event_operator_is_exposed() -> None:
    """EventOperator should be available from the top-level package."""

    assert "IN" in get_args(EventOperator)
