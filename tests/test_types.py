from __future__ import annotations

from datetime import timedelta

import pytest

from ga4bigquery.core.types import FunnelStep


def test_funnel_step_rejects_negative_lower_window() -> None:
    with pytest.raises(ValueError, match="conversion_window_gt must be non-negative"):
        FunnelStep(event_name="purchase", conversion_window_gt=timedelta(minutes=-1))


def test_funnel_step_rejects_upper_bound_not_greater_than_lower() -> None:
    with pytest.raises(ValueError, match="conversion_window_lt must be greater than conversion_window_gt"):
        FunnelStep(
            event_name="purchase",
            conversion_window_gt=timedelta(minutes=10),
            conversion_window_lt=timedelta(minutes=10),
        )

