from __future__ import annotations

from datetime import date

import pytest

from ga4bigquery.core.dates import _parse_date_range


def test_parse_date_range_rejects_end_before_start() -> None:
    with pytest.raises(ValueError, match="end must be on or after start"):
        _parse_date_range(date(2024, 1, 2), date(2024, 1, 1), "UTC")

