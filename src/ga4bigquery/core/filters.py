"""Utilities for translating filter objects into SQL predicates."""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import List

from .sql import format_literal_list
from .types import EventFilter

_NUMERIC_PATTERN = re.compile(r"-?\d+(\.\d+)?")


def _parse_filters(filters: List[EventFilter]) -> List[str]:
    """Convert a list of :class:`EventFilter` objects into SQL predicates."""

    return [_parse_filter(filter_) for filter_ in (filters or [])]


def _parse_filter(filter_: EventFilter) -> str:
    prop_with_prefix = filter_["prop"]
    parts = prop_with_prefix.split(".")
    prefix = parts[0] if len(parts) > 1 else None
    prop_without_prefix = parts[-1]
    op = filter_["op"]

    values = filter_["values"]
    values_sql = format_literal_list(values)

    if prefix in {"event_params", "user_properties"}:
        values_are_numeric = _values_are_numeric(values)
        value_expr = "CAST(value.string_value AS INT64)" if values_are_numeric else "value.string_value"
        return (
            "EXISTS (SELECT * FROM UNNEST({prefix}) WHERE key = '{key}' "
            "AND {value_expr} {op} {values})"
        ).format(prefix=prefix, key=prop_without_prefix, value_expr=value_expr, op=op, values=values_sql)

    return f"{prop_with_prefix} {op} {values_sql}"


def _values_are_numeric(values: Sequence[object]) -> bool:
    """Return ``True`` if every value matches the permissive numeric regex."""

    return all(_NUMERIC_PATTERN.fullmatch(str(value)) is not None for value in values)
