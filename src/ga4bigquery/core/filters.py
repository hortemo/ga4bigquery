"""Utilities for translating filter objects into SQL predicates."""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import List

from ._properties import NESTED_PROPERTY_PREFIXES, parse_property_path
from .sql import format_literal_list
from .types import EventFilter

_NUMERIC_PATTERN = re.compile(r"-?\d+(\.\d+)?")


def _parse_filters(filters: List[EventFilter]) -> List[str]:
    """Convert a list of :class:`EventFilter` objects into SQL predicates."""

    return [_parse_filter(filter_) for filter_ in (filters or [])]


def _parse_filter(filter_: EventFilter) -> str:
    prop_with_prefix = filter_["prop"]
    path = parse_property_path(prop_with_prefix)
    op = filter_["op"]

    values = list(filter_["values"])
    values_sql = format_literal_list(values)

    prefix = path.prefix
    if prefix in NESTED_PROPERTY_PREFIXES:
        value_expr = _value_expression(values)
        return _format_nested_filter(prefix, path.key, op, value_expr, values_sql)

    return f"{prop_with_prefix} {op} {values_sql}"


def _values_are_numeric(values: Sequence[object]) -> bool:
    """Return ``True`` if every value matches the permissive numeric regex."""

    return all(_NUMERIC_PATTERN.fullmatch(str(value)) is not None for value in values)


def _value_expression(values: Sequence[object]) -> str:
    """Return the SQL expression that extracts values from a nested record."""

    return "CAST(value.string_value AS NUMERIC)" if _values_are_numeric(values) else "value.string_value"


def _format_nested_filter(prefix: str, key: str, op: str, value_expr: str, values_sql: str) -> str:
    """Return the ``EXISTS`` clause for parameter and user property filters."""

    return (
        "EXISTS (SELECT * FROM UNNEST({prefix}) WHERE key = '{key}' "
        "AND {value_expr} {op} {values})"
    ).format(prefix=prefix, key=key, value_expr=value_expr, op=op, values=values_sql)
