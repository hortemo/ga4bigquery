"""Utilities for translating filter objects into SQL predicates."""

from __future__ import annotations

import re
from collections.abc import Sequence

from ._properties import NESTED_PROPERTY_PREFIXES, parse_property_path
from .sql import format_literal, format_literal_list
from .types import EventFilter

_NUMERIC_PATTERN = re.compile(r"-?\d+(\.\d+)?")


def _parse_filters(filters: Sequence[EventFilter] | None) -> list[str]:
    """Convert :class:`EventFilter` objects into SQL predicates."""

    if not filters:
        return []
    return [_parse_filter(filter_) for filter_ in filters]


def _parse_filter(filter_: EventFilter) -> str:
    prop_with_prefix = filter_["prop"]
    path = parse_property_path(prop_with_prefix)
    op = filter_["op"]
    values: tuple[object, ...] = tuple(filter_["values"])

    if path.prefix in NESTED_PROPERTY_PREFIXES:
        return _format_nested_filter(path.prefix, path.key, op, values)

    return _format_direct_filter(prop_with_prefix, op, values)


_SCALAR_OPERATORS = {"=", "!=", ">", "<", ">=", "<="}


def _format_operator_values(op: str, values: Sequence[object]) -> str:
    """Return the SQL representation for ``values`` under ``op``."""

    if op in {"IN", "NOT IN"}:
        if not values:
            raise ValueError("IN style operators require at least one value")
        return format_literal_list(values)

    if op in _SCALAR_OPERATORS:
        if len(values) != 1:
            raise ValueError("Comparison operators require exactly one value")
        return format_literal(values[0])

    raise ValueError(f"Unsupported operator: {op}")


def _format_direct_filter(prop_with_prefix: str, op: str, values: Sequence[object]) -> str:
    """Return the SQL predicate for non-nested properties."""

    return f"{prop_with_prefix} {op} {_format_operator_values(op, values)}"


def _values_are_numeric(values: Sequence[object]) -> bool:
    """Return ``True`` if every value matches the permissive numeric regex."""

    return all(_NUMERIC_PATTERN.fullmatch(str(value)) is not None for value in values)


def _value_expression(values: Sequence[object]) -> str:
    """Return the SQL expression that extracts values from a nested record."""

    return "CAST(value.string_value AS NUMERIC)" if _values_are_numeric(values) else "value.string_value"


def _format_nested_filter(prefix: str | None, key: str, op: str, values: Sequence[object]) -> str:
    """Return the ``EXISTS`` clause for parameter and user property filters."""

    assert prefix is not None  # Defensive: ``parse_property_path`` guarantees this for nested props.
    value_expr = _value_expression(values)
    values_sql = _format_operator_values(op, values)
    key_literal = format_literal(key)
    return (
        "EXISTS (SELECT * FROM UNNEST({prefix}) WHERE key = {key_literal} "
        "AND {value_expr} {op} {values_sql})"
    ).format(
        prefix=prefix,
        key_literal=key_literal,
        value_expr=value_expr,
        op=op,
        values_sql=values_sql,
    )
