"""Shared helper utilities for GA4 BigQuery core logic."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from collections.abc import Iterable, Sequence
from typing import Literal

import pandas as pd

from .types import EventFilter


# SQL literal formatting ----------------------------------------------------

def escape_literal(value: str) -> str:
    """Escape a string so it can safely be inserted as a SQL literal."""

    return value.replace("'", "\\'")


def format_literal(value: object) -> str:
    """Return ``value`` formatted as a quoted SQL literal."""

    return "'{}'".format(escape_literal(str(value)))


def format_literal_list(values: Iterable[object]) -> str:
    """Return ``values`` formatted for use inside ``IN`` style expressions."""

    return "({})".format(", ".join(format_literal(value) for value in values))


# Property path helpers -----------------------------------------------------


@dataclass(frozen=True)
class PropertyPath:
    """Representation of a dotted property path."""

    prefix: str | None
    key: str


NESTED_PROPERTY_PREFIXES = frozenset({"event_params", "user_properties"})


def parse_property_path(path: str) -> PropertyPath:
    """Return the :class:`PropertyPath` describing ``path``."""

    parts = path.split(".")
    prefix = parts[0] if len(parts) > 1 else None
    return PropertyPath(prefix=prefix, key=parts[-1])


# Filter parsing ------------------------------------------------------------


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


def _format_direct_filter(
    prop_with_prefix: str, op: str, values: Sequence[object]
) -> str:
    """Return the SQL predicate for non-nested properties."""

    return f"{prop_with_prefix} {op} {_format_operator_values(op, values)}"


def _values_are_numeric(values: Sequence[object]) -> bool:
    """Return ``True`` if every value matches the permissive numeric regex."""

    return all(_NUMERIC_PATTERN.fullmatch(str(value)) is not None for value in values)


def _value_expression(values: Sequence[object]) -> str:
    """Return the SQL expression that extracts values from a nested record."""

    return (
        "CAST(value.string_value AS NUMERIC)"
        if _values_are_numeric(values)
        else "value.string_value"
    )


def _format_nested_filter(
    prefix: str | None, key: str, op: str, values: Sequence[object]
) -> str:
    """Return the ``EXISTS`` clause for parameter and user property filters."""

    assert (
        prefix is not None
    )  # Defensive: ``parse_property_path`` guarantees this for nested props.
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


# Date and interval utilities ----------------------------------------------


def _parse_date_range(start: date, end: date, tz: str) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Return timezone aware timestamps covering the inclusive date range."""

    if end < start:
        raise ValueError("end must be on or after start")

    start_ts = (
        pd.Timestamp(start)
        .tz_localize(tz)
        .replace(hour=0, minute=0, second=0, microsecond=0)
    )
    end_ts = (
        pd.Timestamp(end)
        .tz_localize(tz)
        .replace(hour=23, minute=59, second=59, microsecond=999_999)
    )
    return start_ts, end_ts


@dataclass(frozen=True)
class _IntervalSpec:
    expression_template: str
    alias: str

    def render(self, tz: str) -> tuple[str, str, str]:
        expression = self.expression_template.format(tz=tz, alias=self.alias)
        return expression, self.alias, self.alias


_INTERVAL_SPECS = {
    "day": _IntervalSpec(
        "FORMAT_DATE('%Y-%m-%d', DATE(TIMESTAMP_MICROS(event_timestamp), '{tz}')) AS {alias}",
        "event_date",
    ),
    "hour": _IntervalSpec(
        "FORMAT_TIMESTAMP('%Y-%m-%d %H:00:00', "
        "TIMESTAMP_TRUNC(TIMESTAMP_MICROS(event_timestamp), HOUR, '{tz}'), '{tz}') AS {alias}",
        "event_hour",
    ),
    "week": _IntervalSpec(
        "FORMAT_DATE('%Y-%m-%d', "
        "DATE_TRUNC(DATE(TIMESTAMP_MICROS(event_timestamp), '{tz}'), WEEK(MONDAY))) AS {alias}",
        "event_week",
    ),
    "month": _IntervalSpec(
        "FORMAT_DATE('%Y-%m', "
        "DATE_TRUNC(DATE(TIMESTAMP_MICROS(event_timestamp), '{tz}'), MONTH)) AS {alias}",
        "event_month",
    ),
}

_INTERVAL_ALIASES = {"date": "day"}


def _build_interval_columns(interval: Literal["day", "hour", "week", "month"], tz: str):
    """Return SQL expressions for truncating timestamps to the desired bucket."""

    normalized = interval.lower()
    normalized = _INTERVAL_ALIASES.get(normalized, normalized)
    spec = _INTERVAL_SPECS.get(normalized)
    if spec is None:
        raise ValueError("interval must be one of: 'day', 'hour', 'week', 'month'")
    return spec.render(tz)


def _table_suffix_condition(table_id: str, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> str | None:
    """Return the ``_TABLE_SUFFIX`` predicate for wildcard tables, if needed."""

    if not table_id.endswith("*"):
        return None

    lo = start_ts.tz_convert("UTC").date().strftime("%Y%m%d")
    hi = end_ts.tz_convert("UTC").date().strftime("%Y%m%d")
    return "REGEXP_EXTRACT(_TABLE_SUFFIX, r'(\\d+)$') BETWEEN '{lo}' AND '{hi}'".format(
        lo=lo, hi=hi
    )


# General query assembly helpers -------------------------------------------


def normalize_group_by(group_by: str | Sequence[str] | None) -> list[str]:
    if group_by is None:
        return []
    if isinstance(group_by, str):
        return [group_by]
    return list(group_by)


def normalize_events(events: str | Sequence[str]) -> list[str]:
    if isinstance(events, str):
        return [events]
    return list(events)


def event_name_condition(events: str | Sequence[str]) -> str:
    normalized_events = normalize_events(events)
    return f"event_name IN {format_literal_list(normalized_events)}"


def compile_filters(filters: Sequence[EventFilter] | None) -> list[str]:
    return _parse_filters(filters)


def join_where_clauses(clauses: Sequence[str], *, operator: str = "AND") -> str:
    """Join ``clauses`` with ``operator`` while wrapping each clause in parentheses."""

    joined = f" {operator} ".join(f"({clause})" for clause in clauses)
    return joined


def join_where_clauses_or(clauses: Sequence[str]) -> str:
    return join_where_clauses(clauses, operator="OR")


def timestamp_condition(start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> str:
    return (
        "TIMESTAMP_MICROS(event_timestamp) BETWEEN "
        "TIMESTAMP('{start}') AND TIMESTAMP('{end}')"
    ).format(start=start_ts.isoformat(), end=end_ts.isoformat())


def table_suffix_clauses(
    table_id: str, start_ts: pd.Timestamp, end_ts: pd.Timestamp
) -> tuple[str, ...]:
    clause = _table_suffix_condition(table_id, start_ts, end_ts)
    return (clause,) if clause else tuple()


def prepare_result_dataframe(df: pd.DataFrame, interval_alias: str) -> pd.DataFrame:
    df[interval_alias] = pd.to_datetime(df[interval_alias])
    return df


def _parse_group_by(group_by: Sequence[str]) -> tuple[list[str], list[str]]:
    """Translate a ``GROUP BY`` specification into SQL select statements."""

    statements: list[str] = []
    aliases: list[str] = []

    for prop_with_prefix in group_by or []:
        path = parse_property_path(prop_with_prefix)
        alias = path.key

        prefix = path.prefix
        if prefix in NESTED_PROPERTY_PREFIXES:
            statements.append(
                "(SELECT props.value.string_value FROM UNNEST({prefix}) props WHERE props.key = '{key}') "
                "AS {alias}".format(prefix=prefix, key=alias, alias=alias)
            )
        else:
            statements.append(f"{prop_with_prefix} AS {alias}")
        aliases.append(alias)

    return statements, aliases


__all__ = [
    "PropertyPath",
    "NESTED_PROPERTY_PREFIXES",
    "parse_property_path",
    "escape_literal",
    "format_literal",
    "format_literal_list",
    "_parse_filters",
    "_parse_date_range",
    "_build_interval_columns",
    "_table_suffix_condition",
    "normalize_group_by",
    "normalize_events",
    "event_name_condition",
    "compile_filters",
    "join_where_clauses",
    "join_where_clauses_or",
    "timestamp_condition",
    "table_suffix_clauses",
    "prepare_result_dataframe",
    "_parse_group_by",
]
