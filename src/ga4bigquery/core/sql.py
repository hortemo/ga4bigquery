"""Helper utilities for constructing SQL fragments.

This module intentionally keeps the string formatting logic in one place so that
the rest of the codebase can focus on the semantics of a query.  Having a
single source of truth for string escaping and literal handling reduces the
chance of subtle mistakes (for instance forgetting to escape a single quote),
while also making the generated SQL deterministic which is important for the
snapshot style tests in the repository.
"""

from __future__ import annotations

from collections.abc import Iterable


def escape_literal(value: str) -> str:
    """Escape a string so it can safely be inserted as a SQL literal."""

    return value.replace("'", "\\'")


def format_literal(value: object) -> str:
    """Return ``value`` formatted as a quoted SQL literal.

    The implementation mirrors the previous hand written ``format`` calls so
    that the resulting SQL remains byte-for-byte identical with the historical
    output that the tests assert on.
    """

    return "'{}'".format(escape_literal(str(value)))


def format_literal_list(values: Iterable[object]) -> str:
    """Return ``values`` formatted for use inside ``IN`` style expressions."""

    return "({})".format(", ".join(format_literal(value) for value in values))

