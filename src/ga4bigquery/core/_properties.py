"""Internal helpers for working with dotted property paths."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PropertyPath:
    """Representation of a dotted property path.

    The GA4 export schema stores dynamic event parameters and user properties as
    repeated records.  When expressed as SQL the columns are referenced using a
    ``prefix.key`` syntax (for instance ``event_params.currency``).  Several
    places in the codebase need to tease apart the prefix portion and the final
    key name.  Having a small helper object keeps that parsing logic in one
    place and makes the intent explicit at call sites.
    """

    prefix: str | None
    key: str


NESTED_PROPERTY_PREFIXES = frozenset({"event_params", "user_properties"})


def parse_property_path(path: str) -> PropertyPath:
    """Return the :class:`PropertyPath` describing ``path``.

    The function accepts strings both with and without a prefix.  A missing
    prefix is represented as ``None`` in the returned dataclass which keeps the
    downstream code straightforward and type-safe.
    """

    parts = path.split(".")
    prefix = parts[0] if len(parts) > 1 else None
    return PropertyPath(prefix=prefix, key=parts[-1])

