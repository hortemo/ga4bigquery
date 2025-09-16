from __future__ import annotations

import re
from typing import List

from .types import EventFilter


def _parse_filters(filters: List[EventFilter]) -> List[str]:
    return [_parse_filter(f) for f in (filters or [])]


def _parse_filter(filter: EventFilter) -> str:
    prop_with_prefix = filter["prop"]
    parts = prop_with_prefix.split(".")
    prefix = parts[0] if len(parts) > 1 else None
    prop_without_prefix = parts[-1]
    op = filter["op"]

    values = filter["values"]
    values_sql = "({})".format(", ".join("'{}'".format(str(x).replace("'", "\\'")) for x in values))

    if prefix in {"event_params", "user_properties"}:
        values_are_numeric = all(re.fullmatch(r"-?\d+(\.\d+)?", str(v)) is not None for v in values)
        value_expr = "CAST(value.string_value AS INT64)" if values_are_numeric else "value.string_value"
        return (
            "EXISTS (SELECT * FROM UNNEST({prefix}) WHERE key = '{key}' "
            "AND {value_expr} {op} {values})"
        ).format(prefix=prefix, key=prop_without_prefix, value_expr=value_expr, op=op, values=values_sql)

    return f"{prop_with_prefix} {op} {values_sql}"
