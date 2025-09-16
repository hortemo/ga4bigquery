"""Support for turning ``GROUP BY`` instructions into SQL snippets."""

from __future__ import annotations

from typing import List, Tuple


def _parse_group_by(group_by: List[str]) -> Tuple[List[str], List[str]]:
    """Translate a ``GROUP BY`` specification into SQL select statements."""

    statements: List[str] = []
    aliases: List[str] = []

    for prop_with_prefix in (group_by or []):
        parts = prop_with_prefix.split(".")
        prefix = parts[0] if len(parts) > 1 else None
        prop_without_prefix = parts[-1]

        if prefix in {"event_params", "user_properties"}:
            statements.append(
                "(SELECT props.value.string_value FROM UNNEST({prefix}) props WHERE props.key = '{key}') "
                "AS {alias}".format(prefix=prefix, key=prop_without_prefix, alias=prop_without_prefix)
            )
        else:
            statements.append(f"{prop_with_prefix} AS {prop_without_prefix}")
        aliases.append(prop_without_prefix)

    return statements, aliases
