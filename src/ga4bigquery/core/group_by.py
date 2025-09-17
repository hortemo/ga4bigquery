"""Support for turning ``GROUP BY`` instructions into SQL snippets."""

from __future__ import annotations

from typing import List, Tuple

from ._properties import NESTED_PROPERTY_PREFIXES, parse_property_path


def _parse_group_by(group_by: List[str]) -> Tuple[List[str], List[str]]:
    """Translate a ``GROUP BY`` specification into SQL select statements."""

    statements: List[str] = []
    aliases: List[str] = []

    for prop_with_prefix in (group_by or []):
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
