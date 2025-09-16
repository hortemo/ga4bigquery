from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import List, Literal, TypedDict

EventOperator = Literal["IN", "NOT IN", "=", "!=", ">", "<", ">=", "<="]


class EventFilter(TypedDict):
    prop: str
    op: EventOperator
    values: List[str]


@dataclass
class FunnelStep:
    event_name: str
    conversion_window_gt: timedelta = timedelta(seconds=0)
    conversion_window_lt: timedelta = timedelta(days=30)
    filters: List[EventFilter] = field(default_factory=list)
