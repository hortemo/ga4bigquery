"""Public data structures used by the GA4 BigQuery client."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import List, Literal, TypedDict

__all__ = ["EventFilter", "EventOperator", "FunnelStep"]

EventOperator = Literal["IN", "NOT IN", "=", "!=", ">", "<", ">=", "<="]


class EventFilter(TypedDict):
    """Typed mapping describing a filter applied to GA4 events."""

    prop: str
    op: EventOperator
    values: List[object]


@dataclass
class FunnelStep:
    """Configuration describing a single step in a funnel."""

    event_name: str
    conversion_window_gt: timedelta = timedelta(seconds=0)
    conversion_window_lt: timedelta = timedelta(days=30)
    filters: List[EventFilter] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.conversion_window_gt < timedelta(seconds=0):
            raise ValueError("conversion_window_gt must be non-negative")
        if self.conversion_window_lt <= self.conversion_window_gt:
            raise ValueError("conversion_window_lt must be greater than conversion_window_gt")
