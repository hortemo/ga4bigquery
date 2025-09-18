"""Public package API."""

from importlib import metadata

from .core import EventFilter, FunnelStep, GA4BigQuery

__all__ = ["GA4BigQuery", "FunnelStep", "EventFilter"]

try:
    __version__ = metadata.version("ga4bigquery")
except (
    metadata.PackageNotFoundError
):  # pragma: no cover - fallback for editable installs
    __version__ = "0.0.0"
