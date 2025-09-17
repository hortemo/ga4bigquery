"""Public helpers for referencing GA4 BigQuery property paths."""

from __future__ import annotations

from typing import Mapping

__all__ = ["Properties"]


class _PropertyNamespace:
    """Namespace providing dotted property paths for a given prefix."""

    def __init__(
        self,
        *,
        prefix: str | None,
        properties: Mapping[str, str],
        allow_dynamic: bool = False,
    ) -> None:
        object.__setattr__(self, "_prefix", prefix)
        object.__setattr__(self, "_allow_dynamic", allow_dynamic)
        object.__setattr__(self, "_properties", dict(properties))

        for name, value in properties.items():
            object.__setattr__(self, name, value)

    def __getattr__(self, item: str) -> str:
        properties = object.__getattribute__(self, "_properties")
        if item in properties:
            return properties[item]

        prefix = object.__getattribute__(self, "_prefix")
        allow_dynamic = object.__getattribute__(self, "_allow_dynamic")
        if allow_dynamic and prefix:
            return f"{prefix}.{item}"

        raise AttributeError(f"Unknown property '{item}'")

    def __getitem__(self, item: str) -> str:
        properties = object.__getattribute__(self, "_properties")
        if item in properties:
            return properties[item]

        prefix = object.__getattribute__(self, "_prefix")
        allow_dynamic = object.__getattribute__(self, "_allow_dynamic")
        if allow_dynamic and prefix:
            return f"{prefix}.{item}"

        raise KeyError(item)

    def __call__(self, item: str) -> str:
        prefix = object.__getattribute__(self, "_prefix")
        allow_dynamic = object.__getattribute__(self, "_allow_dynamic")
        if allow_dynamic and prefix:
            return f"{prefix}.{item}"

        raise TypeError("This namespace does not support dynamic properties")

    def __setattr__(self, key, value):  # pragma: no cover - defensive
        raise AttributeError("Properties are read-only")


class _RootProperties:
    """Container exposing GA4 property paths as attributes."""

    __slots__ = (
        "geo",
        "device",
        "event_params",
        "user_properties",
        "app_info",
        "platform",
    )

    def __init__(self) -> None:
        object.__setattr__(
            self,
            "geo",
            _PropertyNamespace(
                prefix="geo",
                properties={
                    "country": "geo.country",
                    "region": "geo.region",
                    "city": "geo.city",
                },
            ),
        )
        object.__setattr__(
            self,
            "device",
            _PropertyNamespace(
                prefix="device",
                properties={
                    "category": "device.category",
                    "mobile_brand_name": "device.mobile_brand_name",
                    "mobile_model_name": "device.mobile_model_name",
                    "mobile_marketing_name": "device.mobile_marketing_name",
                    "language": "device.language",
                },
            ),
        )
        object.__setattr__(
            self,
            "event_params",
            _PropertyNamespace(prefix="event_params", properties={}, allow_dynamic=True),
        )
        object.__setattr__(
            self,
            "user_properties",
            _PropertyNamespace(prefix="user_properties", properties={}, allow_dynamic=True),
        )
        object.__setattr__(
            self,
            "app_info",
            _PropertyNamespace(
                prefix="app_info",
                properties={"version": "app_info.version"},
            ),
        )
        object.__setattr__(self, "platform", "platform")

    def __setattr__(self, key, value):  # pragma: no cover - defensive
        raise AttributeError("Properties are read-only")

    def event_param(self, key: str) -> str:
        """Return the dotted path for a custom event parameter."""

        return self.event_params[key]

    def user_property(self, key: str) -> str:
        """Return the dotted path for a custom user property."""

        return self.user_properties[key]


Properties = _RootProperties()
