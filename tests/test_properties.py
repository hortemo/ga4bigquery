"""Tests for the public GA4 property helpers."""

from __future__ import annotations

import pytest

from ga4bigquery import Properties


def test_geo_properties_are_available() -> None:
    assert Properties.geo.country == "geo.country"
    assert Properties.geo.region == "geo.region"
    assert Properties.geo.city == "geo.city"


def test_device_properties_are_available() -> None:
    assert Properties.device.category == "device.category"
    assert Properties.device.mobile_brand_name == "device.mobile_brand_name"
    assert Properties.device.mobile_model_name == "device.mobile_model_name"
    assert Properties.device.mobile_marketing_name == "device.mobile_marketing_name"
    assert Properties.device.language == "device.language"


def test_app_info_and_platform_properties() -> None:
    assert Properties.app_info.version == "app_info.version"
    assert Properties.platform == "platform"


def test_event_params_and_user_properties_allow_custom_keys() -> None:
    assert Properties.event_params.currency == "event_params.currency"
    assert Properties.event_params["tier"] == "event_params.tier"
    assert Properties.event_param("level") == "event_params.level"

    assert Properties.user_properties.score == "user_properties.score"
    assert Properties.user_properties("rank") == "user_properties.rank"
    assert Properties.user_property("status") == "user_properties.status"


def test_unknown_property_raises_attribute_error() -> None:
    with pytest.raises(AttributeError):
        _ = Properties.geo.continent

