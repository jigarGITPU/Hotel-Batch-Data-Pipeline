"""
Unit tests for simulated Hotel REST API endpoints.
Tests endpoint handler logic, responses, and schemas directly.
"""

import pytest
from api.main import health_check, get_hotels, get_bookings, APIResponse


def test_health_check_endpoint():
    """Verify health endpoint returns status UP and service identifier."""
    res = health_check()
    assert res["status"] == "UP"
    assert "service" in res
    assert "Guwahati" in res["service"]


def test_hotels_endpoint():
    """Verify get_hotels returns exactly 14 Guwahati hotel properties."""
    res = get_hotels()
    assert res["status"] == "success"
    assert res["count"] == 14
    assert len(res["data"]) == 14
    for prop in res["data"]:
        assert "hotel_id" in prop
        assert "name" in prop
        assert "location" in prop


def test_bookings_endpoint_pagination():
    """Verify get_bookings returns paginated results with proper metadata."""
    res = get_bookings(target_date=None, hotel_id=None, page=1, page_size=20)
    assert isinstance(res, APIResponse)
    assert res.status == "success"
    assert res.metadata.page == 1
    assert res.metadata.page_size == 20
    assert len(res.data) <= 20
