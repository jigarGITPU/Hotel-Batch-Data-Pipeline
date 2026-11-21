"""
Unit tests for Data Quality Validation Framework and Quarantine Management.
"""

import pytest
import pandas as pd
from src.quality.validation import DataQualityValidator
from src.quality.quarantine import QuarantineManager
from src.utils.database import db


def test_quality_validation_catches_anomalies():
    validator = DataQualityValidator(db)
    batch_id = "test_quality_batch_001"

    # Dirty bookings DataFrame
    df_dirty = pd.DataFrame([
        # Valid row
        {"booking_id": "BKG-101", "booking_line_id": "BKG-101-01", "guest_id": "GST-001", "hotel_id": "HTL-GAU-001", "room_rate": 4500.0, "currency": "INR", "check_in_date": "2026-02-01", "check_out_date": "2026-02-03"},
        # Null booking ID
        {"booking_id": "", "booking_line_id": "BKG-102-01", "guest_id": "GST-001", "hotel_id": "HTL-GAU-001", "room_rate": 4500.0, "currency": "INR", "check_in_date": "2026-02-01", "check_out_date": "2026-02-03"},
        # Null guest ID
        {"booking_id": "BKG-103", "booking_line_id": "BKG-103-01", "guest_id": "", "hotel_id": "HTL-GAU-001", "room_rate": 4500.0, "currency": "INR", "check_in_date": "2026-02-01", "check_out_date": "2026-02-03"},
        # Invalid negative price
        {"booking_id": "BKG-104", "booking_line_id": "BKG-104-01", "guest_id": "GST-001", "hotel_id": "HTL-GAU-001", "room_rate": -500.0, "currency": "INR", "check_in_date": "2026-02-01", "check_out_date": "2026-02-03"},
        # Invalid currency
        {"booking_id": "BKG-105", "booking_line_id": "BKG-105-01", "guest_id": "GST-001", "hotel_id": "HTL-GAU-001", "room_rate": 4500.0, "currency": "INVALID_CURR", "check_in_date": "2026-02-01", "check_out_date": "2026-02-03"},
        # Invalid dates (checkout before checkin)
        {"booking_id": "BKG-106", "booking_line_id": "BKG-106-01", "guest_id": "GST-001", "hotel_id": "HTL-GAU-001", "room_rate": 4500.0, "currency": "INR", "check_in_date": "2026-02-05", "check_out_date": "2026-02-02"},
        # Missing hotel reference
        {"booking_id": "BKG-107", "booking_line_id": "BKG-107-01", "guest_id": "GST-001", "hotel_id": "HTL-UNKNOWN-999", "room_rate": 4500.0, "currency": "INR", "check_in_date": "2026-02-01", "check_out_date": "2026-02-03"},
    ])

    df_valid, quarantined, checks = validator.validate_bookings(df_dirty, batch_id)

    # Only BKG-101 is valid
    assert len(df_valid) == 1
    assert df_valid.iloc[0]["booking_id"] == "BKG-101"

    # 6 invalid records must be quarantined
    reason_codes = [q["reason_code"] for q in quarantined]
    assert "NULL_BOOKING_ID" in reason_codes
    assert "NULL_GUEST_ID" in reason_codes
    assert "INVALID_PRICE" in reason_codes
    assert "INVALID_CURRENCY" in reason_codes
    assert "INVALID_DATE" in reason_codes
    assert "MISSING_HOTEL_REFERENCE" in reason_codes


def test_inventory_negative_rooms_validation():
    validator = DataQualityValidator(db)
    batch_id = "test_inv_batch_001"

    df_inv = pd.DataFrame([
        {"inventory_id": "INV-001", "available_rooms": 10, "total_rooms": 10},
        {"inventory_id": "INV-002", "available_rooms": -5, "total_rooms": 10},  # Negative
    ])

    df_valid, quarantined, _ = validator.validate_inventory(df_inv, batch_id)
    assert len(df_valid) == 1
    assert len(quarantined) == 1
    assert quarantined[0]["reason_code"] == "NEGATIVE_INVENTORY"


def test_reviews_rating_bounds_validation():
    validator = DataQualityValidator(db)
    batch_id = "test_rev_batch_001"

    df_rev = pd.DataFrame([
        {"review_id": "REV-001", "rating": 5},
        {"review_id": "REV-002", "rating": 1},
        {"review_id": "REV-003", "rating": 6},  # Invalid > 5
        {"review_id": "REV-004", "rating": 0},  # Invalid < 1
    ])

    df_valid, quarantined, _ = validator.validate_reviews(df_rev, batch_id)
    assert len(df_valid) == 2
    assert len(quarantined) == 2
    for q in quarantined:
        assert q["reason_code"] == "INVALID_RATING"
