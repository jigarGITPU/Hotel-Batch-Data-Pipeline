"""
Unit tests for Transformation modules (Currency, Room Mapping, Guest Deduplication, Cleaning).
"""

import pytest
import pandas as pd
from src.transformation.currency import CurrencyTransformer
from src.transformation.room_mapping import RoomTypeMapper
from src.transformation.guest_deduplication import GuestDeduplicator
from src.transformation.cleaning import clean_bookings, clean_stays, clean_reviews, parse_date_to_key
from src.utils.database import db


def test_currency_conversion():
    transformer = CurrencyTransformer(db)

    # 1. INR (1:1)
    inr_amt, rate, ok = transformer.convert_to_inr(5000.0, "INR")
    assert ok is True
    assert inr_amt == 5000.0
    assert rate == 1.0

    # 2. USD (86.5)
    usd_amt, rate, ok = transformer.convert_to_inr(100.0, "USD")
    assert ok is True
    assert usd_amt == 8650.0
    assert rate == 86.5

    # 3. Invalid currency
    inv_amt, rate, ok = transformer.convert_to_inr(100.0, "INVALID_CODE")
    assert ok is False
    assert inv_amt is None


def test_room_type_mapping():
    mapper = RoomTypeMapper(db)

    # Direct canonical
    c_type, cap, ok = mapper.map_room_type("Deluxe King")
    assert ok is True
    assert c_type == "Deluxe King"
    assert cap == 2

    # Inconsistent / messy source descriptions
    c_type2, cap2, ok2 = mapper.map_room_type("king deluxe")
    assert ok2 is True
    assert c_type2 == "Deluxe King"

    c_type3, cap3, ok3 = mapper.map_room_type("STD_DBL")
    assert ok3 is True
    assert c_type3 == "Standard Double"
    assert cap3 == 2

    c_type4, cap4, ok4 = mapper.map_room_type("EXEC_SUITE")
    assert ok4 is True
    assert c_type4 == "Executive Suite"
    assert cap4 == 4


def test_guest_deduplication():
    dedup = GuestDeduplicator(db)

    # Test two records with identical email
    df_raw = pd.DataFrame([
        {"guest_id": "GST-99901", "first_name": "Aarav", "last_name": "Sharma", "email": "aarav.sharma@example.com", "phone": "+91-9876543210"},
        {"guest_id": "GST-99902", "first_name": "Aarav", "last_name": "Sharma", "email": "aarav.sharma@example.com", "phone": "+91-9876543210"},
        {"guest_id": "GST-99903", "first_name": "Vivaan", "last_name": "Bora", "email": "vivaan.bora@example.com", "phone": "+91-9123456789"},
    ])

    df_canonical, df_mappings = dedup.deduplicate_guests(df_raw)

    # GST-99901 and GST-99902 must map to the same canonical guest ID
    can_1 = dedup.get_canonical_id("GST-99901")
    can_2 = dedup.get_canonical_id("GST-99902")
    assert can_1 == can_2
    assert can_1 == "GST-99901"

    # GST-99903 must remain independent
    can_3 = dedup.get_canonical_id("GST-99903")
    assert can_3 == "GST-99903"
    assert can_3 != can_1


def test_date_key_and_metrics_cleaning():
    assert parse_date_to_key("2026-05-18") == 20260518
    assert parse_date_to_key(None) is None
    assert parse_date_to_key("") is None

    df_bkg = pd.DataFrame([{
        "booking_id": "BKG-001",
        "booking_date": "2026-01-10",
        "check_in_date": "2026-01-15",
        "check_out_date": "2026-01-18",
        "room_rate_inr": 5000.0,
        "number_of_guests": 2
    }])

    cleaned = clean_bookings(df_bkg)
    assert cleaned.iloc[0]["booking_date_key"] == 20260110
    assert cleaned.iloc[0]["lead_time_days"] == 5
    assert cleaned.iloc[0]["length_of_stay_days"] == 3
    assert cleaned.iloc[0]["total_amount_inr"] == 15000.0
