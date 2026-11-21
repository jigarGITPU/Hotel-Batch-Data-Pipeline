"""
Integration tests for Watermark Management, Idempotent UPSERT, and Incremental Processing.
"""

import pytest
import pandas as pd
from datetime import datetime, timezone
from src.orchestration.pipeline_runner import HotelPipelineRunner
from src.loading.warehouse_loader import WarehouseLoader
from src.utils.database import db


def test_idempotent_re_execution():
    """Rerunning the pipeline over the same source data must produce zero duplicate facts."""
    runner = HotelPipelineRunner(db)
    tbl_bkg = db.get_table_name("analytics_dw", "fact_bookings")

    # Run 1
    runner.run(incremental=False)
    count_1 = int(db.fetch_dataframe(f"SELECT COUNT(*) as cnt FROM {tbl_bkg}").iloc[0]["cnt"])

    # Run 2
    runner.run(incremental=False)
    count_2 = int(db.fetch_dataframe(f"SELECT COUNT(*) as cnt FROM {tbl_bkg}").iloc[0]["cnt"])

    # Assert exactly identical row count (no duplicates)
    assert count_1 == count_2


def test_watermark_tracking():
    """Verify watermark advances after successful run."""
    loader = WarehouseLoader(db)
    now_utc = datetime.now(timezone.utc)
    batch_test = "batch_test_watermark_001"

    loader.update_watermark("test_source", now_utc, batch_test)
    stored_wm = loader.get_watermark("test_source")

    assert stored_wm is not None
    # Compare timestamps (within 1 second)
    if isinstance(stored_wm, str):
        stored_dt = datetime.fromisoformat(stored_wm.replace("Z", "+00:00"))
    else:
        stored_dt = stored_wm
    assert abs((stored_dt - now_utc).total_seconds()) < 2.0


def test_record_update_via_upsert():
    """Verify that updating an existing booking line item updates the fact record."""
    loader = WarehouseLoader(db)
    batch_id = "test_upsert_update_batch"

    # Initial booking
    df_init = pd.DataFrame([{
        "booking_id": "BKG-TEST-9999",
        "booking_line_id": "BKG-TEST-9999-01",
        "hotel_id": "HTL-GAU-001",
        "canonical_guest_id": "GST-0000001",
        "room_id": "RM-001-001",
        "booking_date_key": 20260115,
        "check_in_date_key": 20260120,
        "check_out_date_key": 20260123,
        "booking_status": "CONFIRMED",
        "room_rate": 5000.0,
        "currency": "INR",
        "exchange_rate_used": 1.0,
        "room_rate_inr": 5000.0,
        "total_amount_inr": 15000.0,
        "number_of_guests": 2,
        "lead_time_days": 5,
        "length_of_stay_days": 3,
        "source_updated_at": "2026-01-15T10:00:00Z"
    }])
    loader.upsert_fact_bookings(df_init, batch_id)

    tbl = db.get_table_name("analytics_dw", "fact_bookings")
    res_1 = db.fetch_dataframe(f"SELECT booking_status, total_amount_inr FROM {tbl} WHERE booking_line_id = 'BKG-TEST-9999-01'")
    assert res_1.iloc[0]["booking_status"] == "CONFIRMED"
    assert float(res_1.iloc[0]["total_amount_inr"]) == 15000.0

    # Updated booking (status change to CHECKED_IN, rate update)
    df_update = pd.DataFrame([{
        "booking_id": "BKG-TEST-9999",
        "booking_line_id": "BKG-TEST-9999-01",
        "hotel_id": "HTL-GAU-001",
        "canonical_guest_id": "GST-0000001",
        "room_id": "RM-001-001",
        "booking_date_key": 20260115,
        "check_in_date_key": 20260120,
        "check_out_date_key": 20260123,
        "booking_status": "CHECKED_IN",
        "room_rate": 6000.0,
        "currency": "INR",
        "exchange_rate_used": 1.0,
        "room_rate_inr": 6000.0,
        "total_amount_inr": 18000.0,
        "number_of_guests": 2,
        "lead_time_days": 5,
        "length_of_stay_days": 3,
        "source_updated_at": "2026-01-20T14:00:00Z"
    }])
    loader.upsert_fact_bookings(df_update, f"{batch_id}_v2")

    res_2 = db.fetch_dataframe(f"SELECT booking_status, total_amount_inr FROM {tbl} WHERE booking_line_id = 'BKG-TEST-9999-01'")
    assert res_2.iloc[0]["booking_status"] == "CHECKED_IN"
    assert float(res_2.iloc[0]["total_amount_inr"]) == 18000.0
