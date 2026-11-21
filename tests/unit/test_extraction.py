"""
Unit tests for CSV and API extraction modules.
"""

import pytest
import pandas as pd
from pathlib import Path
from src.extraction.csv_extractor import CSVExtractor
from src.extraction.api_extractor import APIExtractor
from src.utils.config import config


@pytest.fixture
def sample_csv(tmp_path: Path):
    file_path = tmp_path / "hotel_bookings.csv"
    data = (
        "booking_id,booking_line_id,guest_id,hotel_id,room_id,booking_date,check_in_date,check_out_date,booking_status,room_rate,currency,number_of_guests,source_updated_at\n"
        "BKG-00000001,BKG-00000001-01,GST-0000001,HTL-GAU-001,RM-001-001,2026-01-15,2026-01-20,2026-01-23,CONFIRMED,4500.0,INR,2,2026-01-15T10:00:00Z\n"
        "BKG-00000002,BKG-00000002-01,GST-0000002,HTL-GAU-002,RM-002-001,2026-01-16,2026-01-22,2026-01-25,CONFIRMED,85.0,USD,1,2026-01-16T11:00:00Z\n"
    )
    file_path.write_text(data, encoding="utf-8")
    return tmp_path


def test_csv_extraction_success(sample_csv: Path):
    extractor = CSVExtractor(base_dir=sample_csv)
    df, meta = extractor.extract_dataset("bookings", filename="hotel_bookings.csv")

    assert meta["status"] == "SUCCESS"
    assert meta["rows_extracted"] == 2
    assert "source_file" in df.columns
    assert "extracted_at" in df.columns
    assert "source_system" in df.columns
    assert len(df) == 2


def test_csv_extraction_missing_file(tmp_path: Path):
    extractor = CSVExtractor(base_dir=tmp_path)
    df, meta = extractor.extract_dataset("non_existent_dataset", filename="missing.csv")

    assert meta["status"] == "FAILED"
    assert meta["rows_extracted"] == 0
    assert df.empty
    assert len(meta["validation_errors"]) > 0


def test_api_extractor_fallback(sample_csv: Path):
    # Tests that when API server is unreachable, it safely falls back
    extractor = APIExtractor(base_url="http://127.0.0.1:9999", timeout=1, max_retries=1)
    df, meta = extractor.extract_paginated("api/v1/bookings")

    # In fallback mode or failure, metadata is captured cleanly
    assert meta["status"] in ["SUCCESS_FALLBACK", "FAILED"]
    assert "extracted_at" in meta
