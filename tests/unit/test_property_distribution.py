"""
Unit tests for property distribution across 14 Guwahati hotel properties.
Verifies that generated datasets cover all 14 hotels with balanced distribution.
"""

import pytest
import pandas as pd
from pathlib import Path
from src.utils.config import config


def test_14_properties_in_generated_bookings():
    """Verify generated bookings reference all 14 distinct hotel IDs."""
    bkg_file = config.PIPELINE.generated_data_dir / "hotel_bookings.csv"
    assert bkg_file.exists(), "hotel_bookings.csv does not exist"

    df = pd.read_csv(bkg_file, usecols=["hotel_id"])
    unique_hotels = df["hotel_id"].unique()
    assert len(unique_hotels) == 14

    # Verify every hotel has at least 1,000 bookings
    counts = df["hotel_id"].value_counts()
    for htl_id, cnt in counts.items():
        assert cnt >= 1000, f"Hotel {htl_id} has fewer than 1000 bookings: {cnt}"


def test_14_properties_in_reference_data():
    """Verify reference file contains exactly 14 properties located in Guwahati."""
    ref_file = config.PIPELINE.generated_data_dir / "hotels_reference.csv"
    assert ref_file.exists(), "hotels_reference.csv does not exist"

    df = pd.read_csv(ref_file)
    assert len(df) == 14
    for _, row in df.iterrows():
        assert "Guwahati" in row["location"]
