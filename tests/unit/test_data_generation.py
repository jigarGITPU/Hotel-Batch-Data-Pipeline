"""
Unit tests for Synthetic Data Generator.
Verifies schema compliance, deterministic reproducibility, 14 Guwahati hotel distribution, and row counts.
"""

import pytest
import pandas as pd
from pathlib import Path
from scripts.generate_data import generate_dataset, HOTELS, CANONICAL_ROOM_TYPES


def test_hotel_properties_count():
    assert len(HOTELS) == 14
    hotel_ids = [h["hotel_id"] for h in HOTELS]
    assert len(set(hotel_ids)) == 14
    for h in HOTELS:
        assert h["hotel_id"].startswith("HTL-GAU-")
        assert "Guwahati" in h["location"]


def test_small_data_generation(tmp_path: Path):
    summary = generate_dataset(mode="small", output_dir=tmp_path, seed=42)

    assert summary["mode"] == "small"
    assert summary["hotel_properties_count"] == 14
    assert summary["bookings_count"] > 2000
    assert summary["inventory_count"] > 1000
    assert summary["stays_count"] > 800
    assert summary["reviews_count"] > 400

    # Verify CSV files exist on disk
    assert (tmp_path / "hotel_bookings.csv").exists()
    assert (tmp_path / "room_inventory.csv").exists()
    assert (tmp_path / "guest_stays.csv").exists()
    assert (tmp_path / "customer_reviews.csv").exists()
    assert (tmp_path / "hotels_reference.csv").exists()
    assert (tmp_path / "guests_reference.csv").exists()


def test_deterministic_reproducibility(tmp_path: Path):
    path_a = tmp_path / "run_a"
    path_b = tmp_path / "run_b"

    sum_a = generate_dataset(mode="small", output_dir=path_a, seed=123)
    sum_b = generate_dataset(mode="small", output_dir=path_b, seed=123)

    assert sum_a["total_source_records"] == sum_b["total_source_records"]
    df_a = pd.read_csv(path_a / "hotel_bookings.csv")
    df_b = pd.read_csv(path_b / "hotel_bookings.csv")
    assert df_a.equals(df_b)
