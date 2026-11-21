"""
CSV Extraction module for Hotel Batch Data Pipeline.
Extracts hotel data from daily or batch CSV files, validates schemas,
attaches audit metadata (source_file, extracted_at), and handles missing/malformed files.
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import pandas as pd

from src.utils.config import config
from src.monitoring.logger import get_logger

logger = get_logger("csv_extractor")

EXPECTED_SCHEMAS = {
    "bookings": [
        "booking_id", "booking_line_id", "guest_id", "hotel_id", "room_id",
        "booking_date", "check_in_date", "check_out_date", "booking_status",
        "room_rate", "currency", "number_of_guests", "source_updated_at"
    ],
    "inventory": [
        "inventory_id", "hotel_id", "room_id", "room_type", "inventory_date",
        "available_rooms", "total_rooms", "rate", "currency", "source_updated_at"
    ],
    "stays": [
        "stay_id", "booking_id", "guest_id", "hotel_id", "room_id",
        "actual_check_in", "actual_check_out", "stay_status"
    ],
    "reviews": [
        "review_id", "booking_id", "guest_id", "hotel_id", "rating",
        "review_text", "review_date"
    ],
    "hotels": [
        "hotel_id", "name", "location", "rating_category", "total_rooms"
    ],
    "guests": [
        "guest_id", "first_name", "last_name", "email", "phone", "city", "state", "country"
    ]
}


class CSVExtractor:
    """Handles discovery, structural validation, and reading of source CSV files."""

    def __init__(self, base_dir: Optional[Path] = None):
        self.base_dir = base_dir or config.PIPELINE.generated_data_dir

    def extract_dataset(
        self,
        dataset_name: str,
        filename: Optional[str] = None,
        date_filter_col: Optional[str] = None,
        target_date: Optional[str] = None
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Extract a specific dataset from CSV with schema validation and metadata tracking.

        Returns:
            Tuple of (DataFrame with audit columns, extraction metadata dictionary)
        """
        if filename is None:
            default_map = {
                "bookings": "hotel_bookings.csv",
                "inventory": "room_inventory.csv",
                "stays": "guest_stays.csv",
                "reviews": "customer_reviews.csv",
                "hotels": "hotels_reference.csv",
                "guests": "guests_reference.csv"
            }
            filename = default_map.get(dataset_name, f"{dataset_name}.csv")

        file_path = self.base_dir / filename
        start_ts = datetime.now(timezone.utc)
        extraction_time_iso = start_ts.isoformat()

        metadata: Dict[str, Any] = {
            "dataset": dataset_name,
            "source_type": "CSV",
            "file_path": str(file_path),
            "file_name": file_path.name,
            "extracted_at": extraction_time_iso,
            "status": "FAILED",
            "rows_extracted": 0,
            "file_size_bytes": 0,
            "validation_errors": []
        }

        if not file_path.exists():
            err_msg = f"File not found: {file_path}"
            logger.error(err_msg)
            metadata["validation_errors"].append(err_msg)
            return pd.DataFrame(), metadata

        try:
            metadata["file_size_bytes"] = file_path.stat().st_size
            df = pd.read_csv(file_path, dtype=str, keep_default_na=False)

            # Validate basic column presence
            expected_cols = EXPECTED_SCHEMAS.get(dataset_name, [])
            missing_cols = [c for c in expected_cols if c not in df.columns]
            if missing_cols:
                err_msg = f"Missing expected columns in {file_path.name}: {missing_cols}"
                logger.warning(err_msg)
                metadata["validation_errors"].append(err_msg)

            # Apply date filtering if specified
            if target_date and date_filter_col and date_filter_col in df.columns:
                df = df[df[date_filter_col].str.startswith(target_date)].copy()

            # Attach standard audit columns
            df["source_file"] = file_path.name
            df["extracted_at"] = extraction_time_iso
            df["source_system"] = "CSV_PMS"

            metadata["rows_extracted"] = len(df)
            metadata["status"] = "SUCCESS"
            duration = (datetime.now(timezone.utc) - start_ts).total_seconds()
            metadata["duration_seconds"] = duration

            logger.info(
                f"Extracted {len(df)} rows from {file_path.name} "
                f"({metadata['file_size_bytes']} bytes in {duration:.3f}s)"
            )
            return df, metadata

        except Exception as e:
            err_msg = f"Error extracting {file_path.name}: {str(e)}"
            logger.error(err_msg, exc_info=True)
            metadata["validation_errors"].append(err_msg)
            return pd.DataFrame(), metadata
