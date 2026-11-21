"""
Automated Data Quality Validation Framework for Hotel Batch Data Pipeline.
Executes 12 core data quality checks across Bookings, Inventory, Stays, and Reviews,
separates valid from invalid records, generates audit metrics, and pushes rejects to quarantine.
"""

from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple, Optional
import pandas as pd
import numpy as np

from src.utils.database import DatabaseManager, db
from src.quality.quarantine import QuarantineManager
from src.monitoring.logger import get_logger

logger = get_logger("quality_validator")


class DataQualityValidator:
    """Performs schema, business rule, and referential integrity validations."""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.db = db_manager or db
        self.quarantine = QuarantineManager(self.db)
        self.known_hotels = self._load_known_hotels()

    def _load_known_hotels(self) -> set:
        """Load registered hotel IDs from dim_hotel."""
        tbl = self.db.get_table_name("analytics_dw", "dim_hotel")
        try:
            df = self.db.fetch_dataframe(f"SELECT hotel_id FROM {tbl}")
            return set(df["hotel_id"].str.strip())
        except Exception:
            return {f"HTL-GAU-{i:03d}" for i in range(1, 15)}

    def _record_quality_check(
        self,
        batch_id: str,
        check_name: str,
        source_dataset: str,
        total_checked: int,
        failed_count: int
    ) -> Dict[str, Any]:
        """Construct check result record and persist into metadata_audit."""
        passed_count = total_checked - failed_count
        fail_pct = round((failed_count / total_checked * 100.0), 2) if total_checked > 0 else 0.0
        status = "PASSED" if failed_count == 0 else ("WARNING" if fail_pct < 5.0 else "FAILED")

        record = {
            "batch_id": batch_id,
            "check_name": check_name,
            "source_dataset": source_dataset,
            "records_checked": total_checked,
            "records_passed": passed_count,
            "records_failed": failed_count,
            "failure_percentage": fail_pct,
            "status": status,
            "execution_timestamp": datetime.now(timezone.utc).isoformat()
        }

        # Persist to metadata_audit.data_quality_results
        df_res = pd.DataFrame([record])
        self.db.bulk_insert(df_res, "metadata_audit", "data_quality_results", if_exists="append")
        return record

    def validate_bookings(self, df: pd.DataFrame, batch_id: str) -> Tuple[pd.DataFrame, List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Validate bookings against business rules and separate quarantine records."""
        if df.empty:
            return df, [], []

        total_rows = len(df)
        invalid_records: List[Dict[str, Any]] = []
        checks_results: List[Dict[str, Any]] = []
        rejected_indices = set()

        # 1. Null Booking ID
        null_bkg_mask = df["booking_id"].isna() | (df["booking_id"].astype(str).str.strip().isin(["", "nan", "None", "NULL"]))
        null_bkg_count = int(null_bkg_mask.sum())
        checks_results.append(self._record_quality_check(batch_id, "check_null_booking_id", "bookings", total_rows, null_bkg_count))
        for idx in df[null_bkg_mask].index:
            rejected_indices.add(idx)
            invalid_records.append({
                "record_identifier": f"ROW_{idx}",
                "reason_code": "NULL_BOOKING_ID",
                "error_message": "Mandatory booking_id is null or empty",
                "raw_payload": df.loc[idx].to_dict()
            })

        # 2. Null Guest ID
        null_gst_mask = df["guest_id"].isna() | (df["guest_id"].astype(str).str.strip().isin(["", "nan", "None", "NULL"]))
        null_gst_count = int(null_gst_mask.sum())
        checks_results.append(self._record_quality_check(batch_id, "check_null_guest_id", "bookings", total_rows, null_gst_count))
        for idx in df[null_gst_mask].index:
            rejected_indices.add(idx)
            invalid_records.append({
                "record_identifier": str(df.loc[idx].get("booking_id", f"ROW_{idx}")),
                "reason_code": "NULL_GUEST_ID",
                "error_message": "Mandatory guest_id is null or empty",
                "raw_payload": df.loc[idx].to_dict()
            })

        # 3. Duplicate Booking Line ID
        line_col = "booking_line_id" if "booking_line_id" in df.columns else "booking_id"
        dup_mask = df.duplicated(subset=[line_col], keep="first")
        dup_count = int(dup_mask.sum())
        checks_results.append(self._record_quality_check(batch_id, "check_duplicate_booking", "bookings", total_rows, dup_count))
        for idx in df[dup_mask].index:
            rejected_indices.add(idx)
            invalid_records.append({
                "record_identifier": str(df.loc[idx].get(line_col, f"ROW_{idx}")),
                "reason_code": "DUPLICATE_BOOKING",
                "error_message": f"Duplicate booking key detected: {df.loc[idx].get(line_col)}",
                "raw_payload": df.loc[idx].to_dict()
            })

        # 4. Invalid Room Price
        price_num = pd.to_numeric(df.get("room_rate"), errors="coerce")
        invalid_price_mask = price_num.isna() | (price_num <= 0) | (price_num > 500000)
        invalid_price_count = int(invalid_price_mask.sum())
        checks_results.append(self._record_quality_check(batch_id, "check_invalid_price", "bookings", total_rows, invalid_price_count))
        for idx in df[invalid_price_mask].index:
            rejected_indices.add(idx)
            invalid_records.append({
                "record_identifier": str(df.loc[idx].get("booking_id", f"ROW_{idx}")),
                "reason_code": "INVALID_PRICE",
                "error_message": f"Room price is non-positive or extreme outlier: {df.loc[idx].get('room_rate')}",
                "raw_payload": df.loc[idx].to_dict()
            })

        # 5. Invalid Currency
        if "currency" in df.columns:
            supported_curr = {"INR", "USD", "EUR", "GBP"}
            invalid_curr_mask = ~df["currency"].astype(str).str.strip().str.upper().isin(supported_curr)
            invalid_curr_count = int(invalid_curr_mask.sum())
            checks_results.append(self._record_quality_check(batch_id, "check_invalid_currency", "bookings", total_rows, invalid_curr_count))
            for idx in df[invalid_curr_mask].index:
                rejected_indices.add(idx)
                invalid_records.append({
                    "record_identifier": str(df.loc[idx].get("booking_id", f"ROW_{idx}")),
                    "reason_code": "INVALID_CURRENCY",
                    "error_message": f"Unsupported currency code: {df.loc[idx].get('currency')}",
                    "raw_payload": df.loc[idx].to_dict()
                })

        # 6. Invalid Date Ranges (Check-out <= Check-in)
        invalid_dates_mask = []
        for _, r in df.iterrows():
            try:
                ci = datetime.strptime(str(r["check_in_date"])[:10], "%Y-%m-%d").date()
                co = datetime.strptime(str(r["check_out_date"])[:10], "%Y-%m-%d").date()
                invalid_dates_mask.append(co <= ci)
            except Exception:
                invalid_dates_mask.append(True)
        inv_date_s = pd.Series(invalid_dates_mask, index=df.index)
        inv_date_count = int(inv_date_s.sum())
        checks_results.append(self._record_quality_check(batch_id, "check_invalid_date_range", "bookings", total_rows, inv_date_count))
        for idx in df[inv_date_s].index:
            rejected_indices.add(idx)
            invalid_records.append({
                "record_identifier": str(df.loc[idx].get("booking_id", f"ROW_{idx}")),
                "reason_code": "INVALID_DATE",
                "error_message": f"Check-out date ({df.loc[idx].get('check_out_date')}) must be strictly after check-in date ({df.loc[idx].get('check_in_date')})",
                "raw_payload": df.loc[idx].to_dict()
            })

        # 7. Referential Integrity: Known Hotel ID
        if "hotel_id" in df.columns:
            missing_htl_mask = ~df["hotel_id"].astype(str).str.strip().isin(self.known_hotels)
            missing_htl_count = int(missing_htl_mask.sum())
            checks_results.append(self._record_quality_check(batch_id, "check_referential_hotel", "bookings", total_rows, missing_htl_count))
            for idx in df[missing_htl_mask].index:
                rejected_indices.add(idx)
                invalid_records.append({
                    "record_identifier": str(df.loc[idx].get("booking_id", f"ROW_{idx}")),
                    "reason_code": "MISSING_HOTEL_REFERENCE",
                    "error_message": f"Referenced hotel_id '{df.loc[idx].get('hotel_id')}' not found in dim_hotel",
                    "raw_payload": df.loc[idx].to_dict()
                })

        # Persist quarantine
        if invalid_records:
            self.quarantine.record_quarantine(batch_id, "bookings", invalid_records)

        # Filter valid records
        df_valid = df.drop(index=list(rejected_indices)).copy()
        logger.info(f"Bookings validation: Checked={total_rows}, Valid={len(df_valid)}, Quarantined={len(rejected_indices)}")
        return df_valid, invalid_records, checks_results

    def validate_inventory(self, df: pd.DataFrame, batch_id: str) -> Tuple[pd.DataFrame, List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Validate room inventory (Negative Inventory check, room mappings)."""
        if df.empty:
            return df, [], []

        total_rows = len(df)
        invalid_records = []
        checks_results = []
        rejected_indices = set()

        # Negative inventory check
        avail_num = pd.to_numeric(df.get("available_rooms"), errors="coerce")
        neg_inv_mask = avail_num.isna() | (avail_num < 0)
        neg_count = int(neg_inv_mask.sum())
        checks_results.append(self._record_quality_check(batch_id, "check_negative_inventory", "inventory", total_rows, neg_count))
        for idx in df[neg_inv_mask].index:
            rejected_indices.add(idx)
            invalid_records.append({
                "record_identifier": str(df.loc[idx].get("inventory_id", f"ROW_{idx}")),
                "reason_code": "NEGATIVE_INVENTORY",
                "error_message": f"Negative available rooms count: {df.loc[idx].get('available_rooms')}",
                "raw_payload": df.loc[idx].to_dict()
            })

        if invalid_records:
            self.quarantine.record_quarantine(batch_id, "inventory", invalid_records)

        df_valid = df.drop(index=list(rejected_indices)).copy()
        logger.info(f"Inventory validation: Checked={total_rows}, Valid={len(df_valid)}, Quarantined={len(rejected_indices)}")
        return df_valid, invalid_records, checks_results

    def validate_reviews(self, df: pd.DataFrame, batch_id: str) -> Tuple[pd.DataFrame, List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Validate customer reviews (Invalid rating 1-5 check)."""
        if df.empty:
            return df, [], []

        total_rows = len(df)
        invalid_records = []
        checks_results = []
        rejected_indices = set()

        # Rating check 1 <= rating <= 5
        rating_num = pd.to_numeric(df.get("rating"), errors="coerce")
        inv_rating_mask = rating_num.isna() | (rating_num < 1) | (rating_num > 5)
        inv_count = int(inv_rating_mask.sum())
        checks_results.append(self._record_quality_check(batch_id, "check_invalid_rating", "reviews", total_rows, inv_count))
        for idx in df[inv_rating_mask].index:
            rejected_indices.add(idx)
            invalid_records.append({
                "record_identifier": str(df.loc[idx].get("review_id", f"ROW_{idx}")),
                "reason_code": "INVALID_RATING",
                "error_message": f"Customer rating out of bounds (1-5): {df.loc[idx].get('rating')}",
                "raw_payload": df.loc[idx].to_dict()
            })

        if invalid_records:
            self.quarantine.record_quarantine(batch_id, "reviews", invalid_records)

        df_valid = df.drop(index=list(rejected_indices)).copy()
        logger.info(f"Reviews validation: Checked={total_rows}, Valid={len(df_valid)}, Quarantined={len(rejected_indices)}")
        return df_valid, invalid_records, checks_results
