"""
Quarantine Management module for Hotel Batch Data Pipeline.
Captures and persists rejected records with granular reason codes, payload, and audit metadata.
"""

import json
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import pandas as pd

from src.utils.database import DatabaseManager, db
from src.monitoring.logger import get_logger

logger = get_logger("quarantine_manager")


class QuarantineManager:
    """Manages recording, querying, and auditing of quarantined records."""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.db = db_manager or db

    def record_quarantine(
        self,
        batch_id: str,
        source_dataset: str,
        invalid_records: List[Dict[str, Any]]
    ) -> int:
        """
        Persist a list of quarantined records into quarantine.quarantine_records.
        Each entry in invalid_records must contain:
          - record_identifier (e.g. booking_id or row index)
          - reason_code (e.g. NULL_BOOKING_ID, INVALID_PRICE)
          - error_message
          - raw_payload (dict or json string)
        """
        if not invalid_records:
            return 0

        now_iso = datetime.now(timezone.utc).isoformat()
        rows_to_insert = []

        for item in invalid_records:
            payload = item.get("raw_payload")
            if isinstance(payload, dict):
                payload_str = json.dumps(payload, default=str)
            else:
                payload_str = str(payload or "")

            rows_to_insert.append({
                "batch_id": batch_id,
                "source_dataset": source_dataset,
                "record_identifier": str(item.get("record_identifier", "")),
                "reason_code": str(item.get("reason_code", "UNKNOWN_ERROR")),
                "error_message": str(item.get("error_message", "")),
                "raw_payload": payload_str,
                "detected_at": now_iso
            })

        df_quarantine = pd.DataFrame(rows_to_insert)
        count = self.db.bulk_insert(
            df=df_quarantine,
            schema="quarantine",
            table="quarantine_records",
            if_exists="append",
            chunksize=5000
        )
        logger.warning(f"Quarantined {count} invalid records for {source_dataset} in batch {batch_id}")
        return count

    def get_quarantine_summary(self, batch_id: Optional[str] = None) -> pd.DataFrame:
        """Query quarantine table aggregated by dataset and reason code."""
        tbl = self.db.get_table_name("quarantine", "quarantine_records")
        sql = f"""
            SELECT source_dataset, reason_code, COUNT(*) as failure_count
            FROM {tbl}
            {"WHERE batch_id = :batch_id" if batch_id else ""}
            GROUP BY source_dataset, reason_code
            ORDER BY failure_count DESC
        """
        params = {"batch_id": batch_id} if batch_id else {}
        return self.db.fetch_dataframe(sql, params)
