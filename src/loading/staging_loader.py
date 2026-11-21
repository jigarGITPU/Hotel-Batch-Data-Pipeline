"""
Staging layer loader for Hotel Batch Data Pipeline.
Loads extracted raw datasets into staging tables with audit metadata
(batch_id, source_system, source_file, extracted_at, staged_at).
"""

from datetime import datetime, timezone
from typing import Dict, Any, Optional
import pandas as pd

from src.utils.database import DatabaseManager, db
from src.monitoring.logger import get_logger

logger = get_logger("staging_loader")


class StagingLoader:
    """Manages loading of raw data into staging tables with tracking metadata."""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.db = db_manager or db

    def load_staging_table(
        self,
        df: pd.DataFrame,
        dataset_name: str,
        batch_id: str
    ) -> Dict[str, Any]:
        """
        Stage DataFrame records into the appropriate raw_staging table.
        """
        if df.empty:
            logger.warning(f"No records to stage for dataset: {dataset_name}")
            return {
                "dataset": dataset_name,
                "batch_id": batch_id,
                "records_staged": 0,
                "status": "EMPTY"
            }

        table_map = {
            "bookings": "stg_bookings",
            "inventory": "stg_inventory",
            "stays": "stg_stays",
            "reviews": "stg_reviews"
        }

        table_name = table_map.get(dataset_name)
        if not table_name:
            raise ValueError(f"Unknown dataset for staging: {dataset_name}")

        df_staged = df.copy()
        df_staged["batch_id"] = batch_id
        if "staged_at" not in df_staged.columns:
            df_staged["staged_at"] = datetime.now(timezone.utc).isoformat()

        # Write to staging
        count = self.db.bulk_insert(
            df=df_staged,
            schema="raw_staging",
            table=table_name,
            if_exists="append",
            chunksize=5000
        )

        logger.info(f"Staged {count} records into raw_staging.{table_name} for batch {batch_id}")
        return {
            "dataset": dataset_name,
            "table": table_name,
            "batch_id": batch_id,
            "records_staged": count,
            "status": "SUCCESS"
        }

    def clear_batch_staging(self, batch_id: str):
        """Clean staging records associated with a specific batch (optional cleanup)."""
        tables = ["stg_bookings", "stg_inventory", "stg_stays", "stg_reviews"]
        for tbl in tables:
            full_tbl = self.db.get_table_name("raw_staging", tbl)
            sql = f"DELETE FROM {full_tbl} WHERE batch_id = :batch_id"
            self.db.execute_sql(sql, {"batch_id": batch_id})
        logger.info(f"Cleared staging records for batch {batch_id}")
