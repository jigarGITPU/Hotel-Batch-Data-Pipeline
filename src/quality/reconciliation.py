"""
Reconciliation module for Hotel Batch Data Pipeline.
Audits end-to-end data lineage across Source -> Staging -> Quarantine -> Warehouse Fact Tables,
calculates discrepancy percentages, verifies monetary balances, and asserts < 0.1% unexplained variance.
"""

from datetime import datetime, date, timezone
from typing import Dict, Any, List, Optional
import pandas as pd

from src.utils.database import DatabaseManager, db
from src.monitoring.logger import get_logger

logger = get_logger("reconciliation")


class ReconciliationAuditor:
    """Executes daily source-to-warehouse reconciliation and discrepancy auditing."""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.db = db_manager or db

    def reconcile_dataset(
        self,
        batch_id: str,
        dataset_name: str,
        recon_date: date,
        source_count: int,
        source_monetary_total: float = 0.0
    ) -> Dict[str, Any]:
        """
        Reconcile a specific dataset for a given date across all processing stages.
        """
        stg_tbl = self.db.get_table_name("raw_staging", f"stg_{dataset_name}")
        quar_tbl = self.db.get_table_name("quarantine", "quarantine_records")
        fact_tbl = self.db.get_table_name("analytics_dw", f"fact_{dataset_name}")

        # 1. Staging count
        try:
            sql_stg = f"SELECT COUNT(*) as cnt FROM {stg_tbl} WHERE batch_id = :b_id"
            df_stg = self.db.fetch_dataframe(sql_stg, {"b_id": batch_id})
            staging_count = int(df_stg.iloc[0]["cnt"])
        except Exception:
            staging_count = source_count

        # 2. Quarantine count for this batch and dataset
        try:
            sql_quar = f"SELECT COUNT(*) as cnt FROM {quar_tbl} WHERE batch_id = :b_id AND source_dataset = :ds"
            df_quar = self.db.fetch_dataframe(sql_quar, {"b_id": batch_id, "ds": dataset_name})
            quarantine_count = int(df_quar.iloc[0]["cnt"])
        except Exception:
            quarantine_count = 0

        # 3. Warehouse count for this batch
        try:
            sql_wh = f"SELECT COUNT(*) as cnt FROM {fact_tbl} WHERE dw_batch_id = :b_id"
            df_wh = self.db.fetch_dataframe(sql_wh, {"b_id": batch_id})
            warehouse_count = int(df_wh.iloc[0]["cnt"])
        except Exception:
            warehouse_count = 0

        # 4. Warehouse monetary total (for bookings)
        warehouse_monetary_total = 0.0
        if dataset_name == "bookings":
            try:
                sql_amt = f"SELECT SUM(total_amount_inr) as total_amt FROM {fact_tbl} WHERE dw_batch_id = :b_id"
                df_amt = self.db.fetch_dataframe(sql_amt, {"b_id": batch_id})
                val = df_amt.iloc[0]["total_amt"]
                warehouse_monetary_total = float(val) if val is not None and not pd.isna(val) else 0.0
            except Exception:
                warehouse_monetary_total = 0.0

        # Calculate reconciliation lineage:
        # Expected accounting equation: Source = Loaded in Warehouse + Quarantined Rejects + Duplicate Drop
        accounted_records = warehouse_count + quarantine_count
        unexplained_diff = abs(source_count - accounted_records)
        diff_pct = round((unexplained_diff / source_count * 100.0), 4) if source_count > 0 else 0.0
        amt_diff = round(abs(source_monetary_total - warehouse_monetary_total), 2)

        # Status determination: Must achieve < 0.1% unexplained variance
        status = "PASSED" if diff_pct < 0.10 else "FAILED"

        result = {
            "reconciliation_date": recon_date.isoformat(),
            "batch_id": batch_id,
            "dataset_name": dataset_name,
            "source_count": source_count,
            "staging_count": staging_count,
            "valid_count": warehouse_count,
            "quarantine_count": quarantine_count,
            "warehouse_count": warehouse_count,
            "count_difference": unexplained_diff,
            "difference_percentage": diff_pct,
            "source_total_amount_inr": round(source_monetary_total, 2),
            "warehouse_total_amount_inr": round(warehouse_monetary_total, 2),
            "amount_difference_inr": amt_diff,
            "status": status,
            "generated_at": datetime.now(timezone.utc).isoformat()
        }

        # Persist into metadata_audit.daily_reconciliation
        self._save_reconciliation(result)
        logger.info(
            f"Reconciliation for {dataset_name} ({recon_date}): Source={source_count}, "
            f"WH={warehouse_count}, Quarantine={quarantine_count}, "
            f"Unexplained Diff={unexplained_diff} ({diff_pct}%) -> {status}"
        )
        return result

    def _save_reconciliation(self, r: Dict[str, Any]):
        """Upsert reconciliation result into metadata_audit.daily_reconciliation."""
        tbl = self.db.get_table_name("metadata_audit", "daily_reconciliation")

        if self.db.is_sqlite:
            sql = f"""
                INSERT INTO {tbl} (
                    reconciliation_date, batch_id, dataset_name, source_count, staging_count,
                    valid_count, quarantine_count, warehouse_count, count_difference,
                    difference_percentage, source_total_amount_inr, warehouse_total_amount_inr,
                    amount_difference_inr, status, generated_at
                ) VALUES (
                    :reconciliation_date, :batch_id, :dataset_name, :source_count, :staging_count,
                    :valid_count, :quarantine_count, :warehouse_count, :count_difference,
                    :difference_percentage, :source_total_amount_inr, :warehouse_total_amount_inr,
                    :amount_difference_inr, :status, :generated_at
                )
                ON CONFLICT(reconciliation_date, dataset_name) DO UPDATE SET
                    batch_id = excluded.batch_id,
                    source_count = excluded.source_count,
                    staging_count = excluded.staging_count,
                    valid_count = excluded.valid_count,
                    quarantine_count = excluded.quarantine_count,
                    warehouse_count = excluded.warehouse_count,
                    count_difference = excluded.count_difference,
                    difference_percentage = excluded.difference_percentage,
                    source_total_amount_inr = excluded.source_total_amount_inr,
                    warehouse_total_amount_inr = excluded.warehouse_total_amount_inr,
                    amount_difference_inr = excluded.amount_difference_inr,
                    status = excluded.status,
                    generated_at = excluded.generated_at;
            """
        else:
            sql = f"""
                INSERT INTO {tbl} (
                    reconciliation_date, batch_id, dataset_name, source_count, staging_count,
                    valid_count, quarantine_count, warehouse_count, count_difference,
                    difference_percentage, source_total_amount_inr, warehouse_total_amount_inr,
                    amount_difference_inr, status, generated_at
                ) VALUES (
                    :reconciliation_date, :batch_id, :dataset_name, :source_count, :staging_count,
                    :valid_count, :quarantine_count, :warehouse_count, :count_difference,
                    :difference_percentage, :source_total_amount_inr, :warehouse_total_amount_inr,
                    :amount_difference_inr, :status, :generated_at
                )
                ON CONFLICT (reconciliation_date, dataset_name) DO UPDATE SET
                    batch_id = EXCLUDED.batch_id,
                    source_count = EXCLUDED.source_count,
                    staging_count = EXCLUDED.staging_count,
                    valid_count = EXCLUDED.valid_count,
                    quarantine_count = EXCLUDED.quarantine_count,
                    warehouse_count = EXCLUDED.warehouse_count,
                    count_difference = EXCLUDED.count_difference,
                    difference_percentage = EXCLUDED.difference_percentage,
                    source_total_amount_inr = EXCLUDED.source_total_amount_inr,
                    warehouse_total_amount_inr = EXCLUDED.warehouse_total_amount_inr,
                    amount_difference_inr = EXCLUDED.amount_difference_inr,
                    status = EXCLUDED.status,
                    generated_at = EXCLUDED.generated_at;
            """
        self.db.execute_sql(sql, r)
