"""
CLI Script to inspect and output data quality audit results and quarantine status.
"""

import sys
from pathlib import Path
import pandas as pd

# Ensure project root in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.database import db
from src.quality.quarantine import QuarantineManager


def display_quality_report():
    print("\n" + "=" * 70)
    print("               DATA QUALITY & AUDIT REPORT                 ")
    print("=" * 70)

    # 1. Latest Quality Checks
    tbl_checks = db.get_table_name("metadata_audit", "data_quality_results")
    try:
        sql = f"""
            SELECT check_name, source_dataset, records_checked, records_passed,
                   records_failed, failure_percentage, status, execution_timestamp
            FROM {tbl_checks}
            ORDER BY check_id DESC
            LIMIT 20
        """
        df_checks = db.fetch_dataframe(sql)
        print("\n[LATEST 20 AUTOMATED DATA QUALITY CHECKS]:")
        if df_checks.empty:
            print("  No quality check records found yet.")
        else:
            print(df_checks.to_string(index=False))
    except Exception as e:
        print(f"  Error reading quality checks: {e}")

    # 2. Quarantine Summary
    print("\n[QUARANTINE SUMMARY BY REASON CODE]:")
    qm = QuarantineManager(db)
    df_quar = qm.get_quarantine_summary()
    if df_quar.empty:
        print("  Quarantine is empty. (0 records)")
    else:
        print(df_quar.to_string(index=False))

    # 3. Latest Reconciliation
    tbl_recon = db.get_table_name("metadata_audit", "daily_reconciliation")
    try:
        sql_recon = f"""
            SELECT reconciliation_date, dataset_name, source_count, warehouse_count,
                   quarantine_count, count_difference, difference_percentage, status
            FROM {tbl_recon}
            ORDER BY reconciliation_id DESC
            LIMIT 10
        """
        df_recon = db.fetch_dataframe(sql_recon)
        print("\n[LATEST RECONCILIATIONS]:")
        if df_recon.empty:
            print("  No reconciliation records found yet.")
        else:
            print(df_recon.to_string(index=False))
    except Exception as e:
        print(f"  Error reading reconciliations: {e}")

    print("=" * 70 + "\n")


if __name__ == "__main__":
    display_quality_report()
