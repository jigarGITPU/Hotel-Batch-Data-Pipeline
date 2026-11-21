"""
Integration tests for Hotel Batch Data Pipeline.
Validates complete end-to-end execution, dimension and fact table loading, and daily reconciliation.
"""

import pytest
import pandas as pd
from src.orchestration.pipeline_runner import HotelPipelineRunner
from src.utils.database import db


def test_full_pipeline_execution():
    runner = HotelPipelineRunner(db)
    result = runner.run(incremental=False)

    # 1. Pipeline status
    assert result["status"] == "SUCCESS"
    assert result["records_extracted"] > 0
    assert result["records_loaded"] > 0
    assert result["duration_seconds"] > 0.0

    # 2. Check dimension and fact counts in warehouse
    tbl_htl = db.get_table_name("analytics_dw", "dim_hotel")
    df_htl = db.fetch_dataframe(f"SELECT COUNT(*) as cnt FROM {tbl_htl}")
    assert int(df_htl.iloc[0]["cnt"]) == 14

    tbl_bkg = db.get_table_name("analytics_dw", "fact_bookings")
    df_bkg = db.fetch_dataframe(f"SELECT COUNT(*) as cnt FROM {tbl_bkg}")
    assert int(df_bkg.iloc[0]["cnt"]) > 0

    tbl_sty = db.get_table_name("analytics_dw", "fact_stays")
    df_sty = db.fetch_dataframe(f"SELECT COUNT(*) as cnt FROM {tbl_sty}")
    assert int(df_sty.iloc[0]["cnt"]) > 0

    tbl_rev = db.get_table_name("analytics_dw", "fact_reviews")
    df_rev = db.fetch_dataframe(f"SELECT COUNT(*) as cnt FROM {tbl_rev}")
    assert int(df_rev.iloc[0]["cnt"]) > 0

    # 3. Check reconciliation table
    tbl_recon = db.get_table_name("metadata_audit", "daily_reconciliation")
    df_recon = db.fetch_dataframe(f"SELECT difference_percentage, status FROM {tbl_recon} ORDER BY reconciliation_id DESC LIMIT 1")
    assert not df_recon.empty
    assert df_recon.iloc[0]["status"] == "PASSED"
    assert float(df_recon.iloc[0]["difference_percentage"]) < 0.10
