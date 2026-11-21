"""
Unit tests for pipeline failure recovery and watermark isolation.
Verifies that failed runs do not advance watermarks and error status is safely recorded.
"""

import pytest
from datetime import datetime, timezone
from src.loading.warehouse_loader import WarehouseLoader
from src.orchestration.pipeline_runner import HotelPipelineRunner
from src.utils.database import db
from unittest.mock import patch


def test_watermark_isolation_on_pipeline_failure():
    """Verify that when a stage fails, watermark advancement is not reached."""
    loader = WarehouseLoader(db)
    source_name = "hotel_bookings"

    # 1. Capture current watermark
    initial_wm = loader.get_watermark(source_name)
    assert initial_wm is not None

    # 2. Mock a failure during transformation stage
    runner = HotelPipelineRunner(db)
    with patch.object(runner.validator, "validate_bookings", side_effect=RuntimeError("Simulated Quality Failure")):
        result = runner.run(incremental=True)

    # Verify run failed
    assert result["status"] == "FAILED"
    assert "Simulated Quality Failure" in result["error_message"]

    # 3. Verify watermark was NOT changed by the failed run
    post_failure_wm = loader.get_watermark(source_name)
    assert post_failure_wm == initial_wm
