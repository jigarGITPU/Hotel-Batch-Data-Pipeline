"""
Historical Backfill Script for Hotel Batch Data Pipeline.
Iterates day-by-day through a date range, running incremental loads with idempotency guarantees.
"""

import argparse
import sys
from datetime import datetime, date, timedelta
from pathlib import Path

# Ensure project root in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.orchestration.pipeline_runner import HotelPipelineRunner
from src.monitoring.logger import get_logger

logger = get_logger("backfill_runner")


def run_backfill(start_date: str, end_date: str):
    """Execute day-by-day backfill for specified date range."""
    d_start = datetime.strptime(start_date, "%Y-%m-%d").date()
    d_end = datetime.strptime(end_date, "%Y-%m-%d").date()

    if d_start > d_end:
        logger.error(f"Invalid range: Start date {start_date} is after end date {end_date}")
        sys.exit(1)

    total_days = (d_end - d_start).days + 1
    logger.info(f"=== Starting Historical Backfill: {start_date} to {end_date} ({total_days} days) ===")

    runner = HotelPipelineRunner()
    current = d_start
    success_days = 0
    failed_days = 0

    while current <= d_end:
        day_str = current.isoformat()
        logger.info(f"\n>>> Processing Backfill Day: {day_str} <<<")
        res = runner.run(target_date=day_str, incremental=False)

        if res["status"] == "SUCCESS":
            success_days += 1
        else:
            failed_days += 1
            logger.error(f"Backfill failed on {day_str}: {res.get('error_message')}")

        current += timedelta(days=1)

    print("\n" + "=" * 55)
    print("           HISTORICAL BACKFILL REPORT              ")
    print("=" * 55)
    print(f"  Range            : {start_date} to {end_date}")
    print(f"  Total Days       : {total_days}")
    print(f"  Successful Days  : {success_days}")
    print(f"  Failed Days      : {failed_days}")
    print("=" * 55)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run historical backfill.")
    parser.add_argument("--start-date", type=str, required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, required=True, help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    run_backfill(args.start_date, args.end_date)
