"""
CLI Script to execute the Hotel Batch Data Pipeline.
Supports manual triggers, single-date runs, incremental modes, and dataset reloading.
"""

import argparse
import sys
from pathlib import Path

# Ensure project root in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.orchestration.pipeline_runner import HotelPipelineRunner
from src.monitoring.logger import get_logger

logger = get_logger("cli_run_pipeline")


def main():
    parser = argparse.ArgumentParser(description="Run Hotel Batch Data Pipeline.")
    parser.add_argument("--date", type=str, default=None, help="Target business date (YYYY-MM-DD)")
    parser.add_argument("--full", action="store_true", help="Execute full reload (disable incremental watermarking)")
    parser.add_argument("--run-id", type=str, default=None, help="Custom run identifier")
    parser.add_argument("--batch-id", type=str, default=None, help="Custom batch identifier")

    args = parser.parse_args()

    runner = HotelPipelineRunner()
    result = runner.run(
        target_date=args.date,
        run_id=args.run_id,
        batch_id=args.batch_id,
        incremental=not args.full
    )

    print("\n" + "=" * 50)
    print("        HOTEL PIPELINE EXECUTION SUMMARY        ")
    print("=" * 50)
    for k, v in result.items():
        print(f"  {k:22}: {v}")
    print("=" * 50)

    if result["status"] != "SUCCESS":
        sys.exit(1)


if __name__ == "__main__":
    main()
