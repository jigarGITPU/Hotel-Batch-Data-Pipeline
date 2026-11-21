"""
Apache Airflow DAG: Hotel Batch Data Pipeline.
Orchestrates the daily incremental batch pipeline for Guwahati hotel properties.
Includes extraction, staging, quality gates, quarantine, transformations,
dimensional fact loading (UPSERT), daily reconciliation, and alerting callbacks.
"""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Ensure project root is available to Airflow workers
AIRFLOW_DAGS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = AIRFLOW_DAGS_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    try:
        from airflow.operators.empty import EmptyOperator
    except ImportError:
        from airflow.operators.dummy import DummyOperator as EmptyOperator
    AIRFLOW_INSTALLED = True
except ImportError:
    AIRFLOW_INSTALLED = False

# Import pipeline components
from src.utils.config import config
from src.monitoring.logger import get_logger, generate_batch_id, generate_run_id
from src.monitoring.notifications import notification_service
from src.orchestration.pipeline_runner import HotelPipelineRunner

logger = get_logger("airflow_dag")


def on_failure_callback(context):
    """Callback triggered whenever an Airflow task fails."""
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id if task_instance else "unknown_task"
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown_dag"
    exec_date = context.get("execution_date", datetime.now(timezone.utc)).isoformat()
    exception = context.get("exception", "No exception info provided")

    logger.error(f"[AIRFLOW FAILURE] Task {task_id} in {dag_id} failed on {exec_date}: {exception}")
    notification_service.send_alert(
        alert_type="AIRFLOW_TASK_FAILURE",
        title=f"Airflow Task Failed: {task_id}",
        details={
            "dag_id": dag_id,
            "task_id": task_id,
            "execution_date": exec_date,
            "exception": str(exception)
        },
        severity="CRITICAL"
    )


def on_success_callback(context):
    """Callback triggered on successful DAG completion."""
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown_dag"
    exec_date = context.get("execution_date", datetime.now(timezone.utc)).isoformat()
    logger.info(f"[AIRFLOW SUCCESS] DAG {dag_id} completed successfully for date {exec_date}")


# Default DAG Arguments
default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "email": [config.PIPELINE.data_dir],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": on_failure_callback,
}


def task_validate_config(**context):
    """Verify configuration, directories, and database connectivity."""
    from src.utils.database import db
    with db.connection() as conn:
        logger.info("Database connectivity test: PASSED")
    logger.info("Environment configuration validated successfully.")


def task_run_pipeline_stage(**context):
    """Execute complete end-to-end hotel batch pipeline."""
    exec_date = context.get("ds")  # YYYY-MM-DD
    run_id = f"airflow_{context.get('run_id', generate_run_id())}"
    batch_id = generate_batch_id()

    runner = HotelPipelineRunner()
    result = runner.run(target_date=exec_date, run_id=run_id, batch_id=batch_id, incremental=True)

    if result["status"] != "SUCCESS":
        raise RuntimeError(f"Pipeline execution failed: {result.get('error_message')}")
    return result


if AIRFLOW_INSTALLED:
    with DAG(
        dag_id="hotel_batch_data_pipeline",
        default_args=default_args,
        description="Daily incremental & monitored batch ETL pipeline for Guwahati hotels analytics.",
        schedule_interval="0 2 * * *",  # Daily at 02:00 AM UTC
        start_date=datetime(2026, 1, 1),
        catchup=False,
        max_active_runs=1,
        tags=["hospitality", "hotel", "analytics", "batch_etl"],
    ) as dag:

        start = EmptyOperator(task_id="start_pipeline")

        validate_config = PythonOperator(
            task_id="validate_configuration",
            python_callable=task_validate_config
        )

        execute_pipeline = PythonOperator(
            task_id="execute_hotel_pipeline",
            python_callable=task_run_pipeline_stage
        )

        end = EmptyOperator(
            task_id="pipeline_complete",
            on_success_callback=on_success_callback
        )

        start >> validate_config >> execute_pipeline >> end
else:
    # Standalone mock dag definition for environments where apache-airflow is not in python path
    dag = None
