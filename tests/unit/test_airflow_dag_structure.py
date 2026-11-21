"""
Unit tests for Airflow DAG structure and configuration.
Validates AST parsing, task dependencies, retry intervals, callbacks, and schedule interval.
"""

import ast
import re
from pathlib import Path


DAG_PATH = Path(__file__).resolve().parent.parent.parent / "dags" / "hotel_pipeline_dag.py"


def test_dag_ast_validity():
    """Verify that hotel_pipeline_dag.py is syntactically valid Python code."""
    code = DAG_PATH.read_text(encoding="utf-8")
    tree = ast.parse(code)
    assert len(tree.body) > 0


def test_dag_configuration_parameters():
    """Verify schedule interval, owner, and failure callbacks."""
    code = DAG_PATH.read_text(encoding="utf-8")

    assert 'dag_id="hotel_batch_data_pipeline"' in code
    assert 'schedule_interval="0 2 * * *"' in code
    assert "catchup=False" in code
    assert '"retries": 2' in code
    assert "on_failure_callback" in code


def test_dag_tasks_and_dependencies():
    """Verify required tasks and task pipeline dependency ordering."""
    code = DAG_PATH.read_text(encoding="utf-8")

    assert 'task_id="start_pipeline"' in code
    assert 'task_id="validate_configuration"' in code
    assert 'task_id="execute_hotel_pipeline"' in code
    assert 'task_id="pipeline_complete"' in code

    # Verify dependency chain
    assert "start >> validate_config >> execute_pipeline >> end" in code
