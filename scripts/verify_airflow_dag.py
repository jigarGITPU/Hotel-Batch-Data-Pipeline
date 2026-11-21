"""Airflow DAG inspection and verification script."""
import ast
import os
import sys

dag_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dags", "hotel_pipeline_dag.py")

print("=== AIRFLOW DAG STATIC AUDIT ===")
with open(dag_file, "r", encoding="utf-8") as f:
    code = f.read()

# 1. Parse AST
tree = ast.parse(code)
print(f"AST Parsing: SUCCESS ({len(tree.body)} top-level nodes)")

# 2. Extract key configurations
import re
dag_id_match = re.search(r'dag_id="([^"]+)"', code)
schedule_match = re.search(r'schedule_interval="([^"]+)"', code)
start_date_match = re.search(r'start_date=datetime\(([^)]+)\)', code)
catchup_match = re.search(r'catchup=([A-Za-z]+)', code)
retries_match = re.search(r'"retries":\s*(\d+)', code)
retry_delay_match = re.search(r'"retry_delay":\s*timedelta\(([^)]+)\)', code)
failure_cb_match = re.search(r'"on_failure_callback":\s*([A-Za-z_]+)', code)

print(f"  DAG ID:              {dag_id_match.group(1) if dag_id_match else 'N/A'}")
print(f"  Schedule Interval:   {schedule_match.group(1) if schedule_match else 'N/A'}")
print(f"  Start Date:          {start_date_match.group(1) if start_date_match else 'N/A'}")
print(f"  Catchup:             {catchup_match.group(1) if catchup_match else 'N/A'}")
print(f"  Retries:             {retries_match.group(1) if retries_match else 'N/A'}")
print(f"  Retry Delay:         {retry_delay_match.group(1) if retry_delay_match else 'N/A'}")
print(f"  On Failure Callback: {failure_cb_match.group(1) if failure_cb_match else 'N/A'}")

# 3. Tasks and dependency chain
tasks = re.findall(r'task_id="([^"]+)"', code)
print(f"  Tasks Defined ({len(tasks)}): {tasks}")
dep_chain = re.findall(r'(\w+)\s*>>\s*(\w+)', code)
print(f"  Dependency Chains:   {dep_chain}")

# 4. Import test
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dags.hotel_pipeline_dag import AIRFLOW_INSTALLED, on_failure_callback, on_success_callback, default_args
print(f"  AIRFLOW_INSTALLED in python env: {AIRFLOW_INSTALLED}")
print(f"  Default Args: {default_args}")
print("=== AIRFLOW DAG AUDIT COMPLETE ===")
