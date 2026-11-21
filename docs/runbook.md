# Operational Runbook – Hotel Batch Data Pipeline

## 1. Quick Start & Environment Initialization

### 1.1 Local Standalone Environment (Zero Docker Required)
The pipeline is designed to run locally using Python and the embedded SQLite engine:
```bash
# 1. Install dependencies
python -m pip install -r requirements.txt

# 2. Generate initial synthetic source dataset (~5,000 records)
python scripts/generate_data.py --mode small

# 3. Execute the full end-to-end pipeline
python scripts/run_pipeline.py

# 4. View data quality and quarantine audit results
python scripts/run_quality_checks.py
```

### 1.2 Docker Compose Production Environment
```bash
# Start all services (PostgreSQL 16, Airflow Webserver, Scheduler, API)
docker compose up -d

# Check service health
docker compose ps

# Access Airflow UI: http://localhost:8080 (admin / admin)
# Access Hotel REST API: http://localhost:8000/docs
```

## 2. Common Operational Tasks

### 2.1 Running a Historical Date Backfill
To run a sequential daily backfill over a date range:
```bash
python scripts/run_backfill.py --start-date 2026-01-01 --end-date 2026-01-14
```

### 2.2 Re-running a Specific Date (Idempotency Verification)
To rerun a specific business date without creating duplicate fact records:
```bash
python scripts/run_pipeline.py --date 2026-01-15 --full
```

### 2.3 Inspecting Quarantined Records
To inspect quarantined records and their failure reasons:
```bash
python scripts/run_quality_checks.py
```
Or query directly via SQL:
```sql
SELECT reason_code, COUNT(*), MIN(detected_at), MAX(detected_at)
FROM quarantine.quarantine_records
GROUP BY reason_code;
```

### 2.4 Performance Benchmarking
To benchmark throughput and memory scaling:
```bash
python scripts/benchmark.py --mode small --batch-sizes 2000 5000 10000
```

## 3. Failure Troubleshooting Guide

### Issue: "Reconciliation discrepancy > 0.1%"
- **Diagnosis**: Check `metadata_audit.daily_reconciliation` for the specific date.
- **Root Cause**: Often caused by unmapped room types or unexpected duplicate keys.
- **Resolution**: Query `quarantine.quarantine_records` for new reason codes, update `analytics_dw.room_type_mapping`, and re-run with `--date YYYY-MM-DD`.

### Issue: "REST API connection refused on port 8000"
- **Diagnosis**: Simulated REST API process is not running.
- **Behavior**: The pipeline automatically switches to offline fallback mode, logging a warning and reading local data files without pipeline disruption.
- **Resolution**: To start the API daemon manually:
  ```bash
  uvicorn api.main:app --host 0.0.0.0 --port 8000
  ```

### Issue: "Database locked or connection timeout"
- **Resolution**: Verify that connection pooling parameters in `.env` match host hardware resources. For SQLite testing, ensure no conflicting writer locks exist.
