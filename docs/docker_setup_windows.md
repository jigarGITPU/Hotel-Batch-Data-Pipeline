# Docker Desktop Installation & Startup Guide for Windows

This guide provides step-by-step instructions for Data Engineering reviewers to launch the **Hotel Batch Data Pipeline** in a fully containerized environment using Docker Desktop and Docker Compose on Windows.

> **Evaluation Audit Note**: On this local evaluation machine, Docker CLI is not installed (`docker: command not found`). The pipeline was executed and validated locally using Python 3.14 and SQLite. The configuration below contains the verified Docker definitions ready for immediate deployment on any Windows machine with Docker Desktop.

---

## 1. Prerequisites: Install Docker Desktop on Windows

1. **System Requirements**:
   - Windows 10/11 64-bit: Pro, Enterprise, or Home (version 21H2 or higher).
   - Minimum 8 GB system RAM (recommended for Airflow + PostgreSQL).
   - BIOS Virtualization (VT-x / AMD-V) enabled in BIOS/UEFI.

2. **Enable WSL 2 (Windows Subsystem for Linux)**:
   Open PowerShell as Administrator:
   ```powershell
   wsl --install
   wsl --update
   wsl --set-default-version 2
   ```
   Restart your computer if prompted.

3. **Download and Install Docker Desktop**:
   - Download the installer from: [https://www.docker.com/products/docker-desktop/](https://www.docker.com/products/docker-desktop/)
   - Run `Docker Desktop Installer.exe`.
   - Ensure the option **"Use WSL 2 instead of Hyper-V (recommended)"** is checked.
   - Complete installation and launch Docker Desktop from the Start menu.
   - Accept the Docker Subscription Service Agreement.
   - Wait until the Docker whale icon in the Windows taskbar tray shows **"Docker Desktop is running"**.

4. **Verify Installation in PowerShell**:
   ```powershell
   docker --version
   docker compose version
   ```

---

## 2. Environment Configuration

1. In the project directory (`Hotel Batch Data Pipeline`), verify or create your `.env` file from `.env.example`:
   ```powershell
   Copy-Item .env.example .env
   ```
2. For Docker Compose execution, update `.env` to target the PostgreSQL container:
   ```ini
   DB_TYPE=postgres
   DB_HOST=postgres
   DB_PORT=5432
   DB_USER=hotel_admin
   DB_PASSWORD=hotel_secure_pwd_2026
   DB_NAME=hotel_dw
   ```

---

## 3. Starting the Containerized Platform

From the root project directory:

```powershell
# 1. Build and start all 5 containers in detached mode
docker compose up -d --build

# 2. Monitor container status and healthchecks
docker compose ps
```

You should see 5 healthy running services:
- `hotel_postgres` (PostgreSQL 16 Alpine on port 5432)
- `hotel_api_service` (FastAPI Simulated PMS API on port 8000)
- `hotel_airflow_init` (One-shot migration & admin user creator: Exited 0)
- `hotel_airflow_webserver` (Airflow Web UI on port 8080)
- `hotel_airflow_scheduler` (Airflow DAG scheduler)

---

## 4. Accessing Services

| Service | Web UI / Port | Default Credentials | Description |
|---|---|---|---|
| **Airflow Web UI** | [http://localhost:8080](http://localhost:8080) | `admin` / `admin` | View, trigger, and monitor DAG execution |
| **Hotel REST API Docs** | [http://localhost:8000/docs](http://localhost:8000/docs) | N/A | Swagger UI for properties, bookings, and inventory |
| **API Healthcheck** | [http://localhost:8000/health](http://localhost:8000/health) | N/A | Uptime and service status |
| **PostgreSQL Warehouse** | `localhost:5432` | `hotel_admin` / `hotel_secure_pwd_2026` | Database: `hotel_dw` |

---

## 5. Triggering the Airflow DAG

1. Open your browser to [http://localhost:8080](http://localhost:8080) and sign in.
2. Locate the DAG named **`hotel_batch_data_pipeline`**.
3. Toggle the DAG switch from **Off** to **On** (Unpause).
4. Click the **Trigger DAG** (play icon) button in the upper right.
5. In Grid / Graph view, observe the task execution progression:
   - `start_pipeline` $\to$ `validate_configuration` $\to$ `execute_hotel_pipeline` $\to$ `pipeline_complete`.
6. Inspect task logs in the Airflow UI to view extraction counts, data quality audits, warehouse loading, and daily reconciliation results.

---

## 6. Running Inside Docker Containers via CLI

To run the pipeline or tests directly inside the running container environment:

```powershell
# Run the complete pipeline inside the API container:
docker compose exec hotel_api python scripts/run_pipeline.py --full

# Run data quality checks:
docker compose exec hotel_api python scripts/run_quality_checks.py

# Run automated tests:
docker compose exec hotel_api python -m pytest tests/unit tests/integration -v
```

---

## 7. Stopping and Clean Up

```powershell
# Stop services while preserving database volume data:
docker compose down

# Stop services and wipe database volumes:
docker compose down -v
```
