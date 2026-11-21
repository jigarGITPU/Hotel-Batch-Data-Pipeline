"""
Configuration management module for the Hotel Batch Data Pipeline.
Loads settings from environment variables and provides centralized configuration.
"""

import os
from pathlib import Path
from dataclasses import dataclass
from dotenv import load_dotenv

# Project Root Directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Load .env file if it exists
dotenv_path = PROJECT_ROOT / ".env"
if dotenv_path.exists():
    load_dotenv(dotenv_path=dotenv_path)


@dataclass(frozen=True)
class DatabaseConfig:
    db_type: str = os.getenv("DB_TYPE", "sqlite").lower()
    host: str = os.getenv("DB_HOST", "localhost")
    port: int = int(os.getenv("DB_PORT", "5432"))
    user: str = os.getenv("DB_USER", "hotel_admin")
    password: str = os.getenv("DB_PASSWORD", "hotel_secure_pwd_2026")
    database: str = os.getenv("DB_NAME", "hotel_dw")
    sqlite_path: Path = PROJECT_ROOT / os.getenv("SQLITE_PATH", "data/hotel_warehouse.db")

    @property
    def connection_url(self) -> str:
        if self.db_type == "postgres":
            return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        return f"sqlite:///{self.sqlite_path}"


@dataclass(frozen=True)
class PipelineConfig:
    batch_size: int = int(os.getenv("BATCH_SIZE", "10000"))
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    watermark_overlap_hours: int = int(os.getenv("WATERMARK_OVERLAP_HOURS", "2"))
    data_dir: Path = PROJECT_ROOT / os.getenv("DATA_DIR", "data")
    raw_data_dir: Path = PROJECT_ROOT / os.getenv("RAW_DATA_DIR", "data/raw")
    generated_data_dir: Path = PROJECT_ROOT / os.getenv("GENERATED_DATA_DIR", "data/generated")
    reports_dir: Path = PROJECT_ROOT / "reports"
    sql_dir: Path = PROJECT_ROOT / "sql"


@dataclass(frozen=True)
class APIConfig:
    base_url: str = os.getenv("API_BASE_URL", "http://localhost:8000")
    timeout: int = int(os.getenv("API_TIMEOUT", "30"))
    max_retries: int = int(os.getenv("API_MAX_RETRIES", "3"))


@dataclass(frozen=True)
class QualityConfig:
    max_unexplained_reconciliation_diff_pct: float = 0.1  # Target: < 0.1%
    max_quarantine_rate_pct: float = 5.0  # Alert if > 5% quarantined


class Config:
    PROJECT_ROOT: Path = PROJECT_ROOT
    DB: DatabaseConfig = DatabaseConfig()
    PIPELINE: PipelineConfig = PipelineConfig()
    API: APIConfig = APIConfig()
    QUALITY: QualityConfig = QualityConfig()


config = Config()
