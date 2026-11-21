"""
Database connection and execution abstraction for Hotel Batch Data Pipeline.
Supports both PostgreSQL (production target) and SQLite (local testing fallback),
providing connection pooling, transaction management, and bulk loading utilities.
"""

import re
from pathlib import Path
from typing import Optional, List, Dict, Any, Generator
from contextlib import contextmanager
import pandas as pd
from sqlalchemy import create_engine, text, Engine, Connection
from sqlalchemy.exc import OperationalError

from src.utils.config import config
from src.monitoring.logger import get_logger

logger = get_logger("database")


class DatabaseManager:
    """Manages database engines, schema execution, transactions, and bulk operations."""

    def __init__(self, connection_url: Optional[str] = None):
        self.config_url = connection_url or config.DB.connection_url
        self.is_sqlite = "sqlite" in self.config_url
        self._engine: Optional[Engine] = None
        self._init_engine()

    def _init_engine(self):
        """Initialize engine with graceful fallback if PostgreSQL is not reachable."""
        try:
            if not self.is_sqlite:
                # Test PostgreSQL connection
                test_engine = create_engine(
                    self.config_url,
                    pool_size=10,
                    max_overflow=20,
                    connect_args={"connect_timeout": 3}
                )
                with test_engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                self._engine = test_engine
                logger.info(f"Connected successfully to PostgreSQL at {config.DB.host}:{config.DB.port}/{config.DB.database}")
            else:
                config.DB.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
                self._engine = create_engine(
                    f"sqlite:///{config.DB.sqlite_path}",
                    echo=False
                )
                logger.info(f"Using SQLite database at {config.DB.sqlite_path}")
        except (OperationalError, Exception) as exc:
            logger.warning(
                f"Could not connect to configured DB ({self.config_url}): {exc}. "
                f"Falling back to local SQLite at {config.DB.sqlite_path}"
            )
            config.DB.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
            self.config_url = f"sqlite:///{config.DB.sqlite_path}"
            self.is_sqlite = True
            self._engine = create_engine(self.config_url, echo=False)

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            self._init_engine()
        return self._engine

    @contextmanager
    def connection(self) -> Generator[Connection, None, None]:
        with self.engine.connect() as conn:
            yield conn

    @contextmanager
    def transaction(self) -> Generator[Connection, None, None]:
        with self.engine.begin() as conn:
            yield conn

    def execute_sql(self, sql: str, params: Optional[Dict[str, Any]] = None):
        with self.transaction() as conn:
            return conn.execute(text(sql), params or {})

    def fetch_dataframe(self, sql: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        with self.connection() as conn:
            return pd.read_sql_query(text(sql), conn, params=params or {})

    def execute_script(self, script_path: Path):
        """Execute a multi-statement SQL script file."""
        if not script_path.exists():
            raise FileNotFoundError(f"SQL script not found: {script_path}")

        content = script_path.read_text(encoding="utf-8")
        # In SQLite, schemas like raw_staging.tablename can be rewritten to raw_staging_tablename
        # if multi-schema attachment isn't used
        if self.is_sqlite:
            # Replace schema qualifiers with underscores for SQLite compatibility
            content = re.sub(r'\b(raw_staging|analytics_dw|metadata_audit|quarantine)\.([a-zA-Z0-9_]+)', r'\1_\2', content)
            # Remove Postgres specific extensions or types if present
            content = content.replace("CREATE EXTENSION IF NOT EXISTS", "-- CREATE EXTENSION")
            content = content.replace("TIMESTAMP WITH TIME ZONE", "TEXT")
            content = content.replace("TIMESTAMPTZ", "TEXT")
            content = content.replace("SERIAL PRIMARY KEY", "INTEGER PRIMARY KEY AUTOINCREMENT")
            content = content.replace("BIGSERIAL PRIMARY KEY", "INTEGER PRIMARY KEY AUTOINCREMENT")

        statements = [stmt.strip() for stmt in content.split(";") if stmt.strip()]
        with self.transaction() as conn:
            for stmt in statements:
                # Skip comments-only statements
                clean_lines = [l for l in stmt.splitlines() if not l.strip().startswith("--")]
                clean_stmt = "\n".join(clean_lines).strip()
                if not clean_stmt:
                    continue
                # In SQLite, skip CREATE SCHEMA statements
                if self.is_sqlite and clean_stmt.upper().startswith("CREATE SCHEMA"):
                    continue
                conn.execute(text(clean_stmt))
        logger.info(f"Executed SQL script successfully: {script_path.name}")

    def get_table_name(self, schema: str, table: str) -> str:
        """Format table name according to dialect."""
        if self.is_sqlite:
            return f"{schema}_{table}"
        return f"{schema}.{table}"

    def bulk_insert(
        self,
        df: pd.DataFrame,
        schema: str,
        table: str,
        if_exists: str = "append",
        chunksize: int = 5000
    ) -> int:
        """Bulk insert DataFrame into database."""
        if df.empty:
            return 0

        target_table = f"{schema}_{table}" if self.is_sqlite else table
        target_schema = None if self.is_sqlite else schema

        # Use None method for SQLite to avoid variable count limits (uses fast executemany)
        # Use 'multi' for PostgreSQL
        insert_method = None if self.is_sqlite else "multi"
        actual_chunksize = 1000 if self.is_sqlite else chunksize

        df.to_sql(
            name=target_table,
            schema=target_schema,
            con=self.engine,
            if_exists=if_exists,
            index=False,
            chunksize=actual_chunksize,
            method=insert_method
        )
        return len(df)


# Global default database instance
db = DatabaseManager()
