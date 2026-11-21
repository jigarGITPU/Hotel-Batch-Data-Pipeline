"""
Structured logging module for the Hotel Batch Data Pipeline.
Provides standardized logging format with run ID, batch ID, and pipeline metrics.
"""

import os
import sys
import uuid
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any

from src.utils.config import config


class PipelineLogger:
    """Centralized logger with structured metadata formatting."""

    _instance: Optional["PipelineLogger"] = None

    def __init__(self, log_level: str = "INFO", log_file: Optional[Path] = None):
        self.logger = logging.getLogger("hotel_pipeline")
        numeric_level = getattr(logging, log_level.upper(), logging.INFO)
        self.logger.setLevel(numeric_level)

        if not self.logger.handlers:
            # Console Handler
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(numeric_level)
            formatter = logging.Formatter(
                "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

            # File Handler
            if log_file:
                log_file.parent.mkdir(parents=True, exist_ok=True)
                file_handler = logging.FileHandler(str(log_file), encoding="utf-8")
                file_handler.setLevel(numeric_level)
                file_handler.setFormatter(formatter)
                self.logger.addHandler(file_handler)

    @classmethod
    def get_logger(cls, name: Optional[str] = None) -> logging.Logger:
        if cls._instance is None:
            default_log_file = config.PROJECT_ROOT / "reports" / "pipeline.log"
            cls._instance = cls(log_level=config.PIPELINE.log_level, log_file=default_log_file)
        if name:
            return cls._instance.logger.getChild(name)
        return cls._instance.logger


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Utility function to obtain a configured logger instance."""
    return PipelineLogger.get_logger(name)


def generate_run_id() -> str:
    """Generate a unique run identifier with timestamp."""
    now_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"run_{now_str}_{uuid.uuid4().hex[:8]}"


def generate_batch_id() -> str:
    """Generate a unique batch identifier."""
    return f"batch_{uuid.uuid4().hex[:12]}"
