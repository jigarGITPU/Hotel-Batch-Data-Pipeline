"""
Alerting and Notification module for Hotel Batch Data Pipeline.
Dispatches operational alerts for pipeline failures, data quality breaches, and reconciliation anomalies.
"""

from datetime import datetime, timezone
from typing import Dict, Any, Optional
from src.utils.config import config
from src.monitoring.logger import get_logger

logger = get_logger("notifications")


class NotificationService:
    """Dispatches pipeline notifications via console and standard email formatting."""

    def __init__(self):
        self.email_target = os_email = getattr(config.PIPELINE, "alert_email", "alerts@hotelanalytics.local")

    def send_alert(
        self,
        alert_type: str,
        title: str,
        details: Dict[str, Any],
        severity: str = "ERROR"
    ):
        """Send formatted alert notification."""
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        alert_box = (
            f"\n{'='*70}\n"
            f" [ALERT - {severity}] {title}\n"
            f" Timestamp: {timestamp}\n"
            f" Type: {alert_type}\n"
            f" Target: {self.email_target}\n"
            f" Details:\n"
        )
        for k, v in details.items():
            alert_box += f"   - {k}: {v}\n"
        alert_box += f"{'='*70}\n"

        if severity in ["CRITICAL", "ERROR"]:
            logger.error(alert_box)
        elif severity == "WARNING":
            logger.warning(alert_box)
        else:
            logger.info(alert_box)


notification_service = NotificationService()
