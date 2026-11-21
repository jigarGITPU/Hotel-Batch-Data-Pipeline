"""
Simulated REST API Extractor for Hotel Batch Data Pipeline.
Fetches paginated booking and inventory data via HTTP REST endpoints with
automatic retries, timeout management, rate-limiting considerations, and fallback mode.
"""

import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple
import requests
import pandas as pd

from src.utils.config import config
from src.monitoring.logger import get_logger

logger = get_logger("api_extractor")


class APIExtractor:
    """Extracts hotel booking and inventory records from simulated REST API."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        timeout: Optional[int] = None,
        max_retries: Optional[int] = None
    ):
        self.base_url = base_url or config.API.base_url
        self.timeout = timeout or config.API.timeout
        self.max_retries = max_retries or config.API.max_retries

    def _fetch_page_with_retry(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Fetch an API page with exponential backoff retry strategy."""
        url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        last_error = None

        for attempt in range(1, self.max_retries + 1):
            try:
                response = requests.get(url, params=params, timeout=self.timeout)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code >= 500:
                    logger.warning(f"Server error {response.status_code} on {url} (Attempt {attempt}/{self.max_retries})")
                else:
                    response.raise_for_status()
            except (requests.RequestException, Exception) as exc:
                last_error = exc
                logger.warning(f"Request failed on {url} (Attempt {attempt}/{self.max_retries}): {exc}")

            if attempt < self.max_retries:
                time.sleep(0.5 * (2 ** (attempt - 1)))

        raise RuntimeError(f"Failed to fetch {url} after {self.max_retries} attempts: {last_error}")

    def extract_paginated(
        self,
        endpoint: str,
        target_date: Optional[str] = None,
        hotel_id: Optional[str] = None,
        page_size: int = 500
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Extract all pages from an endpoint, with automatic pagination and metadata tracking.
        If the API server is unreachable, smoothly falls back to reading the generated file
        to guarantee reliable standalone pipeline runs.
        """
        start_ts = datetime.now(timezone.utc)
        extraction_time_iso = start_ts.isoformat()
        all_records: List[Dict[str, Any]] = []

        metadata: Dict[str, Any] = {
            "endpoint": endpoint,
            "source_type": "REST_API",
            "extracted_at": extraction_time_iso,
            "target_date": target_date,
            "pages_fetched": 0,
            "rows_extracted": 0,
            "status": "FAILED",
            "fallback_used": False,
            "errors": []
        }

        # Attempt API extraction
        try:
            page = 1
            has_next = True

            while has_next:
                params = {"page": page, "page_size": page_size}
                if target_date:
                    params["target_date"] = target_date
                if hotel_id:
                    params["hotel_id"] = hotel_id

                payload = self._fetch_page_with_retry(endpoint, params)
                data = payload.get("data", [])
                meta = payload.get("metadata", {})

                all_records.extend(data)
                metadata["pages_fetched"] += 1
                has_next = meta.get("has_next", False)
                page += 1

            df = pd.DataFrame(all_records)
            metadata["status"] = "SUCCESS"

        except Exception as api_err:
            logger.warning(
                f"REST API at {self.base_url} unavailable ({api_err}). "
                f"Activating offline fallback extract."
            )
            metadata["fallback_used"] = True
            metadata["errors"].append(str(api_err))

            # Fallback to direct generated file
            file_map = {
                "api/v1/bookings": "hotel_bookings.csv",
                "api/v1/inventory": "room_inventory.csv",
                "api/v1/hotels": "hotels_reference.csv"
            }
            fname = file_map.get(endpoint.strip("/"), "hotel_bookings.csv")
            fallback_path = config.PIPELINE.generated_data_dir / fname

            if fallback_path.exists():
                df = pd.read_csv(fallback_path, dtype=str, keep_default_na=False)
                if target_date:
                    date_col = "booking_date" if "booking" in endpoint else "inventory_date"
                    if date_col in df.columns:
                        df = df[df[date_col] == target_date].copy()
                metadata["status"] = "SUCCESS_FALLBACK"
            else:
                df = pd.DataFrame()
                metadata["status"] = "FAILED"

        # Attach audit columns
        if not df.empty:
            df["source_file"] = f"API::{endpoint}"
            df["extracted_at"] = extraction_time_iso
            df["source_system"] = "REST_API_CHANNEL"

        metadata["rows_extracted"] = len(df)
        metadata["duration_seconds"] = (datetime.now(timezone.utc) - start_ts).total_seconds()
        logger.info(f"API Extractor extracted {len(df)} records from {endpoint} ({metadata['status']})")
        return df, metadata
