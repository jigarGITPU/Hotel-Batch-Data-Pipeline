"""
Simulated Hotel REST API service using FastAPI.
Provides paginated endpoints for hotel bookings, room inventory, and properties.
"""

from datetime import datetime, date, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any
from fastapi import FastAPI, Query, HTTPException, status
from pydantic import BaseModel
import pandas as pd

from src.utils.config import config
from src.monitoring.logger import get_logger

logger = get_logger("simulated_api")

app = FastAPI(
    title="Guwahati Hotel Systems Simulated REST API",
    description="REST API simulation providing booking and inventory feeds for the batch ETL pipeline.",
    version="1.0.0"
)


class PaginationMetadata(BaseModel):
    page: int
    page_size: int
    total_records: int
    total_pages: int
    has_next: bool


class APIResponse(BaseModel):
    status: str
    metadata: PaginationMetadata
    data: List[Dict[str, Any]]


def _load_data(filename: str) -> pd.DataFrame:
    """Helper to load generated data file."""
    path = config.PIPELINE.generated_data_dir / filename
    if not path.exists():
        # Fallback to empty DataFrame with columns
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str).fillna("")


@app.get("/health", tags=["System"])
def health_check():
    """Health check endpoint."""
    return {
        "status": "UP",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": "Guwahati Hotel Simulated API"
    }


@app.get("/api/v1/hotels", tags=["Hotels"])
def get_hotels():
    """Return all 14 Guwahati hotel properties."""
    df = _load_data("hotels_reference.csv")
    if df.empty:
        raise HTTPException(status_code=404, detail="Hotels reference data not generated yet.")
    return {"status": "success", "count": len(df), "data": df.to_dict(orient="records")}


@app.get("/api/v1/bookings", response_model=APIResponse, tags=["Bookings"])
def get_bookings(
    target_date: Optional[str] = Query(None, description="Filter by booking_date (YYYY-MM-DD)"),
    hotel_id: Optional[str] = Query(None, description="Filter by hotel_id (e.g., HTL-GAU-001)"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(100, ge=1, le=1000, description="Records per page")
):
    """Retrieve paginated hotel bookings with optional date and hotel filtering."""
    df = _load_data("hotel_bookings.csv")
    if df.empty:
        return APIResponse(
            status="success",
            metadata=PaginationMetadata(page=page, page_size=page_size, total_records=0, total_pages=0, has_next=False),
            data=[]
        )

    if target_date:
        df = df[df["booking_date"] == target_date]
    if hotel_id:
        df = df[df["hotel_id"] == hotel_id]

    total_records = len(df)
    total_pages = (total_records + page_size - 1) // page_size if total_records > 0 else 0
    start_idx = (page - 1) * page_size
    end_idx = start_idx + page_size

    sliced = df.iloc[start_idx:end_idx]
    has_next = page < total_pages

    return APIResponse(
        status="success",
        metadata=PaginationMetadata(
            page=page,
            page_size=page_size,
            total_records=total_records,
            total_pages=total_pages,
            has_next=has_next
        ),
        data=sliced.to_dict(orient="records")
    )


@app.get("/api/v1/inventory", response_model=APIResponse, tags=["Inventory"])
def get_inventory(
    target_date: Optional[str] = Query(None, description="Filter by inventory_date (YYYY-MM-DD)"),
    hotel_id: Optional[str] = Query(None, description="Filter by hotel_id"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(100, ge=1, le=1000, description="Records per page")
):
    """Retrieve paginated room inventory with optional date and hotel filtering."""
    df = _load_data("room_inventory.csv")
    if df.empty:
        return APIResponse(
            status="success",
            metadata=PaginationMetadata(page=page, page_size=page_size, total_records=0, total_pages=0, has_next=False),
            data=[]
        )

    if target_date:
        df = df[df["inventory_date"] == target_date]
    if hotel_id:
        df = df[df["hotel_id"] == hotel_id]

    total_records = len(df)
    total_pages = (total_records + page_size - 1) // page_size if total_records > 0 else 0
    start_idx = (page - 1) * page_size
    end_idx = start_idx + page_size

    sliced = df.iloc[start_idx:end_idx]
    has_next = page < total_pages

    return APIResponse(
        status="success",
        metadata=PaginationMetadata(
            page=page,
            page_size=page_size,
            total_records=total_records,
            total_pages=total_pages,
            has_next=has_next
        ),
        data=sliced.to_dict(orient="records")
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=False)
