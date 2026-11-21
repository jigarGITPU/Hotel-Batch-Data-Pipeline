"""
Data Cleaning and Normalization module for Hotel Batch Data Pipeline.
Cleans strings, parses dates/timestamps, calculates derived metrics (lead time, stay length, amounts).
"""

from datetime import datetime, date, timezone
from typing import Optional, Dict, Any, Tuple
import pandas as pd
import numpy as np

from src.monitoring.logger import get_logger

logger = get_logger("cleaning_transformer")


def parse_date_to_key(dt_val: Any) -> Optional[int]:
    """Convert any date or ISO string to integer date key (YYYYMMDD)."""
    if dt_val is None or pd.isna(dt_val):
        return None
    s = str(dt_val).strip()
    if not s or s.upper() in ["NONE", "NAT", "NAN", "NULL", ""]:
        return None
    try:
        # Take first 10 characters (YYYY-MM-DD)
        date_str = s[:10]
        parsed = datetime.strptime(date_str, "%Y-%m-%d").date()
        return int(parsed.strftime("%Y%m%d"))
    except Exception:
        return None


def parse_iso_timestamp(ts_val: Any) -> Optional[str]:
    """Parse and normalize timestamp to standard ISO 8601 UTC string."""
    if ts_val is None or pd.isna(ts_val):
        return None
    s = str(ts_val).strip()
    if not s or s.upper() in ["NONE", "NAT", "NAN", "NULL", ""]:
        return None
    try:
        # Handle trailing Z or offset
        if s.endswith("Z"):
            s_clean = s[:-1] + "+00:00"
        else:
            s_clean = s
        dt = datetime.fromisoformat(s_clean)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return None


def clean_bookings(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and enrich raw booking records."""
    df_clean = df.copy()

    # Clean whitespace
    str_cols = ["booking_id", "booking_line_id", "guest_id", "hotel_id", "room_id", "room_type", "booking_status", "currency"]
    for col in str_cols:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str).str.strip()

    # Parse date keys
    df_clean["booking_date_key"] = df_clean["booking_date"].apply(parse_date_to_key)
    df_clean["check_in_date_key"] = df_clean["check_in_date"].apply(parse_date_to_key)
    df_clean["check_out_date_key"] = df_clean["check_out_date"].apply(parse_date_to_key)

    # Lead time and length of stay calculations
    lead_times = []
    stay_lengths = []
    for _, r in df_clean.iterrows():
        try:
            b_d = datetime.strptime(str(r["booking_date"])[:10], "%Y-%m-%d").date()
            ci_d = datetime.strptime(str(r["check_in_date"])[:10], "%Y-%m-%d").date()
            co_d = datetime.strptime(str(r["check_out_date"])[:10], "%Y-%m-%d").date()
            lt = max(0, (ci_d - b_d).days)
            sl = (co_d - ci_d).days
        except Exception:
            lt = 0
            sl = 1
        lead_times.append(lt)
        stay_lengths.append(sl)

    df_clean["lead_time_days"] = lead_times
    df_clean["length_of_stay_days"] = stay_lengths

    # Clean number of guests
    df_clean["number_of_guests"] = pd.to_numeric(df_clean.get("number_of_guests", 1), errors="coerce").fillna(1).astype(int)

    # Calculate total amount in INR
    if "room_rate_inr" in df_clean.columns:
        df_clean["total_amount_inr"] = (
            df_clean["room_rate_inr"] * df_clean["length_of_stay_days"].clip(lower=1)
        ).round(2)

    # Normalize source_updated_at
    if "source_updated_at" in df_clean.columns:
        df_clean["source_updated_at"] = df_clean["source_updated_at"].apply(parse_iso_timestamp)

    return df_clean


def clean_stays(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and calculate stay durations."""
    df_clean = df.copy()
    str_cols = ["stay_id", "booking_id", "guest_id", "hotel_id", "room_id", "stay_status"]
    for col in str_cols:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str).str.strip()

    # Parse actual timestamps
    df_clean["actual_check_in"] = df_clean["actual_check_in"].apply(parse_iso_timestamp)
    df_clean["actual_check_out"] = df_clean["actual_check_out"].apply(parse_iso_timestamp)

    # Calculate date keys
    df_clean["check_in_date_key"] = df_clean["actual_check_in"].apply(parse_date_to_key)
    df_clean["check_out_date_key"] = df_clean["actual_check_out"].apply(parse_date_to_key)

    # Duration in hours
    durations = []
    for _, r in df_clean.iterrows():
        try:
            if r["actual_check_in"] and r["actual_check_out"]:
                t1 = datetime.fromisoformat(r["actual_check_in"])
                t2 = datetime.fromisoformat(r["actual_check_out"])
                dur = round((t2 - t1).total_seconds() / 3600.0, 2)
            else:
                dur = None
        except Exception:
            dur = None
        durations.append(dur)

    df_clean["actual_duration_hours"] = durations
    return df_clean


def clean_reviews(df: pd.DataFrame) -> pd.DataFrame:
    """Clean reviews and assign sentiment category based on rating."""
    df_clean = df.copy()
    str_cols = ["review_id", "booking_id", "guest_id", "hotel_id"]
    for col in str_cols:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str).str.strip()

    df_clean["review_date_key"] = df_clean["review_date"].apply(parse_date_to_key)
    df_clean["rating"] = pd.to_numeric(df_clean["rating"], errors="coerce")

    # Map sentiment
    def _map_sentiment(rating: Any) -> str:
        if pd.isna(rating):
            return "UNKNOWN"
        r = int(rating)
        if r >= 5:
            return "VERY_POSITIVE"
        elif r == 4:
            return "POSITIVE"
        elif r == 3:
            return "NEUTRAL"
        elif r == 2:
            return "NEGATIVE"
        elif r == 1:
            return "VERY_NEGATIVE"
        return "UNKNOWN"

    df_clean["sentiment_category"] = df_clean["rating"].apply(_map_sentiment)
    return df_clean
