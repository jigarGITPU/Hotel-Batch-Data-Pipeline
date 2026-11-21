"""
Warehouse Loader module for Hotel Batch Data Pipeline.
Implements watermark-based incremental loading, surrogate key lookups,
and idempotent UPSERT operations for dimensions and fact tables.
"""

from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List, Tuple
import pandas as pd
from sqlalchemy import text

from src.utils.config import config
from src.utils.database import DatabaseManager, db
from src.monitoring.logger import get_logger

logger = get_logger("warehouse_loader")


class WarehouseLoader:
    """Handles dimension lookups, incremental watermark tracking, and idempotent UPSERT loading."""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.db = db_manager or db
        # In-memory dimension caches for surrogate key lookups
        self.hotel_cache: Dict[str, int] = {}
        self.guest_cache: Dict[str, int] = {}
        self.room_cache: Dict[str, int] = {}
        self._refresh_dimension_caches()

    def _refresh_dimension_caches(self):
        """Pre-load surrogate keys for dim_hotel, dim_guest, dim_room."""
        try:
            tbl_htl = self.db.get_table_name("analytics_dw", "dim_hotel")
            df_h = self.db.fetch_dataframe(f"SELECT hotel_id, hotel_key FROM {tbl_htl}")
            self.hotel_cache = dict(zip(df_h["hotel_id"].str.strip(), df_h["hotel_key"]))

            tbl_gst = self.db.get_table_name("analytics_dw", "dim_guest")
            df_g = self.db.fetch_dataframe(f"SELECT guest_id, guest_key FROM {tbl_gst}")
            self.guest_cache = dict(zip(df_g["guest_id"].str.strip(), df_g["guest_key"]))

            tbl_rm = self.db.get_table_name("analytics_dw", "dim_room")
            df_r = self.db.fetch_dataframe(f"SELECT room_id, room_key FROM {tbl_rm}")
            self.room_cache = dict(zip(df_r["room_id"].str.strip(), df_r["room_key"]))

            logger.info(
                f"Dimension caches refreshed: {len(self.hotel_cache)} hotels, "
                f"{len(self.guest_cache)} guests, {len(self.room_cache)} rooms."
            )
        except Exception as exc:
            logger.warning(f"Could not load dimension caches: {exc}")

    # --------------------------------------------------------------------------
    # Watermark Management
    # --------------------------------------------------------------------------
    def get_watermark(self, source_name: str) -> Optional[datetime]:
        """Fetch the last successful watermark timestamp for a source."""
        tbl = self.db.get_table_name("metadata_audit", "etl_watermarks")
        sql = f"SELECT last_processed_timestamp FROM {tbl} WHERE source_name = :source"
        df = self.db.fetch_dataframe(sql, {"source": source_name})
        if not df.empty and df.iloc[0]["last_processed_timestamp"]:
            raw_ts = df.iloc[0]["last_processed_timestamp"]
            try:
                if isinstance(raw_ts, str):
                    clean_ts = raw_ts.replace("Z", "+00:00")
                    return datetime.fromisoformat(clean_ts)
                return raw_ts
            except Exception:
                return None
        return None

    def update_watermark(self, source_name: str, new_timestamp: datetime, batch_id: str):
        """Update watermark timestamp atomically after successful batch commit."""
        tbl = self.db.get_table_name("metadata_audit", "etl_watermarks")
        ts_iso = new_timestamp.isoformat()
        now_iso = datetime.now(timezone.utc).isoformat()

        if self.db.is_sqlite:
            sql = f"""
                INSERT INTO {tbl} (source_name, last_processed_timestamp, batch_id, updated_at)
                VALUES (:source_name, :ts, :batch_id, :updated_at)
                ON CONFLICT(source_name) DO UPDATE SET
                    last_processed_timestamp = excluded.last_processed_timestamp,
                    batch_id = excluded.batch_id,
                    updated_at = excluded.updated_at;
            """
        else:
            sql = f"""
                INSERT INTO {tbl} (source_name, last_processed_timestamp, batch_id, updated_at)
                VALUES (:source_name, :ts, :batch_id, :updated_at)
                ON CONFLICT (source_name) DO UPDATE SET
                    last_processed_timestamp = EXCLUDED.last_processed_timestamp,
                    batch_id = EXCLUDED.batch_id,
                    updated_at = EXCLUDED.updated_at;
            """
        self.db.execute_sql(sql, {
            "source_name": source_name,
            "ts": ts_iso,
            "batch_id": batch_id,
            "updated_at": now_iso
        })
        logger.info(f"Updated watermark for '{source_name}' to {ts_iso} (Batch: {batch_id})")

    # --------------------------------------------------------------------------
    # Dimensions Loading & Upsert
    # --------------------------------------------------------------------------
    def upsert_guests(self, df_guests: pd.DataFrame, batch_id: str) -> int:
        """Upsert unique guest dimension records using chunked executemany."""
        if df_guests.empty:
            return 0

        tbl = self.db.get_table_name("analytics_dw", "dim_guest")
        now_iso = datetime.now(timezone.utc).isoformat()
        records = []

        for _, r in df_guests.iterrows():
            g_id = str(r["guest_id"]).strip()
            f_name = str(r.get("first_name", "")).strip()
            l_name = str(r.get("last_name", "")).strip()
            full_name = str(r.get("full_name") or f"{f_name} {l_name}").strip()
            email = str(r.get("email", "")).strip()
            phone = str(r.get("phone", "")).strip()
            city = str(r.get("city", "Guwahati")).strip()
            state = str(r.get("state", "Assam")).strip()
            country = str(r.get("country", "India")).strip()

            records.append({
                "guest_id": g_id,
                "first_name": f_name,
                "last_name": l_name,
                "full_name": full_name,
                "email": email,
                "phone": phone,
                "city": city,
                "state": state,
                "country": country,
                "created_at": now_iso,
                "updated_at": now_iso
            })

        if self.db.is_sqlite:
            sql = f"""
                INSERT INTO {tbl} (guest_id, first_name, last_name, full_name, email, phone, city, state, country, created_at, updated_at)
                VALUES (:guest_id, :first_name, :last_name, :full_name, :email, :phone, :city, :state, :country, :created_at, :updated_at)
                ON CONFLICT(guest_id) DO UPDATE SET
                    first_name = excluded.first_name,
                    last_name = excluded.last_name,
                    full_name = excluded.full_name,
                    email = excluded.email,
                    phone = excluded.phone,
                    city = excluded.city,
                    state = excluded.state,
                    updated_at = excluded.updated_at;
            """
        else:
            sql = f"""
                INSERT INTO {tbl} (guest_id, first_name, last_name, full_name, email, phone, city, state, country, created_at, updated_at)
                VALUES (:guest_id, :first_name, :last_name, :full_name, :email, :phone, :city, :state, :country, :created_at, :updated_at)
                ON CONFLICT (guest_id) DO UPDATE SET
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    full_name = EXCLUDED.full_name,
                    email = EXCLUDED.email,
                    phone = EXCLUDED.phone,
                    city = EXCLUDED.city,
                    state = EXCLUDED.state,
                    updated_at = EXCLUDED.updated_at;
            """

        chunksize = 2000
        stmt = text(sql)
        for i in range(0, len(records), chunksize):
            chunk = records[i:i + chunksize]
            with self.db.transaction() as conn:
                conn.execute(stmt, chunk)

        self._refresh_dimension_caches()
        return len(records)

    def upsert_rooms(self, df_rooms: pd.DataFrame) -> int:
        """Upsert rooms dimension records using chunked executemany."""
        if df_rooms.empty:
            return 0

        tbl = self.db.get_table_name("analytics_dw", "dim_room")
        now_iso = datetime.now(timezone.utc).isoformat()
        records = []

        for _, r in df_rooms.iterrows():
            r_id = str(r["room_id"]).strip()
            h_id = str(r["hotel_id"]).strip()
            c_type = str(r.get("canonical_room_type", "Standard Double")).strip()
            cap = int(r.get("standard_capacity", 2))
            rate = float(r.get("base_rate_inr", 3500.0))

            records.append({
                "room_id": r_id,
                "hotel_id": h_id,
                "canonical_room_type": c_type,
                "standard_capacity": cap,
                "base_rate_inr": rate,
                "created_at": now_iso,
                "updated_at": now_iso
            })

        if self.db.is_sqlite:
            sql = f"""
                INSERT INTO {tbl} (room_id, hotel_id, canonical_room_type, standard_capacity, base_rate_inr, created_at, updated_at)
                VALUES (:room_id, :hotel_id, :canonical_room_type, :standard_capacity, :base_rate_inr, :created_at, :updated_at)
                ON CONFLICT(room_id) DO UPDATE SET
                    canonical_room_type = excluded.canonical_room_type,
                    standard_capacity = excluded.standard_capacity,
                    base_rate_inr = excluded.base_rate_inr,
                    updated_at = excluded.updated_at;
            """
        else:
            sql = f"""
                INSERT INTO {tbl} (room_id, hotel_id, canonical_room_type, standard_capacity, base_rate_inr, created_at, updated_at)
                VALUES (:room_id, :hotel_id, :canonical_room_type, :standard_capacity, :base_rate_inr, :created_at, :updated_at)
                ON CONFLICT (room_id) DO UPDATE SET
                    canonical_room_type = EXCLUDED.canonical_room_type,
                    standard_capacity = EXCLUDED.standard_capacity,
                    base_rate_inr = EXCLUDED.base_rate_inr,
                    updated_at = EXCLUDED.updated_at;
            """

        stmt = text(sql)
        chunksize = 2000
        for i in range(0, len(records), chunksize):
            chunk = records[i:i + chunksize]
            with self.db.transaction() as conn:
                conn.execute(stmt, chunk)

        self._refresh_dimension_caches()
        return len(records)

    # --------------------------------------------------------------------------
    # Facts Loading & Idempotent UPSERT
    # --------------------------------------------------------------------------
    def upsert_fact_bookings(self, df_bookings: pd.DataFrame, batch_id: str) -> int:
        """
        Idempotent UPSERT into analytics_dw.fact_bookings on unique business key booking_line_id
        using high-performance chunked executemany.
        """
        if df_bookings.empty:
            return 0

        self._refresh_dimension_caches()

        # Vectorized check for un-cached rooms
        booking_room_ids = set(df_bookings["room_id"].dropna().astype(str).str.strip())
        missing_room_ids = booking_room_ids - set(self.room_cache.keys())
        if missing_room_ids:
            df_missing = df_bookings[df_bookings["room_id"].isin(missing_room_ids)][
                ["room_id", "hotel_id", "canonical_room_type", "standard_capacity", "room_rate_inr"]
            ].drop_duplicates(subset=["room_id"])
            df_missing_rooms = pd.DataFrame({
                "room_id": df_missing["room_id"],
                "hotel_id": df_missing["hotel_id"],
                "canonical_room_type": df_missing["canonical_room_type"],
                "standard_capacity": df_missing["standard_capacity"],
                "base_rate_inr": df_missing["room_rate_inr"].fillna(3500.0)
            })
            self.upsert_rooms(df_missing_rooms)
            self._refresh_dimension_caches()

        tbl = self.db.get_table_name("analytics_dw", "fact_bookings")
        now_iso = datetime.now(timezone.utc).isoformat()

        # High-performance dictionary comprehension
        records_to_upsert = []
        hotel_cache = self.hotel_cache
        guest_cache = self.guest_cache
        room_cache = self.room_cache

        records_dict = df_bookings.to_dict(orient="records")
        for r in records_dict:
            b_id = str(r.get("booking_id", ""))
            b_line_id = str(r.get("booking_line_id") or f"{b_id}-01").strip()
            h_id = str(r.get("hotel_id", "")).strip()
            g_id = str(r.get("canonical_guest_id") or r.get("guest_id", "")).strip()
            r_id = str(r.get("room_id", "")).strip()

            h_key = hotel_cache.get(h_id, 1)
            g_key = guest_cache.get(g_id, 1)
            r_key = room_cache.get(r_id, 1)

            b_raw = r.get("booking_date_key")
            ci_raw = r.get("check_in_date_key")
            co_raw = r.get("check_out_date_key")

            b_date_key = int(b_raw) if b_raw is not None and not pd.isna(b_raw) else 20260101
            ci_date_key = int(ci_raw) if ci_raw is not None and not pd.isna(ci_raw) else 20260101
            co_date_key = int(co_raw) if co_raw is not None and not pd.isna(co_raw) else 20260102

            records_to_upsert.append({
                "booking_id": b_id,
                "booking_line_id": b_line_id,
                "hotel_key": h_key,
                "guest_key": g_key,
                "room_key": r_key,
                "booking_date_key": b_date_key,
                "check_in_date_key": ci_date_key,
                "check_out_date_key": co_date_key,
                "booking_status": str(r.get("booking_status", "CONFIRMED")),
                "original_rate": float(r.get("room_rate", 0.0) or 0.0),
                "original_currency": str(r.get("currency", "INR")),
                "exchange_rate_used": float(r.get("exchange_rate_used", 1.0) or 1.0),
                "room_rate_inr": float(r.get("room_rate_inr", 0.0) or 0.0),
                "total_amount_inr": float(r.get("total_amount_inr", 0.0) or 0.0),
                "number_of_guests": int(r.get("number_of_guests", 1) or 1),
                "lead_time_days": int(r.get("lead_time_days", 0) or 0),
                "length_of_stay_days": int(r.get("length_of_stay_days", 1) or 1),
                "source_updated_at": str(r.get("source_updated_at", now_iso)),
                "dw_batch_id": batch_id,
                "dw_loaded_at": now_iso
            })

        if self.db.is_sqlite:
            sql = f"""
                INSERT INTO {tbl} (
                    booking_id, booking_line_id, hotel_key, guest_key, room_key,
                    booking_date_key, check_in_date_key, check_out_date_key,
                    booking_status, original_rate, original_currency, exchange_rate_used,
                    room_rate_inr, total_amount_inr, number_of_guests, lead_time_days,
                    length_of_stay_days, source_updated_at, dw_batch_id, dw_loaded_at
                ) VALUES (
                    :booking_id, :booking_line_id, :hotel_key, :guest_key, :room_key,
                    :booking_date_key, :check_in_date_key, :check_out_date_key,
                    :booking_status, :original_rate, :original_currency, :exchange_rate_used,
                    :room_rate_inr, :total_amount_inr, :number_of_guests, :lead_time_days,
                    :length_of_stay_days, :source_updated_at, :dw_batch_id, :dw_loaded_at
                )
                ON CONFLICT(booking_line_id) DO UPDATE SET
                    hotel_key = excluded.hotel_key,
                    guest_key = excluded.guest_key,
                    room_key = excluded.room_key,
                    booking_date_key = excluded.booking_date_key,
                    check_in_date_key = excluded.check_in_date_key,
                    check_out_date_key = excluded.check_out_date_key,
                    booking_status = excluded.booking_status,
                    original_rate = excluded.original_rate,
                    original_currency = excluded.original_currency,
                    exchange_rate_used = excluded.exchange_rate_used,
                    room_rate_inr = excluded.room_rate_inr,
                    total_amount_inr = excluded.total_amount_inr,
                    number_of_guests = excluded.number_of_guests,
                    lead_time_days = excluded.lead_time_days,
                    length_of_stay_days = excluded.length_of_stay_days,
                    source_updated_at = excluded.source_updated_at,
                    dw_batch_id = excluded.dw_batch_id,
                    dw_loaded_at = excluded.dw_loaded_at;
            """
        else:
            sql = f"""
                INSERT INTO {tbl} (
                    booking_id, booking_line_id, hotel_key, guest_key, room_key,
                    booking_date_key, check_in_date_key, check_out_date_key,
                    booking_status, original_rate, original_currency, exchange_rate_used,
                    room_rate_inr, total_amount_inr, number_of_guests, lead_time_days,
                    length_of_stay_days, source_updated_at, dw_batch_id, dw_loaded_at
                ) VALUES (
                    :booking_id, :booking_line_id, :hotel_key, :guest_key, :room_key,
                    :booking_date_key, :check_in_date_key, :check_out_date_key,
                    :booking_status, :original_rate, :original_currency, :exchange_rate_used,
                    :room_rate_inr, :total_amount_inr, :number_of_guests, :lead_time_days,
                    :length_of_stay_days, :source_updated_at, :dw_batch_id, :dw_loaded_at
                )
                ON CONFLICT (booking_line_id) DO UPDATE SET
                    hotel_key = EXCLUDED.hotel_key,
                    guest_key = EXCLUDED.guest_key,
                    room_key = EXCLUDED.room_key,
                    booking_date_key = EXCLUDED.booking_date_key,
                    check_in_date_key = EXCLUDED.check_in_date_key,
                    check_out_date_key = EXCLUDED.check_out_date_key,
                    booking_status = EXCLUDED.booking_status,
                    original_rate = EXCLUDED.original_rate,
                    original_currency = EXCLUDED.original_currency,
                    exchange_rate_used = EXCLUDED.exchange_rate_used,
                    room_rate_inr = EXCLUDED.room_rate_inr,
                    total_amount_inr = EXCLUDED.total_amount_inr,
                    number_of_guests = EXCLUDED.number_of_guests,
                    lead_time_days = EXCLUDED.lead_time_days,
                    length_of_stay_days = EXCLUDED.length_of_stay_days,
                    source_updated_at = EXCLUDED.source_updated_at,
                    dw_batch_id = EXCLUDED.dw_batch_id,
                    dw_loaded_at = EXCLUDED.dw_loaded_at;
            """

        chunksize = 2000
        stmt = text(sql)
        for i in range(0, len(records_to_upsert), chunksize):
            chunk = records_to_upsert[i:i + chunksize]
            with self.db.transaction() as conn:
                conn.execute(stmt, chunk)

        logger.info(f"Loaded {len(records_to_upsert)} fact_bookings records via idempotent UPSERT (Batch: {batch_id})")
        return len(records_to_upsert)

    def upsert_fact_stays(self, df_stays: pd.DataFrame, batch_id: str) -> int:
        """Idempotent UPSERT into analytics_dw.fact_stays using chunked executemany."""
        if df_stays.empty:
            return 0

        self._refresh_dimension_caches()
        tbl = self.db.get_table_name("analytics_dw", "fact_stays")
        now_iso = datetime.now(timezone.utc).isoformat()

        records_to_upsert = []
        hotel_cache = self.hotel_cache
        guest_cache = self.guest_cache
        room_cache = self.room_cache

        records_dict = df_stays.to_dict(orient="records")
        for r in records_dict:
            s_id = str(r["stay_id"]).strip()
            b_id = str(r.get("booking_id", "")).strip()
            h_id = str(r.get("hotel_id", "")).strip()
            g_id = str(r.get("guest_id", "")).strip()
            r_id = str(r.get("room_id", "")).strip()

            h_key = hotel_cache.get(h_id, 1)
            g_key = guest_cache.get(g_id, 1)
            r_key = room_cache.get(r_id, 1)

            ci_raw = r.get("check_in_date_key")
            co_raw = r.get("check_out_date_key")
            ci_key = int(ci_raw) if ci_raw is not None and not pd.isna(ci_raw) else 20260101
            co_key = int(co_raw) if co_raw is not None and not pd.isna(co_raw) else None

            dur_raw = r.get("actual_duration_hours")
            dur = float(dur_raw) if dur_raw is not None and not pd.isna(dur_raw) else 0.0

            records_to_upsert.append({
                "stay_id": s_id,
                "booking_id": b_id,
                "hotel_key": h_key,
                "guest_key": g_key,
                "room_key": r_key,
                "check_in_date_key": ci_key,
                "check_out_date_key": co_key,
                "stay_status": str(r.get("stay_status", "COMPLETED")),
                "actual_check_in": str(r.get("actual_check_in") or ""),
                "actual_check_out": str(r.get("actual_check_out") or ""),
                "actual_duration_hours": dur,
                "dw_batch_id": batch_id,
                "dw_loaded_at": now_iso
            })

        if self.db.is_sqlite:
            sql = f"""
                INSERT INTO {tbl} (
                    stay_id, booking_id, hotel_key, guest_key, room_key,
                    check_in_date_key, check_out_date_key, stay_status,
                    actual_check_in, actual_check_out, actual_duration_hours,
                    dw_batch_id, dw_loaded_at
                ) VALUES (
                    :stay_id, :booking_id, :hotel_key, :guest_key, :room_key,
                    :check_in_date_key, :check_out_date_key, :stay_status,
                    :actual_check_in, :actual_check_out, :actual_duration_hours,
                    :dw_batch_id, :dw_loaded_at
                )
                ON CONFLICT(stay_id) DO UPDATE SET
                    stay_status = excluded.stay_status,
                    actual_check_in = excluded.actual_check_in,
                    actual_check_out = excluded.actual_check_out,
                    actual_duration_hours = excluded.actual_duration_hours,
                    dw_batch_id = excluded.dw_batch_id,
                    dw_loaded_at = excluded.dw_loaded_at;
            """
        else:
            sql = f"""
                INSERT INTO {tbl} (
                    stay_id, booking_id, hotel_key, guest_key, room_key,
                    check_in_date_key, check_out_date_key, stay_status,
                    actual_check_in, actual_check_out, actual_duration_hours,
                    dw_batch_id, dw_loaded_at
                ) VALUES (
                    :stay_id, :booking_id, :hotel_key, :guest_key, :room_key,
                    :check_in_date_key, :check_out_date_key, :stay_status,
                    :actual_check_in, :actual_check_out, :actual_duration_hours,
                    :dw_batch_id, :dw_loaded_at
                )
                ON CONFLICT (stay_id) DO UPDATE SET
                    stay_status = EXCLUDED.stay_status,
                    actual_check_in = EXCLUDED.actual_check_in,
                    actual_check_out = EXCLUDED.actual_check_out,
                    actual_duration_hours = EXCLUDED.actual_duration_hours,
                    dw_batch_id = EXCLUDED.dw_batch_id,
                    dw_loaded_at = EXCLUDED.dw_loaded_at;
            """

        chunksize = 2000
        stmt = text(sql)
        for i in range(0, len(records_to_upsert), chunksize):
            chunk = records_to_upsert[i:i + chunksize]
            with self.db.transaction() as conn:
                conn.execute(stmt, chunk)

        logger.info(f"Loaded {len(records_to_upsert)} fact_stays records via idempotent UPSERT (Batch: {batch_id})")
        return len(records_to_upsert)

    def upsert_fact_reviews(self, df_reviews: pd.DataFrame, batch_id: str) -> int:
        """Idempotent UPSERT into analytics_dw.fact_reviews using chunked executemany."""
        if df_reviews.empty:
            return 0

        self._refresh_dimension_caches()
        tbl = self.db.get_table_name("analytics_dw", "fact_reviews")
        now_iso = datetime.now(timezone.utc).isoformat()

        records_to_upsert = []
        hotel_cache = self.hotel_cache
        guest_cache = self.guest_cache

        records_dict = df_reviews.to_dict(orient="records")
        for r in records_dict:
            rev_id = str(r["review_id"]).strip()
            b_id = str(r.get("booking_id", "")).strip()
            h_id = str(r.get("hotel_id", "")).strip()
            g_id = str(r.get("guest_id", "")).strip()

            h_key = hotel_cache.get(h_id, 1)
            g_key = guest_cache.get(g_id, 1)
            rev_date_key = int(r.get("review_date_key") or 20260101)

            records_to_upsert.append({
                "review_id": rev_id,
                "booking_id": b_id,
                "hotel_key": h_key,
                "guest_key": g_key,
                "review_date_key": rev_date_key,
                "rating": int(r.get("rating", 5)),
                "review_text": str(r.get("review_text", "")),
                "sentiment_category": str(r.get("sentiment_category", "NEUTRAL")),
                "dw_batch_id": batch_id,
                "dw_loaded_at": now_iso
            })

        if self.db.is_sqlite:
            sql = f"""
                INSERT INTO {tbl} (
                    review_id, booking_id, hotel_key, guest_key,
                    review_date_key, rating, review_text, sentiment_category,
                    dw_batch_id, dw_loaded_at
                ) VALUES (
                    :review_id, :booking_id, :hotel_key, :guest_key,
                    :review_date_key, :rating, :review_text, :sentiment_category,
                    :dw_batch_id, :dw_loaded_at
                )
                ON CONFLICT(review_id) DO UPDATE SET
                    rating = excluded.rating,
                    review_text = excluded.review_text,
                    sentiment_category = excluded.sentiment_category,
                    dw_batch_id = excluded.dw_batch_id,
                    dw_loaded_at = excluded.dw_loaded_at;
            """
        else:
            sql = f"""
                INSERT INTO {tbl} (
                    review_id, booking_id, hotel_key, guest_key,
                    review_date_key, rating, review_text, sentiment_category,
                    dw_batch_id, dw_loaded_at
                ) VALUES (
                    :review_id, :booking_id, :hotel_key, :guest_key,
                    :review_date_key, :rating, :review_text, :sentiment_category,
                    :dw_batch_id, :dw_loaded_at
                )
                ON CONFLICT (review_id) DO UPDATE SET
                    rating = EXCLUDED.rating,
                    review_text = EXCLUDED.review_text,
                    sentiment_category = EXCLUDED.sentiment_category,
                    dw_batch_id = EXCLUDED.dw_batch_id,
                    dw_loaded_at = EXCLUDED.dw_loaded_at;
            """

        chunksize = 2000
        stmt = text(sql)
        for i in range(0, len(records_to_upsert), chunksize):
            chunk = records_to_upsert[i:i + chunksize]
            with self.db.transaction() as conn:
                conn.execute(stmt, chunk)

        logger.info(f"Loaded {len(records_to_upsert)} fact_reviews records via idempotent UPSERT (Batch: {batch_id})")
        return len(records_to_upsert)
