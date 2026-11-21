"""
Guest Identity Deduplication module for Hotel Batch Data Pipeline.
Identifies and resolves duplicate guest profiles using verified business keys (Email, Phone)
without conflating different guests with identical names.
"""

import re
from typing import Dict, Any, Optional, List, Tuple
import pandas as pd

from src.utils.database import DatabaseManager, db
from src.monitoring.logger import get_logger

logger = get_logger("guest_deduplicator")


class GuestDeduplicator:
    """Manages guest identity resolution and mapping to canonical identities."""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.db = db_manager or db
        # In-memory lookup tables: email -> canonical_id, phone -> canonical_id, source_id -> canonical_id
        self.email_to_canonical: Dict[str, str] = {}
        self.phone_to_canonical: Dict[str, str] = {}
        self.source_to_canonical: Dict[str, str] = {}
        self._load_existing_mappings()

    def _clean_email(self, email: Any) -> Optional[str]:
        if not email or pd.isna(email):
            return None
        cleaned = str(email).strip().lower()
        return cleaned if "@" in cleaned and "." in cleaned else None

    def _clean_phone(self, phone: Any) -> Optional[str]:
        if not phone or pd.isna(phone):
            return None
        # Extract digits only
        digits = re.sub(r"\D", "", str(phone))
        # Keep last 10 digits for Indian standard phone numbers
        return digits[-10:] if len(digits) >= 10 else None

    def _load_existing_mappings(self):
        """Load persistent mappings from analytics_dw.guest_identity_mapping."""
        tbl = self.db.get_table_name("analytics_dw", "guest_identity_mapping")
        try:
            df = self.db.fetch_dataframe(f"SELECT source_guest_id, canonical_guest_id FROM {tbl}")
            for _, r in df.iterrows():
                self.source_to_canonical[r["source_guest_id"]] = r["canonical_guest_id"]
            logger.info(f"Loaded {len(self.source_to_canonical)} existing guest identity mappings.")
        except Exception as exc:
            logger.warning(f"Could not load existing guest identity mappings: {exc}")

    def deduplicate_guests(self, df_guests: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Resolve guest identities from raw guest records or booking guest attributes.

        Returns:
            Tuple of (df_canonical_guests, df_mappings_to_persist)
        """
        if df_guests.empty:
            return pd.DataFrame(), pd.DataFrame()

        mappings_to_save: List[Dict[str, Any]] = []
        canonical_profiles: Dict[str, Dict[str, Any]] = {}

        for _, row in df_guests.iterrows():
            source_id = str(row.get("guest_id", "")).strip()
            if not source_id or source_id.upper() in ["NONE", "NAN", "NULL", ""]:
                continue

            # Check if source_id already known
            if source_id in self.source_to_canonical:
                canonical_id = self.source_to_canonical[source_id]
                match_rule = "EXISTING_MAP"
                confidence = 1.00
            else:
                email = self._clean_email(row.get("email") or row.get("guest_email"))
                phone = self._clean_phone(row.get("phone") or row.get("guest_phone"))

                canonical_id = None
                match_rule = "EXACT_GUEST_ID"
                confidence = 1.00

                if email and email in self.email_to_canonical:
                    canonical_id = self.email_to_canonical[email]
                    match_rule = "EMAIL_MATCH"
                    confidence = 0.95
                elif phone and phone in self.phone_to_canonical:
                    canonical_id = self.phone_to_canonical[phone]
                    match_rule = "PHONE_MATCH"
                    confidence = 0.90
                else:
                    canonical_id = source_id

                # Update memory indexes
                if email and email not in self.email_to_canonical:
                    self.email_to_canonical[email] = canonical_id
                if phone and phone not in self.phone_to_canonical:
                    self.phone_to_canonical[phone] = canonical_id

                self.source_to_canonical[source_id] = canonical_id

                mappings_to_save.append({
                    "source_guest_id": source_id,
                    "canonical_guest_id": canonical_id,
                    "confidence_score": confidence,
                    "match_rule": match_rule
                })

            # Track canonical profile details
            if canonical_id not in canonical_profiles:
                first = str(row.get("first_name") or row.get("guest_first_name") or "").strip()
                last = str(row.get("last_name") or row.get("guest_last_name") or "").strip()
                full_name = f"{first} {last}".strip()
                canonical_profiles[canonical_id] = {
                    "guest_id": canonical_id,
                    "first_name": first,
                    "last_name": last,
                    "full_name": full_name,
                    "email": row.get("email") or row.get("guest_email") or "",
                    "phone": row.get("phone") or row.get("guest_phone") or "",
                    "city": row.get("city", "Guwahati"),
                    "state": row.get("state", "Assam"),
                    "country": row.get("country", "India")
                }

        # Persist new mappings into database
        df_mappings = pd.DataFrame(mappings_to_save)
        if not df_mappings.empty:
            self.db.bulk_insert(
                df=df_mappings,
                schema="analytics_dw",
                table="guest_identity_mapping",
                if_exists="append"
            )
            logger.info(f"Persisted {len(df_mappings)} new guest identity mappings.")

        df_canonical = pd.DataFrame(list(canonical_profiles.values()))
        return df_canonical, df_mappings

    def get_canonical_id(self, source_guest_id: str) -> str:
        """Lookup canonical guest ID for a given source ID."""
        return self.source_to_canonical.get(source_guest_id, source_guest_id)
