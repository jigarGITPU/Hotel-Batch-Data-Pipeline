"""
Room Type Normalization module for Hotel Batch Data Pipeline.
Standardizes inconsistent hotel room descriptions into canonical types and capacities.
"""

from typing import Dict, Any, Optional, Tuple
import pandas as pd

from src.utils.database import DatabaseManager, db
from src.monitoring.logger import get_logger

logger = get_logger("room_mapper")


class RoomTypeMapper:
    """Standardizes disparate room type descriptions to canonical room types."""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.db = db_manager or db
        self.mapping_dict: Dict[str, Dict[str, Any]] = self._load_mappings()

    def _load_mappings(self) -> Dict[str, Dict[str, Any]]:
        """Load mapping definitions from room_type_mapping table."""
        tbl = self.db.get_table_name("analytics_dw", "room_type_mapping")
        try:
            df = self.db.fetch_dataframe(f"SELECT source_room_type, canonical_room_type, standard_capacity, default_rate_inr FROM {tbl}")
            mappings = {}
            for _, r in df.iterrows():
                key = str(r["source_room_type"]).strip().lower()
                mappings[key] = {
                    "canonical_room_type": r["canonical_room_type"],
                    "standard_capacity": int(r["standard_capacity"]),
                    "default_rate_inr": float(r["default_rate_inr"])
                }
            logger.info(f"Loaded {len(mappings)} room type mappings from {tbl}")
            return mappings
        except Exception as exc:
            logger.warning(f"Could not load room type mappings ({exc}). Using static fallback.")
            return {
                "deluxe king": {"canonical_room_type": "Deluxe King", "standard_capacity": 2, "default_rate_inr": 5500.0},
                "standard single": {"canonical_room_type": "Standard Single", "standard_capacity": 1, "default_rate_inr": 2500.0},
                "standard double": {"canonical_room_type": "Standard Double", "standard_capacity": 2, "default_rate_inr": 3500.0},
            }

    def map_room_type(self, source_desc: str) -> Tuple[Optional[str], Optional[int], bool]:
        """
        Map a source room type to canonical representation.

        Returns:
            Tuple of (canonical_room_type, standard_capacity, is_mapped)
        """
        if not source_desc or pd.isna(source_desc):
            return None, None, False

        clean_desc = str(source_desc).strip().lower()
        if clean_desc in self.mapping_dict:
            entry = self.mapping_dict[clean_desc]
            return entry["canonical_room_type"], entry["standard_capacity"], True

        # Fuzzy / fallback keyword matching
        if "presidential" in clean_desc or "pres" in clean_desc:
            return "Presidential Suite", 4, True
        if "exec" in clean_desc or "suite" in clean_desc:
            return "Executive Suite", 4, True
        if "deluxe" in clean_desc and "king" in clean_desc:
            return "Deluxe King", 2, True
        if "deluxe" in clean_desc and "queen" in clean_desc:
            return "Deluxe Queen", 2, True
        if "single" in clean_desc or "sgl" in clean_desc:
            return "Standard Single", 1, True
        if "double" in clean_desc or "dbl" in clean_desc:
            return "Standard Double", 2, True

        return None, None, False
