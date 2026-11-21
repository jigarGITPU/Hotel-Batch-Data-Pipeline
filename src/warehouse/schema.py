"""
Warehouse schema initialization and reference data seeding.
Executes DDL scripts and seeds dim_date, dim_currency, room_type_mapping, and dim_hotel.
"""

from datetime import date, timedelta
from pathlib import Path
from typing import Optional
import pandas as pd
from sqlalchemy import text

from src.utils.config import config
from src.utils.database import DatabaseManager, db
from src.monitoring.logger import get_logger

logger = get_logger("warehouse_schema")


def initialize_warehouse(db_manager: Optional[DatabaseManager] = None):
    """Execute all SQL DDL scripts and initialize reference dimensions."""
    mgr = db_manager or db
    sql_dir = config.PIPELINE.sql_dir

    script_order = [
        "01_create_schemas.sql",
        "02_create_staging.sql",
        "03_create_dimensions.sql",
        "04_create_facts.sql",
        "05_create_metadata.sql",
        "06_create_indexes.sql"
    ]

    logger.info("Initializing database schemas, tables, and indexes...")
    for script_name in script_order:
        script_path = sql_dir / script_name
        mgr.execute_script(script_path)

    # Seed reference dimensions
    seed_currencies(mgr)
    seed_room_type_mappings(mgr)
    seed_hotels(mgr)
    seed_date_dimension(mgr, start_date=date(2025, 1, 1), end_date=date(2027, 12, 31))
    logger.info("Warehouse initialization and reference data seeding completed successfully.")


def seed_currencies(mgr: DatabaseManager):
    """Seed supported currency exchange rates against base currency INR."""
    tbl = mgr.get_table_name("analytics_dw", "dim_currency")
    try:
        existing = mgr.fetch_dataframe(f"SELECT COUNT(*) as cnt FROM {tbl}")
        if int(existing.iloc[0]["cnt"]) > 0:
            return
    except Exception:
        pass

    currencies = [
        {"currency_code": "INR", "currency_name": "Indian Rupee", "exchange_rate_to_inr": 1.0000, "effective_date": "2026-01-01", "source": "RBI_DEMO_REFERENCE"},
        {"currency_code": "USD", "currency_name": "US Dollar", "exchange_rate_to_inr": 86.5000, "effective_date": "2026-01-01", "source": "RBI_DEMO_REFERENCE"},
        {"currency_code": "EUR", "currency_name": "Euro", "exchange_rate_to_inr": 92.3000, "effective_date": "2026-01-01", "source": "ECB_DEMO_REFERENCE"},
        {"currency_code": "GBP", "currency_name": "British Pound", "exchange_rate_to_inr": 108.5000, "effective_date": "2026-01-01", "source": "BOE_DEMO_REFERENCE"},
    ]
    df = pd.DataFrame(currencies)
    mgr.bulk_insert(df, "analytics_dw", "dim_currency", if_exists="append")
    logger.info(f"Seeded {len(df)} currencies into {tbl}")


def seed_room_type_mappings(mgr: DatabaseManager):
    """Seed room type mappings to standardize messy source representations."""
    tbl = mgr.get_table_name("analytics_dw", "room_type_mapping")
    try:
        existing = mgr.fetch_dataframe(f"SELECT COUNT(*) as cnt FROM {tbl}")
        if int(existing.iloc[0]["cnt"]) > 0:
            return
    except Exception:
        pass

    mappings = [
        # Standard Single
        ("Standard Single", "Standard Single", 1, 2500.00),
        ("standard single", "Standard Single", 1, 2500.00),
        ("STD_SGL", "Standard Single", 1, 2500.00),
        ("Std Single", "Standard Single", 1, 2500.00),
        ("Standard-Single", "Standard Single", 1, 2500.00),
        # Standard Double
        ("Standard Double", "Standard Double", 2, 3500.00),
        ("standard double", "Standard Double", 2, 3500.00),
        ("STD_DBL", "Standard Double", 2, 3500.00),
        ("Std Double", "Standard Double", 2, 3500.00),
        ("Standard-Double", "Standard Double", 2, 3500.00),
        # Deluxe King
        ("Deluxe King", "Deluxe King", 2, 5500.00),
        ("deluxe king", "Deluxe King", 2, 5500.00),
        ("King Deluxe", "Deluxe King", 2, 5500.00),
        ("DELUXE KING", "Deluxe King", 2, 5500.00),
        ("Deluxe-King", "Deluxe King", 2, 5500.00),
        ("DLX_KNG", "Deluxe King", 2, 5500.00),
        # Deluxe Queen
        ("Deluxe Queen", "Deluxe Queen", 2, 5000.00),
        ("deluxe queen", "Deluxe Queen", 2, 5000.00),
        ("Queen Deluxe", "Deluxe Queen", 2, 5000.00),
        ("DELUXE QUEEN", "Deluxe Queen", 2, 5000.00),
        ("Deluxe-Queen", "Deluxe Queen", 2, 5000.00),
        ("DLX_QUN", "Deluxe Queen", 2, 5000.00),
        # Executive Suite
        ("Executive Suite", "Executive Suite", 4, 8500.00),
        ("Exec Suite", "Executive Suite", 4, 8500.00),
        ("EXEC_SUITE", "Executive Suite", 4, 8500.00),
        ("executive suite", "Executive Suite", 4, 8500.00),
        ("Executive-Suite", "Executive Suite", 4, 8500.00),
        # Presidential Suite
        ("Presidential Suite", "Presidential Suite", 4, 18000.00),
        ("Pres Suite", "Presidential Suite", 4, 18000.00),
        ("PRES_SUITE", "Presidential Suite", 4, 18000.00),
        ("presidential suite", "Presidential Suite", 4, 18000.00),
        ("Presidential-Suite", "Presidential Suite", 4, 18000.00),
    ]
    df = pd.DataFrame(mappings, columns=["source_room_type", "canonical_room_type", "standard_capacity", "default_rate_inr"])
    mgr.bulk_insert(df, "analytics_dw", "room_type_mapping", if_exists="append")
    logger.info(f"Seeded {len(df)} room type mappings into {tbl}")


def seed_hotels(mgr: DatabaseManager):
    """Seed the 14 Guwahati hotel properties into dim_hotel."""
    tbl = mgr.get_table_name("analytics_dw", "dim_hotel")
    try:
        existing = mgr.fetch_dataframe(f"SELECT COUNT(*) as cnt FROM {tbl}")
        if int(existing.iloc[0]["cnt"]) > 0:
            return
    except Exception:
        pass

    hotels = [
        ("HTL-GAU-001", "Brahmaputra Grand Palace", "Pan Bazaar, Guwahati", "5-Star", 120),
        ("HTL-GAU-002", "Radisson Blu Hotel Guwahati", "Gotanagar, Guwahati", "5-Star", 196),
        ("HTL-GAU-003", "Vivanta Guwahati", "Khanapara, Guwahati", "5-Star", 150),
        ("HTL-GAU-004", "Kaziranga Orchid Resort", "Dispur, Guwahati", "4-Star", 85),
        ("HTL-GAU-005", "Kamakhya Heritage Inn", "Kamakhya Hill, Guwahati", "3-Star", 45),
        ("HTL-GAU-006", "Umananda Island View Resort", "Uzan Bazaar, Guwahati", "4-Star", 60),
        ("HTL-GAU-007", "Saraighat Riverfront Hotel", "Bharalumukh, Guwahati", "3-Star", 50),
        ("HTL-GAU-008", "Nilachal Gateway Hotel", "Maligaon, Guwahati", "3-Star", 40),
        ("HTL-GAU-009", "Assam Valley Luxury Suites", "GS Road, Guwahati", "4-Star", 95),
        ("HTL-GAU-010", "Deepor Bil Eco Lodge", "Rani, Guwahati", "Resort", 35),
        ("HTL-GAU-011", "Dighalipukhuri Regency", "Ambari, Guwahati", "3-Star", 55),
        ("HTL-GAU-012", "Greenwood Resort Guwahati", "Madhabpur, Guwahati", "4-Star", 110),
        ("HTL-GAU-013", "Dynasty Central Hotel", "Paltan Bazaar, Guwahati", "4-Star", 75),
        ("HTL-GAU-014", "Brahmaputra View Riverside Lodge", "Machkhowa, Guwahati", "3-Star", 30),
    ]
    df = pd.DataFrame(hotels, columns=["hotel_id", "hotel_name", "location", "rating_category", "total_rooms"])
    df["is_active"] = True
    mgr.bulk_insert(df, "analytics_dw", "dim_hotel", if_exists="append")
    logger.info(f"Seeded {len(df)} hotels into {tbl}")


def seed_date_dimension(mgr: DatabaseManager, start_date: date, end_date: date):
    """Seed date dimension for the warehouse date range."""
    tbl = mgr.get_table_name("analytics_dw", "dim_date")
    try:
        existing = mgr.fetch_dataframe(f"SELECT COUNT(*) as cnt FROM {tbl}")
        if int(existing.iloc[0]["cnt"]) > 0:
            return
    except Exception:
        pass

    rows = []
    curr = start_date
    while curr <= end_date:
        date_key = int(curr.strftime("%Y%m%d"))
        rows.append({
            "date_key": date_key,
            "full_date": curr.isoformat(),
            "day_of_week": curr.isoweekday(),
            "day_name": curr.strftime("%A"),
            "day_of_month": curr.day,
            "day_of_year": curr.timetuple().tm_yday,
            "week_of_year": curr.isocalendar()[1],
            "month_num": curr.month,
            "month_name": curr.strftime("%B"),
            "quarter": (curr.month - 1) // 3 + 1,
            "year_num": curr.year,
            "is_weekend": curr.isoweekday() in [6, 7]
        })
        curr += timedelta(days=1)

    df = pd.DataFrame(rows)
    mgr.bulk_insert(df, "analytics_dw", "dim_date", if_exists="append", chunksize=1000)
    logger.info(f"Seeded {len(df)} calendar dates into {tbl}")
