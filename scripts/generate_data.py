"""
Synthetic Data Generator for Hotel Batch Data Pipeline.
Generates realistic, deterministic hotel booking, inventory, stay, and review records
across 14 fictional Guwahati hotel properties with controlled data quality anomalies.
"""

import os
import sys
import random
import argparse
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Any, Tuple
import pandas as pd
import numpy as np

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.config import config
from src.monitoring.logger import get_logger

logger = get_logger("data_generator")

# 14 Fictional Guwahati Hotels
HOTELS = [
    {"hotel_id": "HTL-GAU-001", "name": "Brahmaputra Grand Palace", "location": "Pan Bazaar, Guwahati", "rating_category": "5-Star", "total_rooms": 120},
    {"hotel_id": "HTL-GAU-002", "name": "Radisson Blu Hotel Guwahati", "location": "Gotanagar, Guwahati", "rating_category": "5-Star", "total_rooms": 196},
    {"hotel_id": "HTL-GAU-003", "name": "Vivanta Guwahati", "location": "Khanapara, Guwahati", "rating_category": "5-Star", "total_rooms": 150},
    {"hotel_id": "HTL-GAU-004", "name": "Kaziranga Orchid Resort", "location": "Dispur, Guwahati", "rating_category": "4-Star", "total_rooms": 85},
    {"hotel_id": "HTL-GAU-005", "name": "Kamakhya Heritage Inn", "location": "Kamakhya Hill, Guwahati", "rating_category": "3-Star", "total_rooms": 45},
    {"hotel_id": "HTL-GAU-006", "name": "Umananda Island View Resort", "location": "Uzan Bazaar, Guwahati", "rating_category": "4-Star", "total_rooms": 60},
    {"hotel_id": "HTL-GAU-007", "name": "Saraighat Riverfront Hotel", "location": "Bharalumukh, Guwahati", "rating_category": "3-Star", "total_rooms": 50},
    {"hotel_id": "HTL-GAU-008", "name": "Nilachal Gateway Hotel", "location": "Maligaon, Guwahati", "rating_category": "3-Star", "total_rooms": 40},
    {"hotel_id": "HTL-GAU-009", "name": "Assam Valley Luxury Suites", "location": "GS Road, Guwahati", "rating_category": "4-Star", "total_rooms": 95},
    {"hotel_id": "HTL-GAU-010", "name": "Deepor Bil Eco Lodge", "location": "Rani, Guwahati", "rating_category": "Resort", "total_rooms": 35},
    {"hotel_id": "HTL-GAU-011", "name": "Dighalipukhuri Regency", "location": "Ambari, Guwahati", "rating_category": "3-Star", "total_rooms": 55},
    {"hotel_id": "HTL-GAU-012", "name": "Greenwood Resort Guwahati", "location": "Madhabpur, Guwahati", "rating_category": "4-Star", "total_rooms": 110},
    {"hotel_id": "HTL-GAU-013", "name": "Dynasty Central Hotel", "location": "Paltan Bazaar, Guwahati", "rating_category": "4-Star", "total_rooms": 75},
    {"hotel_id": "HTL-GAU-014", "name": "Brahmaputra View Riverside Lodge", "location": "Machkhowa, Guwahati", "rating_category": "3-Star", "total_rooms": 30},
]

# Canonical Room Types & Base Rates in INR
CANONICAL_ROOM_TYPES = {
    "Standard Single": {"capacity": 1, "base_rate": 2500.0},
    "Standard Double": {"capacity": 2, "base_rate": 3500.0},
    "Deluxe King": {"capacity": 2, "base_rate": 5500.0},
    "Deluxe Queen": {"capacity": 2, "base_rate": 5000.0},
    "Executive Suite": {"capacity": 4, "base_rate": 8500.0},
    "Presidential Suite": {"capacity": 4, "base_rate": 18000.0},
}

# Inconsistent Room Type variations to test standardization
ROOM_TYPE_VARIATIONS = {
    "Standard Single": ["Standard Single", "standard single", "STD_SGL", "Std Single", "Standard-Single"],
    "Standard Double": ["Standard Double", "standard double", "STD_DBL", "Std Double", "Standard-Double"],
    "Deluxe King": ["Deluxe King", "deluxe king", "King Deluxe", "DELUXE KING", "Deluxe-King", "DLX_KNG"],
    "Deluxe Queen": ["Deluxe Queen", "deluxe queen", "Queen Deluxe", "DELUXE QUEEN", "Deluxe-Queen", "DLX_QUN"],
    "Executive Suite": ["Executive Suite", "Exec Suite", "EXEC_SUITE", "executive suite", "Executive-Suite"],
    "Presidential Suite": ["Presidential Suite", "Pres Suite", "PRES_SUITE", "presidential suite", "Presidential-Suite"],
}

FIRST_NAMES = [
    "Aarav", "Vivaan", "Aditya", "Vihaan", "Arjun", "Sai", "Reyansh", "Ayaan", "Krishna", "Ishaan",
    "Diya", "Saanvi", "Aanya", "Aadhya", "Pari", "Ananya", "Myra", "Riya", "Isha", "Kavya",
    "Rahul", "Rohan", "Pooja", "Priya", "Sneha", "Ankit", "Vikram", "Neha", "Deepak", "Sunita",
    "Bikash", "Deben", "Jitu", "Partha", "Dipen", "Runjun", "Himadri", "Barnali", "Mousumi", "Pranab"
]

LAST_NAMES = [
    "Sharma", "Verma", "Gupta", "Baruah", "Bora", "Kalita", "Sarmah", "Deka", "Saikia", "Choudhury",
    "Goswami", "Gogoi", "Phukan", "Hazarika", "Das", "Patel", "Singh", "Mukherjee", "Banerjee", "Roy",
    "Nath", "Medhi", "Dutta", "Kakati", "Bhattacharya", "Chakraborty", "Barman", "Talukdar", "Ahmed", "Ali"
]

CITIES = ["Guwahati", "Kolkata", "Delhi", "Mumbai", "Bangalore", "Hyderabad", "Shillong", "Jorhat", "Dibrugarh", "Silchar", "Tezpur", "Pune"]

REVIEW_TEMPLATES = [
    (5, "Exceptional stay! Beautiful view of the Brahmaputra river and courteous staff."),
    (5, "Luxury at its finest in Guwahati. Breakfast spread was outstanding."),
    (4, "Very pleasant experience. Clean rooms, comfortable beds, good location."),
    (4, "Great business hotel. High speed wifi and excellent conference hall."),
    (3, "Decent stay. Room was clean but room service was slightly delayed."),
    (2, "Disappointed with the air conditioning and slow check-in process."),
    (1, "Terrible experience. Room was noisy and bathroom was not maintained well.")
]


def create_guest_pool(num_guests: int, seed: int = 42) -> List[Dict[str, Any]]:
    """Create a deterministic pool of guest identities with controlled duplicate profiles."""
    rng = random.Random(seed)
    guests = []
    for i in range(1, num_guests + 1):
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        city = rng.choice(CITIES)
        email = f"{first.lower()}.{last.lower()}{rng.randint(10, 999)}@example.com"
        phone = f"+91-9{rng.randint(100000000, 999999999)}"
        guest_id = f"GST-{i:07d}"
        guests.append({
            "guest_id": guest_id,
            "first_name": first,
            "last_name": last,
            "email": email,
            "phone": phone,
            "city": city,
            "state": "Assam" if city in ["Guwahati", "Jorhat", "Dibrugarh", "Silchar", "Tezpur"] else "Other",
            "country": "India"
        })
    return guests


def generate_dataset(
    mode: str = "small",
    output_dir: Path = config.PIPELINE.generated_data_dir,
    seed: int = 42,
    anomaly_rate: float = 0.03
) -> Dict[str, int]:
    """
    Generate synthetic hotel dataset across Bookings, Inventory, Stays, and Reviews.
    Modes:
      - small: ~5,000 records (~2,200 bookings, ~1,500 inventory, ~1,000 stays, ~800 reviews)
      - normal: ~450,000 records (~200,000 bookings, ~130,000 inventory, ~80,000 stays, ~40,000 reviews)
      - peak: ~2,250,000 records (~1,000,000 bookings, ~650,000 inventory, ~400,000 stays, ~200,000 reviews)
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    np_rng = np.random.default_rng(seed)

    if mode == "peak":
        n_bookings = 1_000_000
        n_inventory = 650_000
        n_stays = 400_000
        n_reviews = 200_000
        n_guests = 120_000
        days_span = 180
    elif mode == "normal":
        n_bookings = 200_000
        n_inventory = 130_000
        n_stays = 80_000
        n_reviews = 40_000
        n_guests = 30_000
        days_span = 90
    else:  # small
        n_bookings = 2_200
        n_inventory = 1_500
        n_stays = 900
        n_reviews = 500
        n_guests = 800
        days_span = 14

    logger.info(f"Generating data in '{mode}' mode (Seed={seed}). Target: ~{n_bookings + n_inventory + n_stays + n_reviews} records.")

    base_date = date(2026, 1, 1)
    guests = create_guest_pool(n_guests, seed=seed)

    # Prepare hotel rooms map
    hotel_rooms = {}
    for h in HOTELS:
        h_id = h["hotel_id"]
        hotel_rooms[h_id] = []
        # Distribute room types
        for room_no in range(1, h["total_rooms"] + 1):
            if room_no <= 10:
                c_type = "Presidential Suite"
            elif room_no <= 30:
                c_type = "Executive Suite"
            elif room_no <= 60:
                c_type = "Deluxe King"
            elif room_no <= 90:
                c_type = "Deluxe Queen"
            elif room_no <= 130:
                c_type = "Standard Double"
            else:
                c_type = "Standard Single"
            hotel_rooms[h_id].append({
                "room_id": f"RM-{h_id[-3:]}-{room_no:03d}",
                "canonical_type": c_type
            })

    # 1. GENERATE BOOKINGS
    logger.info(f"Generating {n_bookings} booking records...")
    bookings = []
    statuses = ["CONFIRMED", "CONFIRMED", "CONFIRMED", "CHECKED_IN", "CHECKED_OUT", "CANCELLED", "NO_SHOW"]

    for i in range(1, n_bookings + 1):
        booking_id = f"BKG-{i:08d}"
        booking_line_id = f"{booking_id}-01"
        hotel = rng.choice(HOTELS)
        hotel_id = hotel["hotel_id"]
        room_info = rng.choice(hotel_rooms[hotel_id])
        room_id = room_info["room_id"]
        c_type = room_info["canonical_type"]

        guest = rng.choice(guests)
        guest_id = guest["guest_id"]

        booking_offset = rng.randint(0, days_span)
        b_date = base_date + timedelta(days=booking_offset)
        lead_time = rng.randint(0, 30)
        ci_date = b_date + timedelta(days=lead_time)
        length_of_stay = rng.randint(1, 7)
        co_date = ci_date + timedelta(days=length_of_stay)

        status = rng.choice(statuses)
        base_rate = CANONICAL_ROOM_TYPES[c_type]["base_rate"]
        # Currency: 85% INR, 15% USD
        if rng.random() < 0.15:
            currency = "USD"
            rate = round(base_rate / 86.5 + rng.uniform(-5.0, 15.0), 2)
        else:
            currency = "INR"
            rate = round(base_rate + rng.uniform(-300.0, 1000.0), 2)

        num_guests = rng.randint(1, CANONICAL_ROOM_TYPES[c_type]["capacity"])
        source_ts = datetime.combine(b_date, datetime.min.time(), tzinfo=timezone.utc) + timedelta(minutes=rng.randint(0, 1439))

        # Inject controlled anomalies
        is_anomaly = rng.random() < anomaly_rate
        if is_anomaly:
            anomaly_type = rng.choice([
                "null_guest", "null_booking", "duplicate_booking", "invalid_price",
                "invalid_currency", "invalid_dates", "late_arriving"
            ])
            if anomaly_type == "null_guest":
                guest_id = ""
            elif anomaly_type == "null_booking":
                booking_id = ""
            elif anomaly_type == "invalid_price":
                rate = rng.choice([-150.0, 0.0, -9999.0])
            elif anomaly_type == "invalid_currency":
                currency = rng.choice(["XXX", "INVALID", "EURO"])
            elif anomaly_type == "invalid_dates":
                co_date = ci_date - timedelta(days=2)  # checkout before checkin
            elif anomaly_type == "late_arriving":
                source_ts = source_ts + timedelta(days=45)

        bookings.append({
            "booking_id": booking_id,
            "booking_line_id": booking_line_id,
            "guest_id": guest_id,
            "guest_first_name": guest["first_name"],
            "guest_last_name": guest["last_name"],
            "guest_email": guest["email"],
            "guest_phone": guest["phone"],
            "hotel_id": hotel_id,
            "room_id": room_id,
            "room_type": rng.choice(ROOM_TYPE_VARIATIONS[c_type]),
            "booking_date": b_date.isoformat(),
            "check_in_date": ci_date.isoformat(),
            "check_out_date": co_date.isoformat(),
            "booking_status": status,
            "room_rate": rate,
            "currency": currency,
            "number_of_guests": num_guests,
            "source_updated_at": source_ts.isoformat()
        })

    # Controlled duplicate bookings
    if len(bookings) > 10:
        dup_count = max(2, int(len(bookings) * 0.005))
        for _ in range(dup_count):
            dup_idx = rng.randint(0, len(bookings) - 1)
            dup_record = dict(bookings[dup_idx])
            bookings.append(dup_record)

    # 2. GENERATE ROOM INVENTORY
    logger.info(f"Generating {n_inventory} inventory records...")
    inventory = []
    inv_id = 1
    # Generate inventory records across dates and hotel rooms
    day_i = 0
    while len(inventory) < n_inventory:
        inv_date = base_date + timedelta(days=day_i % max(1, days_span))
        for h in HOTELS:
            h_id = h["hotel_id"]
            h_rms = hotel_rooms[h_id]
            for r_info in h_rms:
                if len(inventory) >= n_inventory:
                    break
                c_type = r_info["canonical_type"]
                meta = CANONICAL_ROOM_TYPES[c_type]
                total_r = 1  # room level inventory
                avail_r = 1 if rng.random() > 0.3 else 0
                rate = meta["base_rate"] + rng.randint(-200, 500)
                curr = "INR" if rng.random() > 0.1 else "USD"
                if curr == "USD":
                    rate = round(rate / 86.5, 2)

                # Anomaly injection: negative inventory
                if rng.random() < anomaly_rate * 0.5:
                    avail_r = -1

                updated_at = datetime.combine(inv_date, datetime.min.time(), tzinfo=timezone.utc) + timedelta(hours=6)
                inventory.append({
                    "inventory_id": f"INV-{inv_id:08d}",
                    "hotel_id": h_id,
                    "room_id": r_info["room_id"],
                    "room_type": rng.choice(ROOM_TYPE_VARIATIONS[c_type]),
                    "inventory_date": inv_date.isoformat(),
                    "available_rooms": avail_r,
                    "total_rooms": total_r,
                    "rate": rate,
                    "currency": curr,
                    "source_updated_at": updated_at.isoformat()
                })
                inv_id += 1
            if len(inventory) >= n_inventory:
                break
        day_i += 1

    # 3. GENERATE GUEST STAYS
    logger.info(f"Generating {n_stays} guest stay records...")
    stays = []
    stay_statuses = ["COMPLETED", "COMPLETED", "CHECKED_IN", "EARLY_CHECKOUT"]
    valid_bookings = [b for b in bookings if b["booking_id"] and b["booking_status"] in ["CHECKED_IN", "CHECKED_OUT", "CONFIRMED"]]
    
    for i in range(1, n_stays + 1):
        stay_id = f"STY-{i:08d}"
        if valid_bookings and i <= len(valid_bookings):
            ref_b = valid_bookings[i - 1]
            b_id = ref_b["booking_id"]
            g_id = ref_b["guest_id"]
            h_id = ref_b["hotel_id"]
            r_id = ref_b["room_id"]
            ci = ref_b["check_in_date"]
            co = ref_b["check_out_date"]
        else:
            ref_b = rng.choice(valid_bookings) if valid_bookings else None
            b_id = ref_b["booking_id"] if ref_b else f"BKG-{rng.randint(1, n_bookings):08d}"
            g_id = ref_b["guest_id"] if ref_b else rng.choice(guests)["guest_id"]
            h_id = ref_b["hotel_id"] if ref_b else rng.choice(HOTELS)["hotel_id"]
            r_id = ref_b["room_id"] if ref_b else f"RM-{h_id[-3:]}-001"
            ci = (base_date + timedelta(days=rng.randint(0, days_span))).isoformat()
            co = (base_date + timedelta(days=rng.randint(0, days_span) + 2)).isoformat()

        status = rng.choice(stay_statuses)
        actual_ci = f"{ci}T{rng.randint(12, 16):02d}:{rng.randint(0, 59):02d}:00Z"
        actual_co = f"{co}T{rng.randint(9, 12):02d}:{rng.randint(0, 59):02d}:00Z" if status != "CHECKED_IN" else None

        stays.append({
            "stay_id": stay_id,
            "booking_id": b_id,
            "guest_id": g_id,
            "hotel_id": h_id,
            "room_id": r_id,
            "actual_check_in": actual_ci,
            "actual_check_out": actual_co,
            "stay_status": status
        })

    # 4. GENERATE CUSTOMER REVIEWS
    logger.info(f"Generating {n_reviews} review records...")
    reviews = []
    for i in range(1, n_reviews + 1):
        rev_id = f"REV-{i:08d}"
        if i <= len(stays):
            ref_stay = stays[i - 1]
            b_id = ref_stay["booking_id"]
            g_id = ref_stay["guest_id"]
            h_id = ref_stay["hotel_id"]
        else:
            ref_b = rng.choice(valid_bookings) if valid_bookings else None
            b_id = ref_b["booking_id"] if ref_b else f"BKG-{rng.randint(1, n_bookings):08d}"
            g_id = ref_b["guest_id"] if ref_b else rng.choice(guests)["guest_id"]
            h_id = ref_b["hotel_id"] if ref_b else rng.choice(HOTELS)["hotel_id"]

        tpl = rng.choice(REVIEW_TEMPLATES)
        rating = tpl[0]
        text = tpl[1]

        # Anomaly: Invalid rating (< 1 or > 5)
        if rng.random() < anomaly_rate * 0.5:
            rating = rng.choice([0, 6, 10, -1])

        rev_date = base_date + timedelta(days=rng.randint(5, days_span + 10))

        reviews.append({
            "review_id": rev_id,
            "booking_id": b_id,
            "guest_id": g_id,
            "hotel_id": h_id,
            "rating": rating,
            "review_text": text,
            "review_date": rev_date.isoformat()
        })

    # Save to DataFrames and CSV files
    df_bookings = pd.DataFrame(bookings)
    df_inventory = pd.DataFrame(inventory)
    df_stays = pd.DataFrame(stays)
    df_reviews = pd.DataFrame(reviews)
    df_hotels = pd.DataFrame(HOTELS)
    df_guests = pd.DataFrame(guests)

    # Export CSVs
    p_bkg = output_dir / "hotel_bookings.csv"
    p_inv = output_dir / "room_inventory.csv"
    p_sty = output_dir / "guest_stays.csv"
    p_rev = output_dir / "customer_reviews.csv"
    p_htl = output_dir / "hotels_reference.csv"
    p_gst = output_dir / "guests_reference.csv"

    df_bookings.to_csv(p_bkg, index=False)
    df_inventory.to_csv(p_inv, index=False)
    df_stays.to_csv(p_sty, index=False)
    df_reviews.to_csv(p_rev, index=False)
    df_hotels.to_csv(p_htl, index=False)
    df_guests.to_csv(p_gst, index=False)

    summary = {
        "mode": mode,
        "seed": seed,
        "bookings_count": len(df_bookings),
        "inventory_count": len(df_inventory),
        "stays_count": len(df_stays),
        "reviews_count": len(df_reviews),
        "total_source_records": len(df_bookings) + len(df_inventory) + len(df_stays) + len(df_reviews),
        "hotel_properties_count": len(HOTELS),
        "guests_pool_count": len(df_guests)
    }

    logger.info(f"Data generation complete: {summary}")
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic hotel data.")
    parser.add_argument("--mode", choices=["small", "normal", "peak"], default="small", help="Dataset size mode")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for deterministic generation")
    parser.add_argument("--output-dir", type=str, default=str(config.PIPELINE.generated_data_dir), help="Output directory")
    args = parser.parse_args()

    res = generate_dataset(mode=args.mode, output_dir=Path(args.output_dir), seed=args.seed)
    print(f"\n--- DATA GENERATION REPORT ---")
    for k, v in res.items():
        print(f"  {k}: {v}")
