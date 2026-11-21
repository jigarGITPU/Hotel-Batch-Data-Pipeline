"""SQL audit queries against the warehouse database."""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.database import db

# Actual SQLite table names
tbl = 'analytics_dw_fact_bookings'
htl = 'analytics_dw_dim_hotel'
gst = 'analytics_dw_dim_guest'
rm  = 'analytics_dw_dim_room'
stf = 'analytics_dw_fact_stays'
rvf = 'analytics_dw_fact_reviews'
qrn = 'quarantine_quarantine_records'
wm  = 'metadata_audit_etl_watermarks'
rec = 'metadata_audit_daily_reconciliation'
eth = 'metadata_audit_etl_run_history'

def q(sql):
    return db.fetch_dataframe(sql).iloc[0, 0]

print('=== WAREHOUSE RECORD COUNTS ===')
for label, sql in [
    ('fact_bookings',   f'SELECT COUNT(*) as c FROM {tbl}'),
    ('fact_stays',      f'SELECT COUNT(*) as c FROM {stf}'),
    ('fact_reviews',    f'SELECT COUNT(*) as c FROM {rvf}'),
    ('dim_hotel',       f'SELECT COUNT(*) as c FROM {htl}'),
    ('dim_guest',       f'SELECT COUNT(*) as c FROM {gst}'),
    ('dim_room',        f'SELECT COUNT(*) as c FROM {rm}'),
    ('quarantine_total',f'SELECT COUNT(*) as c FROM {qrn}'),
]:
    print(f'  {label}: {q(sql)}')

print()
print('=== HOTEL BOOKING DISTRIBUTION (14 properties) ===')
df = db.fetch_dataframe(f'SELECT h.hotel_name, COUNT(*) as bookings FROM {tbl} fb JOIN {htl} h ON fb.hotel_key = h.hotel_key GROUP BY h.hotel_name ORDER BY bookings DESC')
print(df.to_string())

print()
print('=== DUPLICATE BUSINESS KEY CHECK ===')
dup_b = db.fetch_dataframe(f'SELECT booking_line_id, COUNT(*) cnt FROM {tbl} GROUP BY booking_line_id HAVING cnt > 1')
print(f'  Duplicate booking_line_ids: {len(dup_b)}')
dup_s = db.fetch_dataframe(f'SELECT stay_id, COUNT(*) cnt FROM {stf} GROUP BY stay_id HAVING cnt > 1')
print(f'  Duplicate stay_ids:         {len(dup_s)}')
dup_r = db.fetch_dataframe(f'SELECT review_id, COUNT(*) cnt FROM {rvf} GROUP BY review_id HAVING cnt > 1')
print(f'  Duplicate review_ids:       {len(dup_r)}')

print()
print('=== NULL BUSINESS KEY CHECK ===')
print(f"  NULL booking_line_ids: {q(f'SELECT COUNT(*) as c FROM {tbl} WHERE booking_line_id IS NULL')}")
print(f"  NULL stay_ids:         {q(f'SELECT COUNT(*) as c FROM {stf} WHERE stay_id IS NULL')}")
print(f"  NULL review_ids:       {q(f'SELECT COUNT(*) as c FROM {rvf} WHERE review_id IS NULL')}")

print()
print('=== CURRENCY DISTRIBUTION IN FACT_BOOKINGS ===')
cur_df = db.fetch_dataframe(f'SELECT original_currency, COUNT(*) as cnt, ROUND(AVG(total_amount_inr),2) as avg_inr FROM {tbl} GROUP BY original_currency ORDER BY cnt DESC')
print(cur_df.to_string())

print()
print('=== BOOKING STATUS DISTRIBUTION ===')
status_df = db.fetch_dataframe(f'SELECT booking_status, COUNT(*) as cnt FROM {tbl} GROUP BY booking_status ORDER BY cnt DESC')
print(status_df.to_string())

print()
print('=== RATING RANGE IN FACT_REVIEWS ===')
rating_df = db.fetch_dataframe(f'SELECT MIN(rating) as min_rating, MAX(rating) as max_rating, ROUND(AVG(rating),3) as avg_rating, COUNT(*) as total FROM {rvf}')
print(rating_df.to_string())

print()
print('=== INVENTORY STATS IN STAGING ===')
inv_df = db.fetch_dataframe('SELECT MIN(CAST(available_rooms AS REAL)) as min_avail, MAX(CAST(available_rooms AS REAL)) as max_avail, COUNT(*) as total FROM raw_staging_stg_inventory')
print(inv_df.to_string())
neg_inv = db.fetch_dataframe("SELECT COUNT(*) as c FROM raw_staging_stg_inventory WHERE CAST(available_rooms AS REAL) < 0")
print(f'  Negative available_rooms in staging (should be quarantined): {neg_inv.iloc[0,0]}')

print()
print('=== WATERMARKS ===')
wm_df = db.fetch_dataframe(f'SELECT source_name, last_processed_timestamp, batch_id, updated_at FROM {wm} ORDER BY updated_at DESC LIMIT 10')
print(wm_df.to_string())

print()
print('=== QUARANTINE BREAKDOWN BY DATASET & REASON CODE ===')
q_df = db.fetch_dataframe(f'SELECT source_dataset, reason_code, COUNT(*) as cnt FROM {qrn} GROUP BY source_dataset, reason_code ORDER BY source_dataset, cnt DESC')
print(q_df.to_string())

print()
print('=== RECONCILIATION LOG (latest 10) ===')
recon_df = db.fetch_dataframe(f'SELECT reconciliation_date, dataset_name, source_count, warehouse_count, quarantine_count, count_difference, difference_percentage, status FROM {rec} ORDER BY reconciliation_id DESC LIMIT 10')
print(recon_df.to_string())

print()
print('=== ETL RUN HISTORY (latest 5) ===')
eth_df = db.fetch_dataframe(f'SELECT run_id, status, records_extracted, records_loaded, records_quarantined, ROUND(duration_seconds, 2) as duration_sec, start_time FROM {eth} ORDER BY start_time DESC LIMIT 5')
print(eth_df.to_string())

print()
print('=== REFERENTIAL INTEGRITY: ORPHAN FACTS ===')
orphan_hotel = q(f'SELECT COUNT(*) as c FROM {tbl} fb LEFT JOIN {htl} h ON fb.hotel_key = h.hotel_key WHERE h.hotel_key IS NULL')
orphan_guest = q(f'SELECT COUNT(*) as c FROM {tbl} fb LEFT JOIN {gst} g ON fb.guest_key = g.guest_key WHERE g.guest_key IS NULL')
orphan_room  = q(f'SELECT COUNT(*) as c FROM {tbl} fb LEFT JOIN {rm} r ON fb.room_key = r.room_key WHERE r.room_key IS NULL')
print(f'  fact_bookings orphan hotel refs: {orphan_hotel}')
print(f'  fact_bookings orphan guest refs: {orphan_guest}')
print(f'  fact_bookings orphan room refs:  {orphan_room}')

print()
print('=== ALL SQL AUDIT QUERIES COMPLETED SUCCESSFULLY ===')
