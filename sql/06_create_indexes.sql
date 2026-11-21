-- ============================================================================
-- 06_create_indexes.sql
-- Hotel Batch Data Pipeline: Performance Indexes
-- ============================================================================

-- Staging Indexes
CREATE INDEX IF NOT EXISTS idx_stg_bkg_batch ON raw_staging.stg_bookings (batch_id);
CREATE INDEX IF NOT EXISTS idx_stg_bkg_id ON raw_staging.stg_bookings (booking_id);
CREATE INDEX IF NOT EXISTS idx_stg_inv_batch ON raw_staging.stg_inventory (batch_id);
CREATE INDEX IF NOT EXISTS idx_stg_sty_batch ON raw_staging.stg_stays (batch_id);
CREATE INDEX IF NOT EXISTS idx_stg_rev_batch ON raw_staging.stg_reviews (batch_id);

-- Dimension Indexes
CREATE INDEX IF NOT EXISTS idx_dim_guest_id ON analytics_dw.dim_guest (guest_id);
CREATE INDEX IF NOT EXISTS idx_dim_hotel_id ON analytics_dw.dim_hotel (hotel_id);
CREATE INDEX IF NOT EXISTS idx_dim_room_id ON analytics_dw.dim_room (room_id);
CREATE INDEX IF NOT EXISTS idx_dim_date_full ON analytics_dw.dim_date (full_date);

-- Fact Bookings Indexes (for fast joins, filtering by hotel, date, and UPSERT lookups)
CREATE INDEX IF NOT EXISTS idx_fact_bkg_line_id ON analytics_dw.fact_bookings (booking_line_id);
CREATE INDEX IF NOT EXISTS idx_fact_bkg_date ON analytics_dw.fact_bookings (booking_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_bkg_hotel ON analytics_dw.fact_bookings (hotel_key);
CREATE INDEX IF NOT EXISTS idx_fact_bkg_guest ON analytics_dw.fact_bookings (guest_key);
CREATE INDEX IF NOT EXISTS idx_fact_bkg_updated ON analytics_dw.fact_bookings (source_updated_at);

-- Fact Stays Indexes
CREATE INDEX IF NOT EXISTS idx_fact_sty_id ON analytics_dw.fact_stays (stay_id);
CREATE INDEX IF NOT EXISTS idx_fact_sty_bkg ON analytics_dw.fact_stays (booking_id);
CREATE INDEX IF NOT EXISTS idx_fact_sty_checkin ON analytics_dw.fact_stays (check_in_date_key);

-- Fact Reviews Indexes
CREATE INDEX IF NOT EXISTS idx_fact_rev_id ON analytics_dw.fact_reviews (review_id);
CREATE INDEX IF NOT EXISTS idx_fact_rev_bkg ON analytics_dw.fact_reviews (booking_id);
CREATE INDEX IF NOT EXISTS idx_fact_rev_hotel ON analytics_dw.fact_reviews (hotel_key);

-- Quarantine & Metadata Indexes
CREATE INDEX IF NOT EXISTS idx_quarantine_batch ON quarantine.quarantine_records (batch_id);
CREATE INDEX IF NOT EXISTS idx_quarantine_reason ON quarantine.quarantine_records (reason_code);
CREATE INDEX IF NOT EXISTS idx_watermarks_source ON metadata_audit.etl_watermarks (source_name);
