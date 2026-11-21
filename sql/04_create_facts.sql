-- ============================================================================
-- 04_create_facts.sql
-- Hotel Batch Data Pipeline: Fact Tables
-- ============================================================================

-- Fact Bookings: Grain = One row per booking line item
CREATE TABLE IF NOT EXISTS analytics_dw.fact_bookings (
    booking_key SERIAL PRIMARY KEY,
    booking_id VARCHAR(50) NOT NULL,
    booking_line_id VARCHAR(50) NOT NULL UNIQUE,
    hotel_key INT NOT NULL,
    guest_key INT NOT NULL,
    room_key INT NOT NULL,
    booking_date_key INT NOT NULL,
    check_in_date_key INT NOT NULL,
    check_out_date_key INT NOT NULL,
    booking_status VARCHAR(50) NOT NULL,
    original_rate NUMERIC(10, 2) NOT NULL,
    original_currency VARCHAR(10) NOT NULL,
    exchange_rate_used NUMERIC(12, 4) NOT NULL,
    room_rate_inr NUMERIC(10, 2) NOT NULL,
    total_amount_inr NUMERIC(12, 2) NOT NULL,
    number_of_guests INT NOT NULL DEFAULT 1,
    lead_time_days INT NOT NULL DEFAULT 0,
    length_of_stay_days INT NOT NULL DEFAULT 1,
    source_updated_at TIMESTAMP WITH TIME ZONE,
    dw_batch_id VARCHAR(100) NOT NULL,
    dw_loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Fact Stays: Grain = One row per physical guest check-in/stay
CREATE TABLE IF NOT EXISTS analytics_dw.fact_stays (
    stay_key SERIAL PRIMARY KEY,
    stay_id VARCHAR(50) NOT NULL UNIQUE,
    booking_id VARCHAR(50) NOT NULL,
    hotel_key INT NOT NULL,
    guest_key INT NOT NULL,
    room_key INT NOT NULL,
    check_in_date_key INT NOT NULL,
    check_out_date_key INT,
    stay_status VARCHAR(50) NOT NULL,
    actual_check_in TIMESTAMP WITH TIME ZONE,
    actual_check_out TIMESTAMP WITH TIME ZONE,
    actual_duration_hours NUMERIC(8, 2),
    dw_batch_id VARCHAR(100) NOT NULL,
    dw_loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Fact Reviews: Grain = One row per customer feedback review
CREATE TABLE IF NOT EXISTS analytics_dw.fact_reviews (
    review_key SERIAL PRIMARY KEY,
    review_id VARCHAR(50) NOT NULL UNIQUE,
    booking_id VARCHAR(50) NOT NULL,
    hotel_key INT NOT NULL,
    guest_key INT NOT NULL,
    review_date_key INT NOT NULL,
    rating INT NOT NULL,
    review_text TEXT,
    sentiment_category VARCHAR(20),
    dw_batch_id VARCHAR(100) NOT NULL,
    dw_loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
