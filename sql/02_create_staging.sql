-- ============================================================================
-- 02_create_staging.sql
-- Hotel Batch Data Pipeline: Raw Staging Tables
-- ============================================================================

CREATE TABLE IF NOT EXISTS raw_staging.stg_bookings (
    stg_id SERIAL PRIMARY KEY,
    booking_id VARCHAR(100),
    booking_line_id VARCHAR(100),
    guest_id VARCHAR(100),
    guest_first_name VARCHAR(100),
    guest_last_name VARCHAR(100),
    guest_email VARCHAR(150),
    guest_phone VARCHAR(50),
    hotel_id VARCHAR(100),
    room_id VARCHAR(100),
    room_type VARCHAR(100),
    booking_date VARCHAR(50),
    check_in_date VARCHAR(50),
    check_out_date VARCHAR(50),
    booking_status VARCHAR(50),
    room_rate VARCHAR(50),
    currency VARCHAR(20),
    number_of_guests VARCHAR(20),
    source_updated_at VARCHAR(100),
    source_file VARCHAR(255),
    source_system VARCHAR(50),
    extracted_at VARCHAR(100),
    batch_id VARCHAR(100),
    staged_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_staging.stg_inventory (
    stg_id SERIAL PRIMARY KEY,
    inventory_id VARCHAR(100),
    hotel_id VARCHAR(100),
    room_id VARCHAR(100),
    room_type VARCHAR(100),
    inventory_date VARCHAR(50),
    available_rooms VARCHAR(50),
    total_rooms VARCHAR(50),
    rate VARCHAR(50),
    currency VARCHAR(20),
    source_updated_at VARCHAR(100),
    source_file VARCHAR(255),
    source_system VARCHAR(50),
    extracted_at VARCHAR(100),
    batch_id VARCHAR(100),
    staged_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_staging.stg_stays (
    stg_id SERIAL PRIMARY KEY,
    stay_id VARCHAR(100),
    booking_id VARCHAR(100),
    guest_id VARCHAR(100),
    hotel_id VARCHAR(100),
    room_id VARCHAR(100),
    actual_check_in VARCHAR(100),
    actual_check_out VARCHAR(100),
    stay_status VARCHAR(50),
    source_file VARCHAR(255),
    source_system VARCHAR(50),
    extracted_at VARCHAR(100),
    batch_id VARCHAR(100),
    staged_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_staging.stg_reviews (
    stg_id SERIAL PRIMARY KEY,
    review_id VARCHAR(100),
    booking_id VARCHAR(100),
    guest_id VARCHAR(100),
    hotel_id VARCHAR(100),
    rating VARCHAR(20),
    review_text TEXT,
    review_date VARCHAR(50),
    source_file VARCHAR(255),
    source_system VARCHAR(50),
    extracted_at VARCHAR(100),
    batch_id VARCHAR(100),
    staged_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
