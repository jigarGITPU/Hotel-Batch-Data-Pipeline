-- ============================================================================
-- 03_create_dimensions.sql
-- Hotel Batch Data Pipeline: Dimension Tables & Mappings
-- ============================================================================

-- Hotels Dimension (SCD Type 1 with Audit)
CREATE TABLE IF NOT EXISTS analytics_dw.dim_hotel (
    hotel_key SERIAL PRIMARY KEY,
    hotel_id VARCHAR(50) NOT NULL UNIQUE,
    hotel_name VARCHAR(150) NOT NULL,
    location VARCHAR(255) NOT NULL,
    rating_category VARCHAR(50),
    total_rooms INT NOT NULL DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Guest Dimension (Surrogate Key, Unified Identity)
CREATE TABLE IF NOT EXISTS analytics_dw.dim_guest (
    guest_key SERIAL PRIMARY KEY,
    guest_id VARCHAR(50) NOT NULL UNIQUE,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(200),
    email VARCHAR(150),
    phone VARCHAR(50),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100) DEFAULT 'India',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Room Dimension
CREATE TABLE IF NOT EXISTS analytics_dw.dim_room (
    room_key SERIAL PRIMARY KEY,
    room_id VARCHAR(50) NOT NULL UNIQUE,
    hotel_id VARCHAR(50) NOT NULL,
    canonical_room_type VARCHAR(100) NOT NULL,
    standard_capacity INT NOT NULL DEFAULT 2,
    base_rate_inr NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Date Dimension (Standard Analytics Calendar)
CREATE TABLE IF NOT EXISTS analytics_dw.dim_date (
    date_key INT PRIMARY KEY,              -- YYYYMMDD
    full_date DATE NOT NULL UNIQUE,
    day_of_week INT NOT NULL,              -- 1 = Monday, 7 = Sunday
    day_name VARCHAR(20) NOT NULL,
    day_of_month INT NOT NULL,
    day_of_year INT NOT NULL,
    week_of_year INT NOT NULL,
    month_num INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    quarter INT NOT NULL,
    year_num INT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- Currency Reference Table
CREATE TABLE IF NOT EXISTS analytics_dw.dim_currency (
    currency_code VARCHAR(10) PRIMARY KEY,
    currency_name VARCHAR(50) NOT NULL,
    exchange_rate_to_inr NUMERIC(12, 4) NOT NULL,
    effective_date DATE NOT NULL,
    source VARCHAR(100) NOT NULL,
    last_updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Room Type Normalization Mapping Table
CREATE TABLE IF NOT EXISTS analytics_dw.room_type_mapping (
    mapping_id SERIAL PRIMARY KEY,
    source_room_type VARCHAR(100) NOT NULL UNIQUE,
    canonical_room_type VARCHAR(100) NOT NULL,
    standard_capacity INT NOT NULL,
    default_rate_inr NUMERIC(10, 2) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Guest Identity Mapping Table (Tracks resolution of duplicate guest profiles)
CREATE TABLE IF NOT EXISTS analytics_dw.guest_identity_mapping (
    mapping_id SERIAL PRIMARY KEY,
    source_guest_id VARCHAR(50) NOT NULL UNIQUE,
    canonical_guest_id VARCHAR(50) NOT NULL,
    confidence_score NUMERIC(5, 2) DEFAULT 1.00,
    match_rule VARCHAR(100) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
