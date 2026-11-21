-- ============================================================================
-- 05_create_metadata.sql
-- Hotel Batch Data Pipeline: Audit, Watermarks, Quality & Quarantine Tables
-- ============================================================================

-- Incremental Watermark Tracking Table
CREATE TABLE IF NOT EXISTS metadata_audit.etl_watermarks (
    watermark_id SERIAL PRIMARY KEY,
    source_name VARCHAR(100) NOT NULL UNIQUE,
    last_processed_timestamp TIMESTAMP WITH TIME ZONE,
    last_processed_record_id VARCHAR(100),
    batch_id VARCHAR(100) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Pipeline Execution Run History
CREATE TABLE IF NOT EXISTS metadata_audit.etl_run_history (
    run_id VARCHAR(100) PRIMARY KEY,
    batch_id VARCHAR(100) NOT NULL,
    pipeline_name VARCHAR(100) NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time TIMESTAMP WITH TIME ZONE,
    status VARCHAR(50) NOT NULL,
    records_extracted INT DEFAULT 0,
    records_staged INT DEFAULT 0,
    records_quarantined INT DEFAULT 0,
    records_loaded INT DEFAULT 0,
    duration_seconds NUMERIC(10, 2) DEFAULT 0.00,
    error_message TEXT
);

-- Data Quality Validation Results
CREATE TABLE IF NOT EXISTS metadata_audit.data_quality_results (
    check_id SERIAL PRIMARY KEY,
    batch_id VARCHAR(100) NOT NULL,
    check_name VARCHAR(100) NOT NULL,
    source_dataset VARCHAR(50) NOT NULL,
    records_checked INT NOT NULL,
    records_passed INT NOT NULL,
    records_failed INT NOT NULL,
    failure_percentage NUMERIC(6, 2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    execution_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Daily Source-to-Warehouse Reconciliation
CREATE TABLE IF NOT EXISTS metadata_audit.daily_reconciliation (
    reconciliation_id SERIAL PRIMARY KEY,
    reconciliation_date DATE NOT NULL,
    batch_id VARCHAR(100) NOT NULL,
    dataset_name VARCHAR(50) NOT NULL,
    source_count INT NOT NULL DEFAULT 0,
    staging_count INT NOT NULL DEFAULT 0,
    valid_count INT NOT NULL DEFAULT 0,
    quarantine_count INT NOT NULL DEFAULT 0,
    warehouse_count INT NOT NULL DEFAULT 0,
    count_difference INT NOT NULL DEFAULT 0,
    difference_percentage NUMERIC(8, 4) NOT NULL DEFAULT 0.0000,
    source_total_amount_inr NUMERIC(14, 2) DEFAULT 0.00,
    warehouse_total_amount_inr NUMERIC(14, 2) DEFAULT 0.00,
    amount_difference_inr NUMERIC(14, 2) DEFAULT 0.00,
    status VARCHAR(20) NOT NULL,
    generated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_recon_date_dataset UNIQUE (reconciliation_date, dataset_name)
);

-- Quarantine Table for Invalid/Rejected Records
CREATE TABLE IF NOT EXISTS quarantine.quarantine_records (
    quarantine_id SERIAL PRIMARY KEY,
    batch_id VARCHAR(100) NOT NULL,
    source_dataset VARCHAR(50) NOT NULL,
    record_identifier VARCHAR(100),
    reason_code VARCHAR(100) NOT NULL,
    error_message TEXT NOT NULL,
    raw_payload TEXT NOT NULL,
    detected_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
