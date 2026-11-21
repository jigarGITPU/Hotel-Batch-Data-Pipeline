-- ============================================================================
-- 01_create_schemas.sql
-- Hotel Batch Data Pipeline: Database Schemas Definition
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS raw_staging;
CREATE SCHEMA IF NOT EXISTS analytics_dw;
CREATE SCHEMA IF NOT EXISTS metadata_audit;
CREATE SCHEMA IF NOT EXISTS quarantine;
