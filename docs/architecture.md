# System Architecture – Hotel Batch Data Pipeline

## 1. High-Level Architecture Overview

The Hotel Batch Data Pipeline implements a multi-tier, decoupled, and idempotent batch processing architecture designed for high reliability, auditability, and analytical query performance.

```mermaid
flowchart TD
    subgraph Sources ["SOURCE DATA LAYER"]
        CSV_FILES["Daily Hotel CSV Extracts<br/>(Bookings, Inventory, Stays, Reviews)"]
        REST_API["Simulated Property API Feeds<br/>(Paginated REST Endpoints)"]
    end

    subgraph Extraction ["EXTRACTION & AUDIT LAYER"]
        CSV_EXT["CSV Extractor<br/>(Schema Validation & Audit Metadata)"]
        API_EXT["API Extractor<br/>(Pagination, Retries, Backoff)"]
    end

    subgraph Staging ["STAGING DATABASE (raw_staging)"]
        STG_BKG["raw_staging.stg_bookings"]
        STG_INV["raw_staging.stg_inventory"]
        STG_STY["raw_staging.stg_stays"]
        STG_REV["raw_staging.stg_reviews"]
    end

    subgraph Quality ["QUALITY GATE & QUARANTINE"]
        DQ_ENGINE["Automated Quality Engine<br/>(12 Business Rules & Key Validation)"]
        QUARANTINE_TBL["quarantine.quarantine_records<br/>(Granular Reason Codes & Payloads)"]
        VALID_STREAM["Validated Clean Records"]
    end

    subgraph Transformation ["TRANSFORMATION LAYER"]
        GUEST_DEDUP["Guest Deduplication Engine<br/>(Identity Resolution & Canonical Keys)"]
        CURR_STD["Currency Normalization Engine<br/>(Standardize to INR via dim_currency)"]
        ROOM_MAP["Room Type Mapping Engine<br/>(Canonicalize Fuzzy & Non-standard Types)"]
        CLEAN_ENRICH["Derived Metrics & Date Normalization<br/>(Lead Time, Stay Length, Date Keys)"]
    end

    subgraph Warehouse ["ANALYTICS DATA WAREHOUSE (analytics_dw)"]
        DIM_HOTEL["dim_hotel (SCD Type 1)"]
        DIM_GUEST["dim_guest (Surrogate Keys)"]
        DIM_ROOM["dim_room"]
        DIM_DATE["dim_date (Calendar Keys)"]
        FACT_BKG["fact_bookings (Idempotent UPSERT)"]
        FACT_STY["fact_stays"]
        FACT_REV["fact_reviews"]
    end

    subgraph Governance ["AUDIT, RECONCILIATION & MONITORING"]
        RECON["Source-to-Warehouse Reconciliation<br/>(Assert < 0.1% Discrepancy)"]
        WATERMARKS["metadata_audit.etl_watermarks<br/>(Atomic Watermark Tracking)"]
        ALERTING["Monitoring & Notifications<br/>(Console & Email Alerts)"]
    end

    CSV_FILES --> CSV_EXT
    REST_API --> API_EXT
    CSV_EXT --> STG_BKG & STG_INV & STG_STY & STG_REV
    API_EXT --> STG_BKG & STG_INV

    STG_BKG & STG_INV & STG_STY & STG_REV --> DQ_ENGINE
    DQ_ENGINE -->|Failed Checks| QUARANTINE_TBL
    DQ_ENGINE -->|Passed Checks| VALID_STREAM

    VALID_STREAM --> GUEST_DEDUP
    GUEST_DEDUP --> CURR_STD
    CURR_STD --> ROOM_MAP
    ROOM_MAP --> CLEAN_ENRICH

    CLEAN_ENRICH --> DIM_HOTEL & DIM_GUEST & DIM_ROOM & DIM_DATE
    CLEAN_ENRICH --> FACT_BKG & FACT_STY & FACT_REV

    FACT_BKG & QUARANTINE_TBL --> RECON
    RECON --> ALERTING
    FACT_BKG --> WATERMARKS
```

## 2. Component Breakdown

### 2.1 Extraction Layer
- **CSV Extractor (`src/extraction/csv_extractor.py`)**: Reads structured CSV files from source directories, performs structural column verification, and appends pipeline audit metadata (`source_file`, `source_system`, `extracted_at`).
- **REST API Extractor (`src/extraction/api_extractor.py`)**: Interacts with the simulated FastAPI service (`api/main.py`), utilizing cursor/page-based pagination, exponential retry backoff, and automatic fallback.

### 2.2 Staging Layer
- Schema: `raw_staging`
- Staging tables store ingested records as raw strings with audit metadata (`batch_id`, `staged_at`, `source_system`), ensuring raw data auditability and isolation from the core warehouse.

### 2.3 Quality Gate & Quarantine
- Evaluates 12 deterministic data quality assertions.
- Non-compliant rows are redirected into `quarantine.quarantine_records` alongside original payloads and explicit failure reason codes.

### 2.4 Transformation Layer
- **Guest Deduplication**: Matches guest profiles across bookings using email, phone, and name heuristics without merging distinct individuals.
- **Currency Standardization**: Converts non-INR transactions (e.g. USD) into INR using exchange rates from `dim_currency`.
- **Room Standardization**: Normalizes disparate property descriptions into canonical room categories.
- **Date Normalization**: Standardizes dates and derives analytics fields (`lead_time_days`, `length_of_stay_days`, `total_amount_inr`).

### 2.5 Star Schema Warehouse
- Schema: `analytics_dw`
- Star schema with surrogate keys and dimensional integrity.
- Incremental loading via SQL `ON CONFLICT (...) DO UPDATE` ensuring idempotency.
