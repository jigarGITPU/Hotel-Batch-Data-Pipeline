# Data Quality & Governance Framework – Hotel Batch Data Pipeline

## 1. Quality Validation Philosophy

Data quality is enforced using an automated, transparent gate architecture. Instead of silently dropping corrupt or non-compliant records or halting the entire pipeline on isolated errors, the framework:
1. Validates all ingested rows against 12 core business and referential checks.
2. Directs violating records into `quarantine.quarantine_records` along with raw JSON payloads and standardized failure reason codes.
3. Allows compliant clean records to progress through the warehouse loading pipeline.
4. Records structured execution metrics into `metadata_audit.data_quality_results`.
5. Compares Stage vs Warehouse in `metadata_audit.daily_reconciliation` and alerts when unexplained discrepancy exceeds 0.1%.

## 2. The 12 Mandatory Data Quality Checks

| # | Check Identifier | Target Dataset | Failure Reason Code | Rule Description | Criticality |
|---|---|---|---|---|---|
| 1 | `check_null_booking_id` | Bookings | `NULL_BOOKING_ID` | `booking_id` must not be null, empty string, or NaN | High |
| 2 | `check_null_guest_id` | Bookings | `NULL_GUEST_ID` | `guest_id` must not be null or empty string | High |
| 3 | `check_duplicate_booking` | Bookings | `DUPLICATE_BOOKING` | `booking_line_id` must be unique per batch | High |
| 4 | `check_invalid_price` | Bookings | `INVALID_PRICE` | `room_rate` must be strictly > 0 and <= 500,000 INR | High |
| 5 | `check_invalid_currency` | Bookings | `INVALID_CURRENCY` | `currency` must exist in supported currencies (INR, USD, EUR, GBP) | Medium |
| 6 | `check_invalid_date_range`| Bookings | `INVALID_DATE` | `check_out_date` must be strictly after `check_in_date` | High |
| 7 | `check_referential_hotel` | Bookings | `MISSING_HOTEL_REFERENCE` | `hotel_id` must reference an active property in `dim_hotel` | High |
| 8 | `check_negative_inventory`| Inventory | `NEGATIVE_INVENTORY` | `available_rooms` must be >= 0 | Medium |
| 9 | `check_invalid_rating` | Reviews | `INVALID_RATING` | `rating` must be an integer between 1 and 5 inclusive | Medium |
| 10 | `check_room_type_mapping`| Bookings/Inv | `MISSING_ROOM_MAPPING` | Room description must map to a canonical type in `room_type_mapping` | Medium |
| 11 | `check_source_row_count` | All | `ROW_COUNT_MISMATCH` | Verifies source row extraction equals staging table insert count | High |
| 12 | `check_reconciliation` | All | `RECONCILIATION_BREACH` | Unexplained discrepancy between source and warehouse must be < 0.1% | Critical |

## 3. Quarantine Schema Specification

Table: `quarantine.quarantine_records`

- `quarantine_id`: Auto-incrementing primary key.
- `batch_id`: ETL batch identifier.
- `source_dataset`: Source entity (`bookings`, `inventory`, `stays`, `reviews`).
- `record_identifier`: Business key of rejected record (e.g. `BKG-00000045`).
- `reason_code`: Standard uppercase reason code (e.g. `INVALID_PRICE`).
- `error_message`: Human-readable technical description of the validation failure.
- `raw_payload`: Complete original JSON record payload for recovery and replay.
- `detected_at`: ISO 8601 UTC timestamp of anomaly detection.

## 4. Daily Source-to-Warehouse Reconciliation

Reconciliation balances the data lineage:
$$\text{Source Records} = \text{Warehouse Loaded Records} + \text{Quarantined Records} + \text{Deduplicated Source Records}$$

$$\text{Unexplained Discrepancy} = |\text{Source Records} - (\text{Warehouse Loaded} + \text{Quarantined})|$$

$$\text{Discrepancy Percentage} = \frac{\text{Unexplained Discrepancy}}{\text{Source Records}} \times 100\%$$

**Acceptance Standard**: $\text{Discrepancy Percentage} < 0.1\%$. The pipeline enforces this invariant, issuing automated alerts if exceeded.
