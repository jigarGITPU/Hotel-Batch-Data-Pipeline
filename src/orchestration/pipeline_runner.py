"""
Central Pipeline Runner for Hotel Batch Data Pipeline.
Coordinates end-to-end execution: Extraction -> Staging -> Quality Validation ->
Transformation -> Dimensional Loading (Idempotent UPSERT) -> Reconciliation -> Watermarking.
"""

from datetime import datetime, date, timezone, timedelta
from typing import Dict, Any, Optional, Tuple
import pandas as pd

from src.utils.config import config
from src.utils.database import DatabaseManager, db
from src.monitoring.logger import get_logger, generate_run_id, generate_batch_id
from src.monitoring.notifications import notification_service
from src.extraction.csv_extractor import CSVExtractor
from src.extraction.api_extractor import APIExtractor
from src.loading.staging_loader import StagingLoader
from src.quality.validation import DataQualityValidator
from src.transformation.guest_deduplication import GuestDeduplicator
from src.transformation.currency import CurrencyTransformer
from src.transformation.room_mapping import RoomTypeMapper
from src.transformation.cleaning import clean_bookings, clean_stays, clean_reviews
from src.loading.warehouse_loader import WarehouseLoader
from src.quality.reconciliation import ReconciliationAuditor
from src.warehouse.schema import initialize_warehouse

logger = get_logger("pipeline_runner")


class HotelPipelineRunner:
    """Orchestrates the incremental, monitored ETL workflow for Guwahati hotels."""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.db = db_manager or db
        self.csv_extractor = CSVExtractor()
        self.api_extractor = APIExtractor()
        self.staging_loader = StagingLoader(self.db)
        self.validator = DataQualityValidator(self.db)
        self.guest_dedup = GuestDeduplicator(self.db)
        self.currency_trans = CurrencyTransformer(self.db)
        self.room_mapper = RoomTypeMapper(self.db)
        self.wh_loader = WarehouseLoader(self.db)
        self.reconciler = ReconciliationAuditor(self.db)

    def run(
        self,
        target_date: Optional[str] = None,
        run_id: Optional[str] = None,
        batch_id: Optional[str] = None,
        incremental: bool = True
    ) -> Dict[str, Any]:
        """
        Execute full batch pipeline run.

        Args:
            target_date: Target business date in YYYY-MM-DD format (None = all generated)
            run_id: Optional unique run identifier
            batch_id: Optional batch identifier
            incremental: If True, uses watermark-based filtering
        """
        run_id = run_id or generate_run_id()
        batch_id = batch_id or generate_batch_id()
        start_time = datetime.now(timezone.utc)
        start_time_iso = start_time.isoformat()

        logger.info(f"=== Starting Hotel Batch Data Pipeline [Run: {run_id}] [Batch: {batch_id}] ===")
        if target_date:
            logger.info(f"Target Business Date: {target_date}")

        # Metrics accumulator
        metrics = {
            "run_id": run_id,
            "batch_id": batch_id,
            "pipeline_name": "hotel_analytics_batch_etl",
            "target_date": target_date,
            "start_time": start_time_iso,
            "status": "FAILED",
            "records_extracted": 0,
            "records_staged": 0,
            "records_quarantined": 0,
            "records_loaded": 0,
            "duration_seconds": 0.0,
            "error_message": None
        }

        try:
            # 1. Initialize schema if tables don't exist
            initialize_warehouse(self.db)

            # 2. Watermark check for incremental extraction
            watermark_ts = self.wh_loader.get_watermark("hotel_bookings") if incremental else None
            overlap_hours = config.PIPELINE.watermark_overlap_hours
            if watermark_ts and incremental:
                effective_watermark = watermark_ts - timedelta(hours=overlap_hours)
                logger.info(f"Incremental mode active: Watermark = {watermark_ts.isoformat()}, Overlap Window = {overlap_hours}h")
            else:
                effective_watermark = None

            # 3. EXTRACTION
            logger.info("Phase: EXTRACTION")
            # CSV: Stays, Reviews, Reference Guests, CSV Bookings
            df_csv_bkg, meta_bkg = self.csv_extractor.extract_dataset("bookings", target_date=target_date, date_filter_col="booking_date")
            df_csv_inv, meta_inv = self.csv_extractor.extract_dataset("inventory", target_date=target_date, date_filter_col="inventory_date")
            df_stays, meta_sty = self.csv_extractor.extract_dataset("stays")
            df_reviews, meta_rev = self.csv_extractor.extract_dataset("reviews")
            df_guests_raw, _ = self.csv_extractor.extract_dataset("guests")

            # API: Paginated Bookings channel extract
            df_api_bkg, meta_api = self.api_extractor.extract_paginated("api/v1/bookings", target_date=target_date)

            # Combine bookings sources (CSV + API channels)
            if not df_api_bkg.empty and not df_csv_bkg.empty:
                # Merge without double counting identical bookings
                combined_bkg = pd.concat([df_csv_bkg, df_api_bkg], ignore_index=True).drop_duplicates(subset=["booking_id"], keep="first")
            elif not df_csv_bkg.empty:
                combined_bkg = df_csv_bkg
            else:
                combined_bkg = df_api_bkg

            # Apply incremental watermark filter on source_updated_at if available
            if effective_watermark and not combined_bkg.empty and "source_updated_at" in combined_bkg.columns:
                try:
                    ts_col = pd.to_datetime(combined_bkg["source_updated_at"], utc=True, errors="coerce")
                    combined_bkg = combined_bkg[ts_col >= effective_watermark].copy()
                except Exception as w_err:
                    logger.warning(f"Watermark filtering warning: {w_err}")

            total_extracted = len(combined_bkg) + len(df_csv_inv) + len(df_stays) + len(df_reviews)
            metrics["records_extracted"] = total_extracted
            logger.info(f"Extraction completed: Total records extracted = {total_extracted}")

            # 4. STAGING LAYER
            logger.info("Phase: STAGING")
            self.staging_loader.load_staging_table(combined_bkg, "bookings", batch_id)
            self.staging_loader.load_staging_table(df_csv_inv, "inventory", batch_id)
            self.staging_loader.load_staging_table(df_stays, "stays", batch_id)
            self.staging_loader.load_staging_table(df_reviews, "reviews", batch_id)
            metrics["records_staged"] = total_extracted

            # 5. DATA QUALITY VALIDATION & QUARANTINE
            logger.info("Phase: QUALITY VALIDATION & QUARANTINE")
            valid_bkg, quar_bkg, _ = self.validator.validate_bookings(combined_bkg, batch_id)
            valid_inv, quar_inv, _ = self.validator.validate_inventory(df_csv_inv, batch_id)
            valid_rev, quar_rev, _ = self.validator.validate_reviews(df_reviews, batch_id)
            valid_sty = df_stays.copy()

            total_quarantined = len(quar_bkg) + len(quar_inv) + len(quar_rev)
            metrics["records_quarantined"] = total_quarantined

            # 6. TRANSFORMATION & ENRICHMENT
            logger.info("Phase: TRANSFORMATION")
            # A. Guest Deduplication & Identity Resolution
            if not df_guests_raw.empty:
                df_canonical_guests, _ = self.guest_dedup.deduplicate_guests(df_guests_raw)
            else:
                df_canonical_guests, _ = self.guest_dedup.deduplicate_guests(valid_bkg)

            # Map canonical guest IDs onto valid bookings
            valid_bkg["canonical_guest_id"] = valid_bkg["guest_id"].apply(self.guest_dedup.get_canonical_id)

            # B. Currency Standardization to INR
            valid_bkg = self.currency_trans.standardize_dataframe(
                valid_bkg,
                amount_col="room_rate",
                currency_col="currency",
                out_amount_col="room_rate_inr",
                out_rate_col="exchange_rate_used"
            )

            # C. Room Type Normalization
            mapped_types = []
            mapped_caps = []
            for _, r in valid_bkg.iterrows():
                c_type, cap, _ = self.room_mapper.map_room_type(r.get("room_type", "Standard Double"))
                mapped_types.append(c_type or "Standard Double")
                mapped_caps.append(cap or 2)
            valid_bkg["canonical_room_type"] = mapped_types
            valid_bkg["standard_capacity"] = mapped_caps

            # D. Cleaning, Derived Metrics (lead time, stay length, totals)
            valid_bkg = clean_bookings(valid_bkg)
            valid_sty = clean_stays(valid_sty)
            valid_rev = clean_reviews(valid_rev)

            # 7. DIMENSIONAL & FACT LOADING (IDEMPOTENT UPSERT)
            logger.info("Phase: WAREHOUSE LOADING (UPSERT)")
            # Upsert dimensions
            self.wh_loader.upsert_guests(df_canonical_guests, batch_id)

            # Upsert facts
            loaded_bkg = self.wh_loader.upsert_fact_bookings(valid_bkg, batch_id)
            loaded_sty = self.wh_loader.upsert_fact_stays(valid_sty, batch_id)
            loaded_rev = self.wh_loader.upsert_fact_reviews(valid_rev, batch_id)

            total_loaded = loaded_bkg + loaded_sty + loaded_rev
            metrics["records_loaded"] = total_loaded

            # 8. RECONCILIATION
            logger.info("Phase: RECONCILIATION")
            recon_date = datetime.strptime(target_date, "%Y-%m-%d").date() if target_date else date.today()
            bkg_monetary_total = float(valid_bkg["total_amount_inr"].sum()) if "total_amount_inr" in valid_bkg.columns else 0.0

            recon_res = self.reconciler.reconcile_dataset(
                batch_id=batch_id,
                dataset_name="bookings",
                recon_date=recon_date,
                source_count=len(combined_bkg),
                source_monetary_total=bkg_monetary_total
            )

            # Assert reconciliation quality threshold (< 0.1% unexplained variance)
            if recon_res["difference_percentage"] > config.QUALITY.max_unexplained_reconciliation_diff_pct:
                logger.warning(
                    f"Reconciliation difference {recon_res['difference_percentage']}% exceeds threshold "
                    f"{config.QUALITY.max_unexplained_reconciliation_diff_pct}%!"
                )
                notification_service.send_alert(
                    alert_type="RECONCILIATION_BREACH",
                    title="Source-to-Warehouse Reconciliation Discrepancy",
                    details=recon_res,
                    severity="WARNING"
                )

            # 9. WATERMARK UPDATE
            now_utc = datetime.now(timezone.utc)
            self.wh_loader.update_watermark("hotel_bookings", now_utc, batch_id)

            # Mark run success
            end_time = datetime.now(timezone.utc)
            duration = round((end_time - start_time).total_seconds(), 2)
            metrics["end_time"] = end_time.isoformat()
            metrics["duration_seconds"] = duration
            metrics["status"] = "SUCCESS"

            logger.info(
                f"=== Pipeline Run Successful! [Duration: {duration}s] "
                f"[Extracted: {total_extracted}] [Loaded: {total_loaded}] "
                f"[Quarantined: {total_quarantined}] ==="
            )

        except Exception as e:
            end_time = datetime.now(timezone.utc)
            duration = round((end_time - start_time).total_seconds(), 2)
            metrics["end_time"] = end_time.isoformat()
            metrics["duration_seconds"] = duration
            metrics["status"] = "FAILED"
            metrics["error_message"] = str(e)
            logger.error(f"Pipeline execution failed: {e}", exc_info=True)

            notification_service.send_alert(
                alert_type="PIPELINE_FAILURE",
                title=f"Hotel Batch Data Pipeline Failed (Run: {run_id})",
                details=metrics,
                severity="CRITICAL"
            )

        # 10. PERSIST RUN HISTORY
        self._record_run_history(metrics)
        return metrics

    def _record_run_history(self, m: Dict[str, Any]):
        """Persist execution run summary into metadata_audit.etl_run_history."""
        tbl = self.db.get_table_name("metadata_audit", "etl_run_history")
        df_run = pd.DataFrame([{
            "run_id": m["run_id"],
            "batch_id": m["batch_id"],
            "pipeline_name": m["pipeline_name"],
            "start_time": m["start_time"],
            "end_time": m.get("end_time"),
            "status": m["status"],
            "records_extracted": m["records_extracted"],
            "records_staged": m["records_staged"],
            "records_quarantined": m["records_quarantined"],
            "records_loaded": m["records_loaded"],
            "duration_seconds": m["duration_seconds"],
            "error_message": m.get("error_message")
        }])
        self.db.bulk_insert(df_run, "metadata_audit", "etl_run_history", if_exists="append")
