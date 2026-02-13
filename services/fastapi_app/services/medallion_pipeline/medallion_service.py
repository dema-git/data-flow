#################################################
# medallion_service.py
#
# Implements the Medallion Architecture ETL pipeline:
# - Bronze → Silver: raw events cleanup & validation
# - Silver → Gold: business logic transformations
#
# Each layer stores data in MinIO and archives
# processed files to prevent reprocessing !!!
#
##################################################

from dataclasses import asdict
from typing import List, Dict
from minio_utils.files_handler import get_files_data, upload_batch

from services.medallion_pipeline.outbox import enqueue_archive_task
from services.medallion_pipeline.pipeline_state import update_processing_state
from services.medallion_models.bronze_model import BronzeWebEvent
from services.medallion_models.gold_models import GoldProductEvent, GoldPageView
from services.medallion_models.silver_model import SilverWebEvent
from services.medallion_models.helpers import (bronze_to_silver, silver_to_gold_page_view,
                                               silver_to_gold_product)

BRONZE_BUCKET = "events-bronze"
BRONZE_ARCHIVE_BUCKET = "events-bronze-archive"
SILVER_BUCKET = "events-silver"
SILVER_ARCHIVE_BUCKET = "events-silver-archive"

GOLD_PAGE_VIEWS_BUCKET = "events-gold-page-views"
GOLD_PAGE_VIEWS_ARCHIVE_BUCKET = "events-gold-page-views-archive"

GOLD_PRODUCT_VIEWS_BUCKET = "events-gold-product-events"
GOLD_PRODUCT_VIEWS_ARCHIVE_BUCKET = "events-gold-product-events-archive"


def run_bronze_to_silver() -> Dict[str, int]:
    """
    Read all Bronze events from MinIO, transform them to Silver,
    store them in the Silver bucket, and archive processed Bronze files.
    """
    try:
        bronze_files = get_files_data(BRONZE_BUCKET)

        bronze_events: List[BronzeWebEvent] = []
        for f in bronze_files:
            for row in f["data"]:
                # Each row is a flat Bronze event dict → map it to dataclass.
                bronze_events.append(BronzeWebEvent(**row))

        if not bronze_events:
            # Nothing to process.
            return {
                "bronze_count": 0,
                "silver_count": 0,
            }

        # Transform Bronze → Silver via helper func.
        silver_events: List[SilverWebEvent] = []
        for bronze in bronze_events:
            silver = bronze_to_silver(bronze)
            silver_events.append(silver)

        # Convert Silver dataclasses to plain dicts for upload
        silver_records: List[Dict] = [asdict(e) for e in silver_events]

        # upload_batch expects List[List[Dict]] → wrap into one batch.
        upload_batch(batch=[silver_records], bucket_name=SILVER_BUCKET)

    except Exception as e:
        print(f"[ERROR] Bronze→Silver failed: {e}")
        return {"bronze_count": 0, "silver_count": 0}

    # Move processed Bronze files to an archive bucket.
    # This prevents re-processing the same data in the next run
    # move_files_to_another_bucket(BRONZE_BUCKET, BRONZE_ARCHIVE_BUCKET)
    try:
        max_ts = max(e.event_time for e in silver_events)
        update_processing_state(
            dataset="web_events",
            layer="bronze_to_silver",
            last_processed_ts=max_ts,
        )

        for f in bronze_files:
            bronze_object_name = f.get("object_name")
            enqueue_archive_task(
                dataset="web_events",
                layer="bronze",
                partition_key=bronze_object_name,
                event_type="ARCHIVE",
            )
    except Exception as e:

        print(f"[ERROR] Side-effect failed (state/outbox): {e}")

    return {
        "bronze_count": len(bronze_events),
        "silver_count": len(silver_events),
    }


def run_silver_to_gold() -> Dict[str, int]:
    """
    Read all Silver events from MinIO, transform them to Gold models,
    store them in two separate Gold buckets, and archive processed
    Silver files.
    """
    try:
        silver_files = get_files_data(SILVER_BUCKET)

        silver_events: List[SilverWebEvent] = []
        for f in silver_files:
            for row in f["data"]:
                # Each row is a flat Silver event dict → map it to dataclass.
                silver_events.append(SilverWebEvent(**row))

        if not silver_events:
            # Nothing to process.
            return {
                "silver_count": 0,
                "gold_page_views_count": 0,
                "gold_product_events_count": 0,
            }

        # Transform Silver → Gold models.
        page_views: List[GoldPageView] = []
        product_events: List[GoldProductEvent] = []

        for silver in silver_events:
            # Build page-view style gold record (if applicable).
            pv = silver_to_gold_page_view(silver)
            if pv is not None:
                page_views.append(pv)

            # Build product-event style gold record (if applicable).
            pe = silver_to_gold_product(silver)
            if pe is not None:
                product_events.append(pe)

        # Convert Gold dataclasses to plain dicts for upload.
        page_view_records: List[Dict] = [asdict(e) for e in page_views]
        product_event_records: List[Dict] = [asdict(e) for e in product_events]

        page_views_object_name = None
        product_events_object_name = None

        # Upload Gold batches to their buckets (if there is any data).
        # upload_batch expects List[List[Dict]] → wrap into one batch.
        if page_view_records:
            upload_batch(batch=[page_view_records], bucket_name=GOLD_PAGE_VIEWS_BUCKET)

        if product_event_records:
            upload_batch(batch=[product_event_records], bucket_name=GOLD_PRODUCT_VIEWS_BUCKET)

    except Exception as e:
        print(f"[ERROR] Silver→Gold failed: {e}")
        return {"bronze_count": 0, "silver_count": 0}

    # Move processed Silver files to an archive bucket.
    # This prevents re-processing the same data in the next run.
    # move_files_to_another_bucket(SILVER_BUCKET, SILVER_ARCHIVE_BUCKET)

    try:
        max_ts = max(e.event_time for e in silver_events)
        update_processing_state(
            dataset="web_events",
            layer="silver_to_gold",
            last_processed_ts=max_ts,
        )

        for f in silver_files:
            silver_object_name = f.get("object_name")
            enqueue_archive_task(
                dataset="web_events",
                layer="silver",
                partition_key=silver_object_name,
                event_type="ARCHIVE",
            )
    except Exception as e:
        print(f"[ERROR] Side-effect failed (state/outbox): {e}")


    return {
        "silver_count": len(silver_events),
        "gold_page_views_count": len(page_views),
        "gold_product_events_count": len(product_events),
    }