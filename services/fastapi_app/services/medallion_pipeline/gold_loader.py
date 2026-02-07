
import logging
import math
from dataclasses import asdict
from typing import List, Dict

from sqlalchemy import create_engine, text

from minio_utils.files_handler import get_files_data
from services.medallion_pipeline.medallion_service import GOLD_PAGE_VIEWS_BUCKET
from services.medallion_pipeline.pipeline_state import fetch_pending_tasks, mark_task_done

from services.medallion_pipeline.outbox import enqueue_archive_task

from exceptions_logging.logger import info_logger, error_logger

from services.medallion_models.gold_models import GoldPageView, GoldProductEvent


DATABASE_URL = "postgresql+psycopg2://admin1:pass12345%40@db:5432/main"

engine = create_engine(DATABASE_URL)

BATCH_SIZE = 10_000

def insert_gold_page_views(events: List[GoldPageView]) -> int:
    """
    Insert a list of GoldPageView instances into mart.gold_page_views.
    """
    if not events:
        return 0

    rows = [asdict(e) for e in events]
    total = 0

    info_logger.info(f"[GOLD] insert_page_views: total_rows={len(rows)}")

    with engine.begin() as conn:
        for start in range(0, len(rows), BATCH_SIZE):
            chunk = rows[start : start + BATCH_SIZE]
            info_logger.info(
                f"[GOLD] insert_page_views chunk "
                f"{start}..{start + len(chunk) - 1}"
            )
            conn.execute(
                text(
                    """
                    INSERT INTO mart.gold_page_views (
                        event_time,
                        session_id,
                        user_id,
                        page_url,
                        page_category,
                        page_item,
                        scroll_depth,
                        ab_group
                    )
                    VALUES (
                        :event_time,
                        :session_id,
                        :user_id,
                        :page_url,
                        :page_category,
                        :page_item,
                        :scroll_depth,
                        :ab_group
                    )
                    """
                ),
                chunk,
            )
            total += len(chunk)

    info_logger.info(f"[GOLD] insert_page_views done, inserted={total}")
    return total


def insert_gold_product_events(events: List[GoldProductEvent]) -> int:
    """
    Insert a list of GoldProductEvent instances into mart.gold_product_events.
    """
    if not events:
        return 0

    rows = [asdict(e) for e in events]
    total = 0

    info_logger.info(f"[GOLD] insert_product_events: total_rows={len(rows)}")

    with engine.begin() as conn:
        for start in range(0, len(rows), BATCH_SIZE):
            chunk = rows[start : start + BATCH_SIZE]
            info_logger.info(
                f"[GOLD] insert_product_events chunk "
                f"{start}..{start + len(chunk) - 1}"
            )
            conn.execute(
                text(
                    """
                    INSERT INTO mart.gold_product_events (
                        event_time,
                        session_id,
                        user_id,
                        product_id,
                        price,
                        ab_group,
                        page_url
                    )
                    VALUES (
                        :event_time,
                        :session_id,
                        :user_id,
                        :product_id,
                        :price,
                        :ab_group,
                        :page_url
                    )
                    """
                ),
                chunk,
            )
            total += len(chunk)

    info_logger.info(f"[GOLD] insert_product_events done, inserted={total}")
    return total


GOLD_PAGE_VIEWS_BUCKET = "events-gold-page-views"
GOLD_PRODUCT_VIEWS_BUCKET = "events-gold-product-events"

def clean_nan(d: dict) -> dict:
    cleaned = {}
    for k, v in d.items():
        if isinstance(v, float) and math.isnan(v):
            cleaned[k] = None
        else:
            cleaned[k] = v
    return cleaned

def process_gold_outbox_tasks() -> Dict[str, int]:
    print("GOLD START")
    info_logger.info(
        f"[GOLD] started"
    )


    page_view_files = get_files_data(GOLD_PAGE_VIEWS_BUCKET)
    product_event_files = get_files_data(GOLD_PRODUCT_VIEWS_BUCKET)

    print(
        f"[GOLD] files: page_views={len(page_view_files)}, "
        f"products={len(product_event_files)}"
    )
    page_views: List[GoldPageView] = []
    product_events: List[GoldProductEvent] = []


    for f in page_view_files:
        for row in f["data"]:
            row = clean_nan(row)
            page_views.append(GoldPageView(**row))


    for f in product_event_files:
        for row in f["data"]:
            product_events.append(GoldProductEvent(**row))


    info_logger.info(
        f"[GOLD] page_views: {len(product_events)}"
    )
    info_logger.info(
        f"[GOLD] product_events {len(product_events)}"
    )
    if not page_views and not product_events:
        info_logger.info(
            f"[GOLD] return"
        )
        return {
            "inserted_page_views": 0,
            "inserted_product_events": 0,
            "page_view_files": len(page_view_files),
            "product_event_files": len(product_event_files),
        }

    try:

        info_logger.info(
            f"[GOLD] try to insert"
        )

        inserted_pv = insert_gold_page_views(page_views) if page_views else 0
        inserted_pe = insert_gold_product_events(product_events) if product_events else 0

        info_logger.info(
            f"[GOLD] inserted"
        )

        for f in page_view_files:
            enqueue_archive_task(
                dataset="web_events",
                layer="gold_page_views",
                partition_key=f["object_name"],
                event_type="ARCHIVE",
            )

        for f in product_event_files:
            enqueue_archive_task(
                dataset="web_events",
                layer="gold_product_events",
                partition_key=f["object_name"],
                event_type="ARCHIVE",
            )
        print(f"[GOLD] GOLD DB load success")
    except Exception as e:

        print(f"[ERROR] GOLD DB load failed: {e}")
        return {
            "inserted_page_views": 0,
            "inserted_product_events": 0,
            "page_view_files": len(page_view_files),
            "product_event_files": len(product_event_files),
        }

    return {
        "inserted_page_views": inserted_pv,
        "inserted_product_events": inserted_pe,
        "page_view_files": len(page_view_files),
        "product_event_files": len(product_event_files),
    }