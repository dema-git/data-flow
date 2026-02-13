from typing import Dict

from minio_utils.minio_client import get_minio_manager
from services.medallion_pipeline.pipeline_state import (
    fetch_pending_tasks,
    mark_task_in_progress,
    mark_task_done,
    mark_task_failed,
    OutboxTask,
    map_outbox_tasks,
)

from services.medallion_pipeline.medallion_service import (
    BRONZE_BUCKET,
    BRONZE_ARCHIVE_BUCKET,
    SILVER_BUCKET,
    SILVER_ARCHIVE_BUCKET,
    GOLD_PAGE_VIEWS_BUCKET,
    GOLD_PAGE_VIEWS_ARCHIVE_BUCKET,
    GOLD_PRODUCT_VIEWS_BUCKET,
    GOLD_PRODUCT_VIEWS_ARCHIVE_BUCKET,
)


minio_manager = get_minio_manager()


BUCKET_ROUTING: Dict[tuple, tuple] = {
    ("web_events", "bronze"): (BRONZE_BUCKET, BRONZE_ARCHIVE_BUCKET),
    ("web_events", "silver"): (SILVER_BUCKET, SILVER_ARCHIVE_BUCKET),
    ("web_events", "gold_page_views"): (GOLD_PAGE_VIEWS_BUCKET, GOLD_PAGE_VIEWS_ARCHIVE_BUCKET),
    ("web_events", "gold_product_events"): (GOLD_PRODUCT_VIEWS_BUCKET, GOLD_PRODUCT_VIEWS_ARCHIVE_BUCKET),
}


def run_archive_worker(limit: int = 50) -> Dict[str, int]:


    raw_rows = fetch_pending_tasks(
        event_type="ARCHIVE",
        dataset="web_events",
        limit=limit,
    )

    tasks = map_outbox_tasks(raw_rows)

    if not tasks:
        return {"taken": 0, "archived": 0, "failed": 0, "skipped": 0}

    archived = 0
    failed = 0
    skipped = 0

    for task in tasks:
        route_key = (task.dataset, task.layer)
        buckets = BUCKET_ROUTING.get(route_key)


        if not buckets:
            msg = f"Unknown dataset/layer for archive: {task.dataset}/{task.layer}"
            mark_task_failed(task.id, msg)
            failed += 1
            continue

        source_bucket, archive_bucket = buckets

        try:
            mark_task_in_progress(task.id)


            minio_manager.move_single_object(
                source_bucket=source_bucket,
                target_bucket=archive_bucket,
                object_name=task.partition_key,
            )


            mark_task_done(task.id)
            archived += 1

        except Exception as e:
            mark_task_failed(task.id, str(e))
            failed += 1

    return {
        "taken": len(tasks),
        "archived": archived,
        "failed": failed,
        "skipped": skipped,
    }
