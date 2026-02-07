#################################################
# pipeline_state.py
#
# Processing state tracker for incremental data pipeline processing.
#
# This module maintains a record of the last successfully processed record for each
# dataset and layer combination. It enables incremental processing by tracking both
# the timestamp and ID of the last processed record, allowing subsequent pipeline
# runs to resume from where they left off.
#
# The processing_state table acts as a checkpoint mechanism. Each time a batch of
# records is successfully processed, this function updates the checkpoint to reflect
# the latest processed record.
#
# Key features:
# - Tracks processing progress per dataset/layer combination
# - Uses timestamp and ID for reliable incremental processing
# - Updates are atomic to prevent partial state corruption
# - Automatically takes the maximum timestamp to handle concurrent update
####################################################################

from datetime import datetime
from typing import Optional, List, Dict

from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql+psycopg2://admin1:pass12345%40@db:5432/main"

engine = create_engine(DATABASE_URL)


def update_processing_state(dataset: str, layer: str,
                            last_processed_ts: Optional[datetime] = None,
                            last_processed_id: Optional[int] = None) -> None:

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO pipeline.processing_state (
                    dataset, layer, last_processed_ts, last_processed_id
                )
                VALUES (:dataset, :layer, :ts, :id)
                ON CONFLICT (dataset, layer)
                DO UPDATE
                SET
                    last_processed_ts = GREATEST(
                        COALESCE(pipeline.processing_state.last_processed_ts, '-infinity'::timestamp),
                        COALESCE(EXCLUDED.last_processed_ts, '-infinity'::timestamp)
                    ),
                    last_processed_id = COALESCE(EXCLUDED.last_processed_id,
                                                 pipeline.processing_state.last_processed_id),
                    updated_at = NOW()
                """
            ),
            {
                "dataset": dataset,
                "layer": layer,
                "ts": last_processed_ts,
                "id": last_processed_id,
            },
        )

def fetch_pending_tasks(limit: int = 50) -> List[Dict]:
    """
    Fetch tasks from pipeline.outbox_tasks that are ready for execution.
    Conditions:
      - status = 'PENDING' or 'FAILED'
      - attempts < 5
    """
    query = text(
        """
        SELECT 
            id, event_type, dataset, layer, partition_key,
            status, attempts, last_error, created_at, updated_at
        FROM pipeline.outbox_tasks
        WHERE (status = 'PENDING' OR status = 'FAILED')
          AND layer = 'silver' attempts < 5
        ORDER BY updated_at ASC
        LIMIT :limit
        """
    )

    with engine.begin() as conn:
        rows = conn.execute(query, {"limit": limit}).mappings().all()

    return [dict(row) for row in rows]


def mark_task_in_progress(task_id: int) -> None:
    """
    Mark the task as IN_PROGRESS.
    """
    query = text(
        """
        UPDATE pipeline.outbox_tasks
        SET status = 'IN_PROGRESS',
            attempts = attempts + 1,
            updated_at = NOW()
        WHERE id = :id
        """
    )

    with engine.begin() as conn:
        conn.execute(query, {"id": task_id})


def mark_task_done(task_id: int) -> None:
    """
    Mark the task as DONE after successful processing.
    """
    query = text(
        """
        UPDATE pipeline.outbox_tasks
        SET status = 'DONE',
            updated_at = NOW()
        WHERE id = :id
        """
    )

    with engine.begin() as conn:
        conn.execute(query, {"id": task_id})


def mark_task_failed(task_id: int, error_message: str) -> None:
    """
    Mark task as FAILED and store the error message.
    """
    query = text(
        """
        UPDATE pipeline.outbox_tasks
        SET status = 'FAILED',
            last_error = :err,
            updated_at = NOW()
        WHERE id = :id
        """
    )

    with engine.begin() as conn:
        conn.execute(query, {"id": task_id, "err": error_message[:500]})