#################################################
# pipeline_state.py
#
# This module manages the processing state and the outbox table
# for the medallion data pipeline.
#
# It is responsible for:
# - Tracking incremental progress (checkpoint logic)
# - Reading pending tasks from the outbox table
# - Updating task status (IN_PROGRESS, DONE, FAILED)
#
# The processing_state table works as a checkpoint mechanism.
# It stores the last successfully processed timestamp and ID
# for each dataset and layer.
#
# The outbox_tasks table is used for background workers
# (for example, archive worker).
#
# All updates are executed inside transactions to keep
# the state consistent.

from dataclasses import dataclass
from typing import Optional, Any
from datetime import datetime
from typing import Optional, List, Dict
import os
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASS = quote_plus(os.getenv("POSTGRES_PASSWORD"))

DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASS}@db:5432/main"

engine = create_engine(DATABASE_URL)

@dataclass
class OutboxTask:
    """
    Represents one row from pipeline.outbox_tasks.

    This object is used by workers to process
    archive or other background tasks.
    """
    id: int
    event_type: str
    dataset: str
    layer: str
    partition_key: str
    status: str
    attempts: int
    last_error: Optional[str]
    created_at: datetime
    updated_at: datetime


def map_outbox_tasks(rows: List[Dict[str, Any]]) -> List[OutboxTask]:
    """
    Convert raw DB rows (dict format) into OutboxTask objects.
    """
    return [OutboxTask(**row) for row in rows]


def update_processing_state(dataset: str, layer: str,
                            last_processed_ts: Optional[datetime] = None,
                            last_processed_id: Optional[int] = None) -> None:
    """
    Update incremental checkpoint for a dataset and layer.

    If the record does not exist, it will be inserted.
    If it exists, it will be updated.

    The timestamp is updated using GREATEST() to ensure
    we never move the checkpoint backwards.
    """
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

def fetch_pending_tasks(*, event_type: Optional[str] = None,
                        dataset: Optional[str] = None, layer: Optional[str] = None,
                        limit: int = 50,) -> List[Dict[str, Any]]:
    """
    Fetch tasks from pipeline.outbox_tasks that are ready to run.

    Conditions:
    - status is PENDING or FAILED
    - attempts < 5

    Optional filters:
    - event_type
    - dataset
    - layer

    Tasks are ordered by updated_at (oldest first).
    """
    base_sql = """
        SELECT 
            id, event_type, dataset, layer, partition_key,
            status, attempts, last_error, created_at, updated_at
        FROM pipeline.outbox_tasks
        WHERE (status = 'PENDING' OR status = 'FAILED')
          AND attempts < 5
    """

    params: Dict[str, Any] = {"limit": limit}

    if event_type:
        base_sql += " AND event_type = :event_type"
        params["event_type"] = event_type
    if dataset:
        base_sql += " AND dataset = :dataset"
        params["dataset"] = dataset
    if layer:
        base_sql += " AND layer = :layer"
        params["layer"] = layer

    base_sql += " ORDER BY updated_at ASC LIMIT :limit"

    query = text(base_sql)

    with engine.begin() as conn:
        rows = conn.execute(query, params).mappings().all()

    return [dict(row) for row in rows]


def mark_task_in_progress(task_id: int) -> bool:
    """"
    Try to mark a task as IN_PROGRESS.

    The update will succeed only if:
    - status is PENDING or FAILED
    - attempts < 5

    Returns True if the task was successfully claimed.
    Returns False if another worker already claimed it.
    """
    query = text(
        """
        UPDATE pipeline.outbox_tasks
        SET status = 'IN_PROGRESS',
            attempts = attempts + 1,
            updated_at = NOW()
        WHERE id = :id
          AND status IN ('PENDING', 'FAILED')
          AND attempts < 5
        """
    )

    with engine.begin() as conn:
        res = conn.execute(query, {"id": task_id})
        return res.rowcount == 1


def mark_task_done(task_id: int) -> None:
    """
    Mark task as DONE.

    This will work only if the task is currently IN_PROGRESS.
    """
    query = text(
        """
        UPDATE pipeline.outbox_tasks
        SET status = 'DONE',
            updated_at = NOW()
        WHERE id = :id
          AND status = 'IN_PROGRESS'
        """
    )

    with engine.begin() as conn:
        conn.execute(query, {"id": task_id})


def mark_task_failed(task_id: int, error_message: str) -> None:
    """
    Mark task as FAILED and store the error message.

    Only tasks in IN_PROGRESS status can be marked as FAILED.
    Error message is limited to 500 characters.
    """
    query = text(
        """
        UPDATE pipeline.outbox_tasks
        SET status = 'FAILED',
            last_error = :err,
            updated_at = NOW()
        WHERE id = :id
          AND status = 'IN_PROGRESS'
        """
    )

    with engine.begin() as conn:
        conn.execute(query, {"id": task_id, "err": error_message[:500]})