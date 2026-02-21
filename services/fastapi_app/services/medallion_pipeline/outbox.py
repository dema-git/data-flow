####################################################################
# outbox.py
#
# Outbox pattern implementation for tracking successfully archived files.
#
# This module provides functionality to record files that have been successfully transferred
# from one storage bucket to another. Records are only written to the database AFTER the file
# transfer is confirmed successful, ensuring data consistency.
#
# The outbox table serves as a reliable log of all completed file transfers. Background workers
# use this table to discover which files have been moved to the next layer's archive and need
# to be archived in their original location. This prevents data loss and ensures proper cleanup
# of transferred files.
#
# Key guarantees:
# - Records are only created after successful file transfer
# - Each record confirms a file is safely stored in the destination bucket
# - Workers can reliably find all files ready for archival
####################################################################

from datetime import datetime
from typing import Optional
import os
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = quote_plus(os.getenv("POSTGRES_PASSWORD"))

DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@db:5432/main"

engine = create_engine(DATABASE_URL)


def enqueue_archive_task(dataset: str,layer: str, partition_key: str,
                         event_type: str = "ARCHIVE",
                         next_retry_at: Optional[datetime] = None
                         ) -> None:
    """
    Register a file that has been successfully transferred to the next layer.

    IMPORTANT: This function should ONLY be called after confirming that the file
    has been successfully copied to the destination bucket. The database record
    acts as proof that the file transfer completed successfully and is now ready
    for archival in the source bucket.
    """

    if next_retry_at is None:
        next_retry_at = datetime.now()

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO pipeline.outbox_tasks (
                    event_type, dataset, layer, partition_key,
                    status, attempts, next_retry_at
                )
                VALUES (:event_type, :dataset, :layer, :partition_key,
                        'PENDING', 0, :next_retry_at)
                """
            ),
            {
                "event_type": event_type,
                "dataset": dataset,
                "layer": layer,
                "partition_key": partition_key,
                "next_retry_at": next_retry_at,
            },
        )
