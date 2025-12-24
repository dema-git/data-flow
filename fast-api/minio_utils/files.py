#################################################################
# files.py
#
# MinIO Batch Uploader
#
# Provides utilities to transform nested event data into Parquet files
# and upload, download, move, or delete them in MinIO buckets.
# Uses a global MinioManager with a default configuration.
################################################################

from dataclasses import dataclass
from io import BytesIO
from typing import List, Dict, Any
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile
import os
from datetime import datetime

from .client import get_minio_manager, MinioManager

manager: MinioManager = get_minio_manager()


@dataclass
class BatchUploader:
    minio_manager: MinioManager
    bucket_name: str

    def __post_init__(self):
        self.minio_manager.ensure_bucket(self.bucket_name)

    @staticmethod
    def transform_batch_to_rows(batch: List[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        rows = []
        for records in batch:
            for record in records:
                events = record.get("events", [])
                for event in events:
                    row = {
                        "session_id": record.get("session_id"),
                        "user_id": record.get("user_id"),
                        "timestamp": event.get("timestamp"),
                        "event_type": event.get("type"),
                        "browser": record.get("metadata", {}).get("browser"),
                        "device": record.get("metadata", {}).get("device")
                    }
                    rows.append(row)
        return rows

    def upload_batch(self, batch: List[List[Dict[str, Any]]]):
        if not batch or not batch[0]:
            return
        rows = self.transform_batch_to_rows(batch)
        if not rows:
            return

        df = pd.DataFrame(rows)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')

        with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmp:
            parquet_file = tmp.name
            table = pa.Table.from_pandas(df)
            pq.write_table(table, parquet_file)

        timestamp_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        object_name = f"data_{timestamp_str}.parquet"

        self.minio_manager.upload_file(self.bucket_name, object_name, parquet_file)
        os.remove(parquet_file)


###########################
# helper functions
###########################
def upload_batch(batch: List[List[Dict[str, Any]]], bucket_name="data-bucket"):
    uploader = BatchUploader(manager, bucket_name=bucket_name)
    uploader.upload_batch(batch)


def get_all_files_from_bucket(bucket_name: str):
    """
    Load files from MinIO and process user events.

    - Reads parquet files from MinIO bucket
    - Validates session IDs
    - Creates users, sessions and events in database

    Returns processing status and skipped session IDs.
    """
    return manager.download_all_objects(bucket_name)


def move_files_to_another_bucket(source_bucket: str, target_bucket: str):
    manager.move_objects_to_bucket(source_bucket, target_bucket)


def delete_all_objects(bucket_name: str):
    manager.delete_all_objects(bucket_name)


def get_files_data(bucket_name):
    files = get_all_files_from_bucket(bucket_name)
    arr = []

    for filename, content in files.items():
        df = pd.read_parquet(BytesIO(content))
        arr.append({
            "filename": filename,
            "data": df.to_dict(orient="records")
        })
    return arr