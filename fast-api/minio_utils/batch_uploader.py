########################################################################
# batch_uploader.py
#
# This module defines a BatchUploader class responsible for transforming
# incoming batched event data into a tabular format and uploading it to
# MinIO as Parquet files.
########################################################################

from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Any
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile
import os
from exceptions_logging.custom_exceptions import MinIOException
from .minio_client import MinioManager

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
            print("Empty batch, skipping upload.")
            return

        rows = self.transform_batch_to_rows(batch)
        if not rows:
            print("No events found, skipping upload.")
            return
        try:
            df = pd.DataFrame(rows)
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')

            with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmp:
                parquet_file = tmp.name
                table = pa.Table.from_pandas(df)
                pq.write_table(table, parquet_file)

            object_name = f"data_{datetime.now():%Y-%m-%d_%H-%M-%S}.parquet"
            self.minio_manager.upload_file(self.bucket_name, object_name, parquet_file)
        except Exception as e:
            raise MinIOException("Failed to upload batch to MinIO") from e

        finally:
            os.remove(parquet_file)
