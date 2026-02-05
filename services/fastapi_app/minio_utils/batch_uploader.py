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
import tempfile
import os
from exceptions_logging.custom_exceptions import MinIOException
from .minio_client import MinioManager
from exceptions_logging.logger import info_logger, error_logger, warn_logger


@dataclass
class BatchUploader:
    """
    Helper class to upload batched events into MinIO as Parquet files.
    """
    minio_manager: MinioManager
    bucket_name: str

    def __post_init__(self):
        # make sure bucket exists
        self.minio_manager.ensure_bucket(self.bucket_name)

    @staticmethod
    def build_dated_object_name(base_prefix: str = "events") -> str:
        """
        Build object name with date-based prefix:
        <base_prefix>/YYYY/MM/DD/events_YYYYMMDD_HHMMSS_micro.parquet

        Example:
            2026/02/05/events_20260205_192555_376174.parquet
        """
        now = datetime.now()
        date_prefix = now.strftime("%Y/%m/%d")
        file_name = now.strftime(f"{base_prefix}_%Y%m%d_%H%M%S_%f.parquet")
        return f"{date_prefix}/{file_name}"


    @staticmethod
    def transform_batch_to_rows(batch: List[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """
        Flatten nested batch [[dict, dict...], [...]] -> [dict, dict...]
        """
        rows: List[Dict[str, Any]] = []
        for records in batch:
            for record in records:
                rows.append(record)
        return rows

    def upload_batch(self, batch: List[List[Dict[str, Any]]]) -> None:
        """
        - flatten batch
        - make pandas DataFrame
        - write to Parquet temporary file
        - upload file to MinIO
        """
        if not batch or not batch[0]:
            info_logger.info("Empty batch, skipping upload.")
            return

        rows = self.transform_batch_to_rows(batch)
        if not rows:
            info_logger.info("No rows after transform, skipping upload.")
            return

        parquet_file = None

        try:
            info_logger.info(f"Building DataFrame for {len(rows)} rows (bucket={self.bucket_name})")

            df = pd.DataFrame(rows)

            with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmp:
                parquet_file = tmp.name
                # simple parquet writer via pandas
                df.to_parquet(parquet_file, index=False)

            object_name = self.build_dated_object_name(base_prefix="events")
            info_logger.info(
                f"Uploading parquet to MinIO bucket={self.bucket_name}, object_name={object_name}"
            )
            self.minio_manager.upload_file(self.bucket_name, object_name, parquet_file)
            info_logger.info("Upload to MinIO finished")


        except Exception as e:
            error_logger.exception(f"Failed to upload batch to MinIO {e.args}")
            raise MinIOException("Failed to upload batch to MinIO") from e

        finally:
            if parquet_file and os.path.exists(parquet_file):
                os.remove(parquet_file)
