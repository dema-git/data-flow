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
import tempfile
import os
from exceptions_logging.custom_exceptions import MinIOException
from .minio_client import MinioManager
from exceptions_logging.logger import info_logger, error_logger, warn_logger


@dataclass
class BatchUploader:
    minio_manager: MinioManager
    bucket_name: str

    def __post_init__(self):
        # make sure bucket exists
        self.minio_manager.ensure_bucket(self.bucket_name)

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

    def upload_batch(self, batch: List[List[Dict[str, Any]]]):
        """
        - flatten batch
        - make pandas DataFrame
        - write to parquet temp file
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
                # simple parquet writer via pandas (uses pyarrow/fastparquet under the hood)
                df.to_parquet(parquet_file, index=False)

            object_name = f"events_{datetime.utcnow():%Y%m%d_%H%M%S_%f}.parquet"
            info_logger.info(
                f"Uploading parquet to MinIO bucket={self.bucket_name}, object_name={object_name}"
            )
            self.minio_manager.upload_file(self.bucket_name, object_name, parquet_file)
            info_logger.info("Upload to MinIO finished")

        except Exception as e:
            error_logger.exception("Failed to upload batch to MinIO")
            raise MinIOException("Failed to upload batch to MinIO") from e

        finally:
            if parquet_file and os.path.exists(parquet_file):
                os.remove(parquet_file)