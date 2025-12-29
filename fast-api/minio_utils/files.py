##############################################################################
# files.py
#
# This module provides helper functions for working with batches of files
# stored in MinIO. It wraps MinIO manager and batch uploader
# logic and exposes simple, task-oriented functions.
##############################################################################

from io import BytesIO
from typing import List, Dict
import pandas as pd

from .minio_client import get_minio_manager
from .batch_uploader import BatchUploader
from exceptions_logging.custom_exceptions import MinIOException

manager = get_minio_manager()

def upload_batch(batch: List[List[Dict]], bucket_name="active-bucket"):
    # Upload a batch of data to a MinIO bucket
    uploader = BatchUploader(manager, bucket_name)
    uploader.upload_batch(batch)


def get_all_files_from_bucket(bucket_name: str) -> Dict[str, bytes]:
    # Returns a dictionary where keys are object names and values
    # are raw file contents as bytes.
    return manager.download_all_objects(bucket_name)


def get_files_data(bucket_name: str) -> List[Dict]:
    # Downloads all files from the specified bucket, reads them as
    # Parquet files using pandas, and returns their contents as
    # Python dictionaries
    files = get_all_files_from_bucket(bucket_name)
    arr = []
    for filename, content in files.items():
        try:
            df = pd.read_parquet(BytesIO(content))
        except Exception as e:
            raise MinIOException(
                f"Failed to read parquet file '{filename}' from bucket '{bucket_name}'"
            ) from e
        arr.append({
            "filename": filename,
            "data": df.to_dict(orient="records")
        })
    return arr


def move_files_to_another_bucket(source_bucket: str, target_bucket: str):
    # Move all files from one bucket to another
    manager.move_objects_to_bucket(source_bucket, target_bucket)


def delete_all_objects(bucket_name: str):
    # Delete all objects from a bucket.
    # If the bucket is empty, no action is performed.
    manager.delete_all_objects(bucket_name)