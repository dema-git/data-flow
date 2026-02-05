##############################################################################
# files_handler.py
#
# High-level helpers over MinIO for this project:
# - upload_batch(...)  → write events as Parquet
# - get_files_data(...) → read Parquet into Python dicts
# - move_files_to_another_bucket(...) → bulk move between buckets
# - delete_all_objects(...) → clean bucket
##############################################################################

import os
from io import BytesIO
from typing import List, Dict
import pandas as pd

from .minio_client import get_minio_manager
from .batch_uploader import BatchUploader
from exceptions_logging.custom_exceptions import MinIOException
from exceptions_logging.logger import info_logger, error_logger

manager = get_minio_manager()

def upload_batch(batch: List[List[Dict]], bucket_name="active-bucket") -> None:
    """
    Helper to upload a batch of records to a MinIO bucket
    as a single Parquet file.
    """
    uploader = BatchUploader(manager, bucket_name)
    try:
        uploader.upload_batch(batch)
    except Exception as e:
        print("error: ", e.args)


def get_all_files_from_bucket(bucket_name: str) -> Dict[str, bytes]:
    """
    Return a dictionary where keys are object names and values
    are raw file contents as bytes.
    """
    return manager.download_all_objects(bucket_name)


def get_files_data(bucket_name: str) -> List[Dict]:
    """
    Download all Parquet files from the specified bucket, read them via pandas
    and return a list of dicts:
    {
        "object_name": "YYYY/MM/DD/events_....parquet",
        "filename":    "events_....parquet",
        "data":        [ {column: value, ...}, ... ]
    }
    """
    files = get_all_files_from_bucket(bucket_name)
    arr = []
    for object_name, content in files.items():
        info_logger.info(f"Reading parquet file {object_name} from bucket {bucket_name}")
        try:
            df = pd.read_parquet(BytesIO(content))
        except Exception as e:
            error_logger.exception(
                f"Failed to read parquet file '{object_name}' from bucket '{bucket_name}'"
            )
            raise MinIOException(
                f"Failed to read parquet file '{object_name}' from bucket '{bucket_name}'"
            ) from e


        arr.append({
            "object_name": object_name,
            "filename": os.path.basename(object_name),
            "data": df.to_dict(orient="records"),
        })
        info_logger.info(f"Successfully read file {object_name} with {len(df)} records")

    return arr


def move_files_to_another_bucket(source_bucket: str, target_bucket: str) -> None:
    """
    Move all files from one bucket to another (bulk).
    """
    manager.move_objects_to_bucket(source_bucket, target_bucket)


def delete_all_objects(bucket_name: str) -> None:
    """
    Delete all objects from a bucket.
    """
    manager.delete_all_objects(bucket_name)