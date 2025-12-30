"""
MinIO Batch Uploader

Provides classes and functions to upload event data batches to MinIO.
- MinioConfig: holds connection details.
- MinioManager: handles bucket creation and file uploads.
- BatchUploader: transforms nested event data to Parquet and uploads.
- upload_batch: helper function to upload a batch (used in kafka_consumer.py).
"""

from dataclasses import dataclass
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from minio import Minio
from minio.commonconfig import CopySource
from datetime import datetime
import tempfile
import os
from typing import List, Dict, Any
from exceptions_logging.custom_exceptions import MinIOException
from exceptions_logging.logger import info_logger, error_logger, warn_logger

@dataclass
class MinioConfig:
    host: str
    access_key: str
    secret_key: str
    secure: bool = False


@dataclass
class MinioManager:
    config: MinioConfig

    def __post_init__(self):
        info_logger.info(
            "Initializing MinIO client"
        )
        try:
            self.client = Minio(
                self.config.host,
                access_key=self.config.access_key,
                secret_key=self.config.secret_key,
                secure=self.config.secure
            )
        except Exception as e:
            error_logger.exception(
                f"Failed to initialize MinIO client: {e.args}"
            )
            raise MinIOException("Failed to initialize MinIO client") from e

    def ensure_bucket(self, bucket_name: str):
        # Create bucket if it doesn't exist
        info_logger.info(
            "Ensuring the MinIO bucket"
        )
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
        except Exception as e:
            error_logger.exception(
                f"Failed to ensure bucket: {e.args}"
            )
            raise MinIOException(f"Failed to ensure bucket '{bucket_name}'") from e

    def upload_file(self, bucket_name: str, object_name: str, file_path: str):
        info_logger.info(
            f"Uploading files '{object_name}' to bucket '{bucket_name}'"
        )
        try:
            with open(file_path, "rb") as f:
                self.client.put_object(
                    bucket_name=bucket_name,
                    object_name=object_name,
                    data=f,
                    length=os.path.getsize(file_path),
                    content_type="application/octet-stream"
            )
        except Exception as e:
            error_logger.exception(
                f"Failed to upload '{object_name}' to bucket '{bucket_name}': {e.args}"
            )
            raise MinIOException(f"Failed to upload '{object_name}' to bucket '{bucket_name}'") from e


    def download_all_objects(self, bucket_name: str, download_path: str = None) -> dict:
        objects_data = {}
        info_logger.info(
            f"Downloading objects from bucket '{bucket_name}'"
        )
        for obj in self.client.list_objects(bucket_name, recursive=True):
            response = None
            try:
                response = self.client.get_object(bucket_name, obj.object_name)
                if download_path:

                    file_path = os.path.join(download_path, obj.object_name)
                    os.makedirs(os.path.dirname(file_path), exist_ok=True)
                    with open(file_path, "wb") as f:
                        for chunk in response.stream(32 * 1024):
                            f.write(chunk)
                else:
                    objects_data[obj.object_name] = response.read()

            except Exception as e:
                error_logger.exception(
                    f"Failed to download object '{obj.object_name}': {e.args}"
                )
                raise MinIOException(f"Failed to download object '{obj.object_name}'") from e
            finally:
                if response:
                    response.close()

        return objects_data


    def delete_all_objects(self, bucket_name: str):
        info_logger.info(
            f"Deleting objects from bucket '{bucket_name}'"
        )
        try:
            objects_to_delete = [obj.object_name for obj in self.client.list_objects(bucket_name, recursive=True)]
            if objects_to_delete:
                delete_gen = map(lambda name: {"ObjectName": name}, objects_to_delete)
                self.client.remove_objects(bucket_name, delete_gen)
                info_logger.info(
                    f"All objects were deleted from bucket '{bucket_name}'"
                )
            else:
                return
        except Exception as e:
            error_logger.exception(
                f"Failed to delete objects from bucket '{bucket_name}': {e.args}"
            )
            raise MinIOException(f"Failed to delete objects from bucket '{bucket_name}'") from e


    def move_objects_to_bucket(self, source_bucket: str, target_bucket: str):
        info_logger.info(
            f"Moving objects objects from '{source_bucket}' to '{target_bucket}'"
        )
        try:
            self.ensure_bucket(target_bucket)
            objects = self.client.list_objects(source_bucket, recursive=True)
            for obj in objects:

                source = CopySource(source_bucket, obj.object_name)
                result = self.client.copy_object(
                    bucket_name=target_bucket,
                    object_name=obj.object_name,
                    source=source
                )
                self.client.remove_object(source_bucket, obj.object_name)
        except Exception as e:
            error_logger.exception(
                f"Failed to move objects from '{source_bucket}' to '{target_bucket}': {e.args}"
            )
            raise MinIOException(f"Failed to move objects from '{source_bucket}' to '{target_bucket}'") from e

@dataclass
class BatchUploader:
    minio_manager: MinioManager
    bucket_name: str

    def __post_init__(self):
        # Ensure the bucket exists on init
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
        # Skip empty batches
        if not batch or not batch[0]:
            warn_logger.warning("Warning: empty batch, nothing to upload")
            return

        # Transform batch to flat rows
        rows = self.transform_batch_to_rows(batch)
        if not rows:
            warn_logger.warning("no events found in batch")
            return

        # Create DataFrame и convert to timestamp
        info_logger.info(
            f"Uploading batch to MinIO"
        )
        try:
            df = pd.DataFrame(rows)
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')

            # Write DataFrame to temporary Parquet file
            with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmp:
                parquet_file = tmp.name
                table = pa.Table.from_pandas(df)
                pq.write_table(table, parquet_file)

            # Prepare object name and upload
            timestamp_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            object_name = f"data_{timestamp_str}.parquet"

            # Upload file
            self.minio_manager.upload_file(self.bucket_name, object_name, parquet_file)
        except Exception as e:
            error_logger.exception(
                f"Failed to upload batch to MinIO: {e.args}"
            )
            raise MinIOException("Failed to upload batch to MinIO") from e
        finally:
            # Remove temporary file
            os.remove(parquet_file)


def upload_batch(batch: List[List[Dict[str, Any]]]):
    config = MinioConfig(
        host="minio:9010",
        access_key="admin",
        secret_key="pass12345@",
        secure=False
    )

    # Initialize manager and uploader
    minio_mgr = MinioManager(config)
    uploader = BatchUploader(minio_mgr, bucket_name="active-bucket")

    # Upload the batch (used in kafka_consumer.py at /kafka/consumer Endpoint)
    uploader.upload_batch(batch)


def get_all_files_from_bucket(bucket_name: str):
    config = MinioConfig(
        host="minio:9010",
        access_key="admin",
        secret_key="pass12345@",
        secure=False
    )
    manager = MinioManager(config)
    #  Get all objects as bytes in memory
    all_objects = manager.download_all_objects("active-bucket")

    return all_objects


def move_files_to_another_bucket(source_bucket: str, target_bucket: str) -> None:
    config = MinioConfig(
        host="minio:9010",
        access_key="admin",
        secret_key="pass12345@",
        secure=False
    )
    manager = MinioManager(config)

    manager.move_objects_to_bucket(source_bucket, target_bucket)


def delete_all_objects(bucket_name: str) -> None:
    config = MinioConfig(
        host="minio:9010",
        access_key="admin",
        secret_key="pass12345@",
        secure=False
    )
    manager = MinioManager(config)

    manager.delete_all_objects(bucket_name)