##################################################################################
# minio_client.py
#
# Provides MinioConfig and MinioManager for basic bucket and object operations:
# upload, download, move and delete objects.
###################################################################################

from dataclasses import dataclass
from minio import Minio
from minio.commonconfig import CopySource
import os

@dataclass
class MinioConfig:
    """
    MinIO connection configuration.
    All required parameters to initialize a MinIO client instance.
    """
    host: str = "minio:9010"
    access_key: str = "admin"
    secret_key: str = "pass12345@"
    secure: bool = False

@dataclass
class MinioManager:
    """
    MinIO client wrapper.

    Provides high-level methods for managing buckets and objects
    using an initialized MinIO client.
    """
    config: MinioConfig

    def __post_init__(self):
        # Initialize MinIO client
        self.client = Minio(
            self.config.host,
            access_key=self.config.access_key,
            secret_key=self.config.secret_key,
            secure=self.config.secure
        )

    def ensure_bucket(self, bucket_name: str):
        # Create bucket if it does not exist.
        if not self.client.bucket_exists(bucket_name):
            self.client.make_bucket(bucket_name)

    def upload_file(self, bucket_name: str, object_name: str, file_path: str):
        # Upload file to a bucket.
        with open(file_path, "rb") as f:
            self.client.put_object(
                bucket_name=bucket_name,
                object_name=object_name,
                data=f,
                length=os.path.getsize(file_path),
                content_type="application/octet-stream"
            )
        print(f"Uploaded {object_name} to bucket {bucket_name}")


    def download_all_objects(self, bucket_name: str, download_path: str = None) -> dict:
        # Download all objects from a bucket
        objects_data = {}
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
            finally:
                if response:
                    response.close()
        return objects_data


    def delete_all_objects(self, bucket_name: str):
        # Delete all objects in a bucket.
        objects_to_delete = [obj.object_name for obj in self.client.list_objects(bucket_name, recursive=True)]
        if objects_to_delete:
            delete_gen = map(lambda name: {"ObjectName": name}, objects_to_delete)
            self.client.remove_objects(bucket_name, delete_gen)
            print(f"Deleted all objects from bucket '{bucket_name}'")
        else:
            print(f"Bucket '{bucket_name}' is already empty.")


    def move_objects_to_bucket(self, source_bucket: str, target_bucket: str):
        # Move all objects from one bucket to another
        self.ensure_bucket(target_bucket)
        for obj in self.client.list_objects(source_bucket, recursive=True):
            source = CopySource(source_bucket, obj.object_name)
            result = self.client.copy_object(bucket_name=target_bucket, object_name=obj.object_name, source=source)
            print(f"Copied '{obj.object_name}' to '{target_bucket}' (version: {result.version_id})")
            self.client.remove_object(source_bucket, obj.object_name)
            print(f"Removed '{obj.object_name}' from '{source_bucket}'")


def get_minio_manager(config: MinioConfig = None) -> MinioManager:
    # Return initialized MinioManager instance
    if config is None:
        config = MinioConfig()
    return MinioManager(config)