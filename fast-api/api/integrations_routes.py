###########################################################################################################
# integrations_routes.py
#
# This module defines endpoints for interacting with external systems such as Kafka and MinIO,
# as well as processing and storing data in PostgreSQL. These endpoints are typically
# triggered by Airflow DAGs as part of the data pipeline.
#
# Endpoints include:
# - /kafka/consumer: Consume messages from Kafka and persist processed data to MinIO.
# - /minino/getallfiles: Fetch all files from the primary MinIO bucket, normalize, and store in PostgreSQL.
# - /minino/movetoarchive: Move all files from the primary MinIO bucket to the archive bucket.
#
# The routes use service layer functions to handle database operations and keeping the
# API layer clean
#
# !!! Manual invocation of these endpoints should only be performed in exceptional cases
# (e.g., debugging or recovery), as it may affect pipeline consistency or result in
# duplicate/inconsistent records. !!!
###########################################################################################################

from fastapi import APIRouter
from kafka_consumer import start_consumer_loop, get_messages
from minio_utils.files import move_files_to_another_bucket, get_files_data
from db_utils.helpers import process_records
from db_utils.database import get_db_session
from models import engine

router = APIRouter(tags=["Integrations"])


start_consumer_loop()

@router.get(
    "/kafka/consumer",
    summary="Consume messages from Kafka and store data in MinIO",
    description="""
    Consumes a batch of messages from the Kafka queue and persists the processed data to a MinIO bucket.
    This endpoint is primarily triggered by an Airflow DAG as part of the data pipeline.
    Manual invocation should be performed **only in exceptional cases**, such as debugging or recovery.
    
    ⚠️ Manual execution may affect pipeline consistency.
    """
)
def kafka_consumer():
    batch = get_messages()
    return {"status": "success", "batch": batch}


@router.get(
    "/minino/getallfiles",
    summary="Fetch and process all files from MinIO bucket",
    description="""
    Fetches all files from the primary MinIO bucket, normalizes the data, 
    and stores the processed records in PostgreSQL.
    This endpoint is primarily triggered by an Airflow DAG as part of the data pipeline.
    Manual invocation should be performed **only in exceptional cases**, such as debugging or recovery.
    
    ⚠️ Manual execution may affect pipeline consistency.
    """
)
def get_files_from_minio_bucket():
    files_data = get_files_data('data-bucket')
    with get_db_session() as db:
        skipped = process_records(db, files_data)
    return {"status": "done", "skipped_sessions": skipped}


@router.get(
    "/minino/movetoarchive",
    summary="Move all files from primary to archive MinIO bucket",
    description="""
    Moves all files from the primary MinIO bucket to the archive bucket.
    This endpoint is primarily triggered by an Airflow DAG as part of the data pipeline.
    Manual invocation should be performed **only in exceptional cases**, such as debugging or recovery.
    
    ⚠️ Manual execution may affect pipeline consistency.
    """
)
def move_all_files_from_primary_to_archive_bucket():
    move_files_to_another_bucket('data-bucket', 'parquet-bucket')
    return {"status": "success"}
