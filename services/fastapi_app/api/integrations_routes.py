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

from fastapi import APIRouter, HTTPException
from kafka_consumer import start_consumer_loop, get_messages
from minio_utils.files import move_files_to_another_bucket, get_files_data
from db_utils.helpers import process_records
from db_utils.database import get_db_session
from models import engine
from exceptions_logging.custom_exceptions import MinIOException, KafkaException


from services.faker.generator import SessionEventFaker
from services.kafka.producer import KafkaProducerContext

router = APIRouter(tags=["Integrations"])


# faker = SessionEventFaker(FakerConfig())
# kafka_ctx = KafkaProducerContext()

@router.get(
    "/kafka/consumer",
    summary="Consume messages from Kafka and store data in MinIO",
    description="""
    Consumes a batch of messages from the Kafka queue and persists the processed data to a MinIO bucket.
    This endpoint is primarily triggered by an Airflow DAG as part of the data pipeline.
    Manual invocation should be performed **only in exceptional cases**, such as debugging or recovery.
    
    WARNING: Manual execution may affect pipeline consistency.
    """
)
def kafka_consumer():
    try:
        batch = get_messages()
        return {"status": "success", "batch": batch}
    except MinIOException as e:
        raise HTTPException(status_code=500, detail=e.message)
    except KafkaException as e:
        raise HTTPException(status_code=500, detail=e.message)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/minino/getallfiles",
    summary="Fetch and process all files from MinIO bucket",
    description="""
    Fetches all files from the primary MinIO bucket, normalizes the data, 
    and stores the processed records in PostgreSQL.
    This endpoint is primarily triggered by an Airflow DAG as part of the data pipeline.
    Manual invocation should be performed **only in exceptional cases**, such as debugging or recovery.
    
    WARNING: Manual execution may affect pipeline consistency.
    """
)
def get_files_from_minio_bucket():
    try:
        files_data = get_files_data('active-bucket')
        with get_db_session() as db:
            skipped = process_records(db, files_data)
        return {"status": "done", "skipped_sessions": skipped}
    except MinIOException as e:
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except KafkaException as e:
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/minino/movetoarchive",
    summary="Move all files from primary to archive MinIO bucket",
    description="""
    Moves all files from the primary MinIO bucket to the archive bucket.
    This endpoint is primarily triggered by an Airflow DAG as part of the data pipeline.
    Manual invocation should be performed **only in exceptional cases**, such as debugging or recovery.
    
    WARNING: Manual execution may affect pipeline consistency.
    """
)
def move_all_files_from_primary_to_archive_bucket():
    try:
        move_files_to_another_bucket('active-bucket', 'archive-bucket')
        return {"status": "success"}
    except MinIOException as e:
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except KafkaException as e:
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
