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
from dataclasses import asdict

from fastapi import APIRouter, HTTPException

# from services.medallion_pipeline.medallion_service import get_medallion_layers
from minio_utils.files_handler import move_files_to_another_bucket, get_files_data, upload_batch
from db_utils.helpers import process_records
from db_utils.database import get_db_session
from models import engine
from exceptions_logging.custom_exceptions import MinIOException, KafkaException
from services.faker.config import FakerConfig

from services.faker.generator import SessionEventFaker
from services.medallion_pipeline.medallion_service import run_bronze_to_silver, run_silver_to_gold

from services.kafka.producer import KafkaProducerContext
from services.medallion_models.bronze_model import BronzeWebEvent
from services.medallion_models.gold_models import GoldPageView, GoldProductEvent
from services.medallion_models.silver_model import SilverWebEvent

router = APIRouter(tags=["Integrations"])



def upload_bronze_events(events: list[BronzeWebEvent]) -> None:
    payload = [asdict(e) for e in events]
    upload_batch([payload], bucket_name="events-bronze")


def upload_silver_events(events: list[SilverWebEvent]) -> None:
    payload = [asdict(e) for e in events]
    upload_batch([payload], bucket_name="events-silver")


def upload_gold_page_views(events: list[GoldPageView]) -> None:
    payload = [asdict(e) for e in events]
    upload_batch([payload], bucket_name="events-gold-page-views")


def upload_gold_product_events(events: list[GoldProductEvent]) -> None:
    payload = [asdict(e) for e in events]
    upload_batch([payload], bucket_name="events-gold-product-events")

faker = SessionEventFaker(FakerConfig())
kafka_ctx = KafkaProducerContext()

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
        run_bronze_to_silver()
        run_silver_to_gold()
    except MinIOException as e:
        raise HTTPException(status_code=500, detail=e.message)
    except KafkaException as e:
        raise HTTPException(status_code=500, detail=e.message)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# @router.get(
#     "/minino/getallfiles",
#     summary="Fetch and process all files from MinIO bucket",
#     description="""
#     Fetches all files from the primary MinIO bucket, normalizes the data,
#     and stores the processed records in PostgreSQL.
#     This endpoint is primarily triggered by an Airflow DAG as part of the data pipeline.
#     Manual invocation should be performed **only in exceptional cases**, such as debugging or recovery.
#
#     WARNING: Manual execution may affect pipeline consistency.
#     """
# )
# def get_files_from_minio_bucket():
#     try:
#         files_data = get_files_data('active-bucket')
#         with get_db_session() as db:
#             skipped = process_records(db, files_data)
#         return {"status": "done", "skipped_sessions": skipped}
#     except MinIOException as e:
#         raise HTTPException(status_code=e.status_code, detail=e.message)
#     except KafkaException as e:
#         raise HTTPException(status_code=e.status_code, detail=e.message)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))
#
#
# @router.get(
#     "/minino/movetoarchive",
#     summary="Move all files from primary to archive MinIO bucket",
#     description="""
#     Moves all files from the primary MinIO bucket to the archive bucket.
#     This endpoint is primarily triggered by an Airflow DAG as part of the data pipeline.
#     Manual invocation should be performed **only in exceptional cases**, such as debugging or recovery.
#
#     WARNING: Manual execution may affect pipeline consistency.
#     """
# )
# def move_all_files_from_primary_to_archive_bucket():
#     try:
#         move_files_to_another_bucket('active-bucket', 'archive-bucket')
#         return {"status": "success"}
#     except MinIOException as e:
#         raise HTTPException(status_code=e.status_code, detail=e.message)
#     except KafkaException as e:
#         raise HTTPException(status_code=e.status_code, detail=e.message)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))
