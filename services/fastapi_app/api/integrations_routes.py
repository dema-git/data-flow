##############################################################################
# integrations_routes.py
#
# This module contains API endpoints used by external systems such as Airflow.
# The endpoints run parts of the Medallion ETL pipeline:
# - Bronze → Silver transformation
# - Silver → Gold transformation
# - Loading Gold data into PostgreSQL
# - Running the archive worker for moving files to archive buckets
#
# These endpoints should be called only by automated jobs (Airflow DAGs).
# Calling them manually may create duplicates or break the pipeline flow.
#
# The routes only trigger pipeline steps. All real work happens inside the
# service layer modules, keeping API logic simple and clean.
################################################################################

from fastapi import APIRouter, HTTPException
from minio_utils.files_handler import get_files_data, upload_batch, get_minio_manager
from db_utils.helpers import process_records
from db_utils.database import get_db_session
from minio.error import S3Error
from exceptions_logging.custom_exceptions import MinIOException, KafkaException
from services.faker.config import FakerConfig
from services.faker.generator import SessionEventFaker
from services.medallion_pipeline.medallion_service import (run_bronze_to_silver, run_silver_to_gold, BRONZE_ARCHIVE_BUCKET,
                                                    SILVER_ARCHIVE_BUCKET)
from services.medallion_pipeline.gold_loader import process_gold_outbox_tasks
from services.medallion_pipeline.archive_worker import run_archive_worker
from services.medallion_pipeline.pipeline_state import (
    fail_etl_run,
    finish_etl_run,
    start_etl_run,
)
from services.kafka.producer import KafkaProducerContext



router = APIRouter(tags=["Integrations"])

minio_manager = get_minio_manager()
faker = SessionEventFaker(FakerConfig())
kafka_ctx = KafkaProducerContext()

@router.get(
    "/etl/run-full",
    summary="Run the full ETL pipeline (Bronze → Silver → Gold → DB)",
    description="""
    Runs the complete Medallion ETL process:

    1. Reads Bronze files and converts them into Silver.
    2. Converts Silver records into Gold datasets.
    3. Loads Gold data into PostgreSQL analytical tables.
    4. Creates outbox tasks for archiving processed files.

    This endpoint is called by an Airflow DAG.
    Manual invocation should be performed **only in exceptional cases**, such as debugging or recovery.
    
    WARNING: Manual execution may affect pipeline consistency.
    """
)
def run_full_etl():
    run_id = start_etl_run()
    try:
        bronze_to_silver = run_bronze_to_silver()
        silver_to_gold = run_silver_to_gold()
        gold = process_gold_outbox_tasks()
        result = {
            "run_id": run_id,
            "bronze_to_silver": bronze_to_silver,
            "silver_to_gold": silver_to_gold,
            "gold_loader": gold,
        }
        finish_etl_run(run_id, result)
        return result
    except MinIOException as e:
        fail_etl_run(run_id, e.message)
        raise HTTPException(status_code=500, detail=e.message)
    except KafkaException as e:
        fail_etl_run(run_id, e.message)
        raise HTTPException(status_code=500, detail=e.message)
    except Exception as e:
        fail_etl_run(run_id, str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/outbox/archive-run",
            summary="Run archive worker",
            description="""
                Processes pending outbox tasks and moves MinIO files to archive buckets.
                This endpoint is normally executed by an Airflow DAG.
            
                The worker:
                - Reads pending tasks
                - Moves files from main buckets to archive buckets
                - Marks tasks as DONE or FAILED
                
                 This endpoint is called by an Airflow DAG.
                 Manual invocation should be performed **only in exceptional cases**, such as debugging or recovery.
                
                 WARNING: Manual execution may affect pipeline consistency.
                """,
            )
def trigger_archive_worker():
    result = run_archive_worker(limit=500)
    return result


@router.get("/bronze-archive/cleanup", summary="Clear Bronze archive bucket",
            description="""
            Deletes all objects from the Bronze archive bucket in MinIO.

            This endpoint is intended to be triggered only by an Airflow DAG
            on a strictly defined schedule (fixed interval).

            WARNING: Manual execution may cause unintended data loss
            by deleting archived objects earlier than expected.
            """
            )
def clear_bronze_archive():
    try:
        return minio_manager.delete_all_objects(BRONZE_ARCHIVE_BUCKET)
    except S3Error as e:
        raise HTTPException(status_code=500, detail=f"S3Error {e.code}: {e.message}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/silver-archive/cleanup",
            summary="Clear Silver archive bucket",
            description="""
            Deletes all objects from the Silver archive bucket in MinIO.
            
            This endpoint is intended to be triggered only by an Airflow DAG
            on a strictly defined schedule (fixed interval).
            
            WARNING: Manual execution may cause unintended data loss
            by deleting archived objects earlier than expected.
            """
            )
def clear_silver_archive():
    try:
        return minio_manager.delete_all_objects(SILVER_ARCHIVE_BUCKET)
    except S3Error as e:
        raise HTTPException(status_code=500, detail=f"S3Error {e.code}: {e.message}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
