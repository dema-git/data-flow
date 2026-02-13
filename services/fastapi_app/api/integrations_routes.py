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
from minio_utils.files_handler import get_files_data, upload_batch
from db_utils.helpers import process_records
from db_utils.database import get_db_session
from exceptions_logging.custom_exceptions import MinIOException, KafkaException
from services.faker.config import FakerConfig
from services.faker.generator import SessionEventFaker
from services.medallion_pipeline.medallion_service import run_bronze_to_silver, run_silver_to_gold
from services.medallion_pipeline.gold_loader import process_gold_outbox_tasks
from services.medallion_pipeline.archive_worker import run_archive_worker
from services.kafka.producer import KafkaProducerContext


router = APIRouter(tags=["Integrations"])

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
    try:
        run_bronze_to_silver()
        run_silver_to_gold()
        process_gold_outbox_tasks()
    except MinIOException as e:
        raise HTTPException(status_code=500, detail=e.message)
    except KafkaException as e:
        raise HTTPException(status_code=500, detail=e.message)
    except Exception as e:
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
