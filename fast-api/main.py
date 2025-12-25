from fastapi import FastAPI
import pandas as pd
from db_utils.helpers import process_records
from db_utils.database import get_db_session
from kafka_consumer import start_consumer_loop, get_messages
from minio_utils.files import move_files_to_another_bucket, get_files_data

app = FastAPI()

start_consumer_loop()

@app.get("/")
def homepage():
    return {"page": "Homepage"}


@app.get("/page/{number}")
def homepage_number(number: int):
    return {"page": f"Homepage {number}"}


@app.get("/kafka/consumer")
def kafka_consumer():
    batch = get_messages()
    return {"status": "success" ,
            "batch": batch}


@app.get("/minino/getallfiles")
def get_files_from_minio_bucket():
    # Get files from MinIO bucket
    files_data = get_files_data('data-bucket')

    # Process records in DB
    with get_db_session() as db:
        skipped = process_records(db, files_data)

    return {"status": "done", "skipped_sessions": skipped}


@app.get("/minino/movetoarchive")
def move_all_files_from_primary_to_archive_bucket():
    move_files_to_another_bucket('data-bucket', 'parquet-bucket')
    return {"status": "success"}


@app.get("/minino/deleteall")
def delete_all_files_from_minio_primary_bucket():
    pass