from fastapi import FastAPI, HTTPException, Query
import pandas as pd

from db_utils.helpers import process_records
from db_utils.database import get_db_session
from kafka_consumer import start_consumer_loop, get_messages
from minio_utils.files import move_files_to_another_bucket, get_files_data
from sqlalchemy import select, create_engine
from sqlalchemy.orm import Session
import uuid


engine = create_engine(
    "postgresql://admin1:pass12345%40@db:5432/main",
    echo=True
)

from models import User, Session as SessionModel, Event


app = FastAPI()

start_consumer_loop()

@app.get("/", tags=["Home/Dashboard"])
def homepage():
    return {"page": "Homepage"}


@app.get("/kafka/consumer", tags=['Integration'],
         summary="Consume messages from Kafka and store data in MinIO",
         description="""
         Consumes a batch of messages from the Kafka queue and persists the processed data to a MinIO bucket.
         This endpoint is primarily triggered by an Airflow DAG as part of the data pipeline.
         Manual invocation should be performed **only in exceptional cases**, such as debugging or recovery.
        
         WARNING: Manual execution may affect pipeline consistency.
         """
         )
def kafka_consumer():
    batch = get_messages()
    return {"status": "success" ,
            "batch": batch}


@app.get("/minino/getallfiles", tags=['Integration'],
         summary="Fetch and process all files from MinIO bucket",
         description="""
         Fetches all files from the primary MinIO bucket, normalizes the data, 
         and stores the processed records in PostgreSQL.
         This endpoint is primarily triggered by an Airflow DAG as part of the data pipeline.
         Manual invocation should be performed **only in exceptional cases**, such as debugging or recovery.

         ⚠️ Manual execution may affect pipeline consistency and may lead to duplicate or inconsistent records if run out of sequence.
         """
         )
def get_files_from_minio_bucket():
    # Get files from MinIO bucket
    files_data = get_files_data('data-bucket')

    # Process records in DB
    with get_db_session() as db:
        skipped = process_records(db, files_data)

    return {"status": "done", "skipped_sessions": skipped}


@app.get("/minino/movetoarchive", tags=['Integration'],
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


###########################


@app.get("/users", tags=["Users"])
def get_users(skip: int = 0, limit: int = 20):
    with Session(engine) as db:
        users = db.execute(select(User).offset(skip).limit(limit)).scalars().all()
        return [{"user_id": u.user_id} for u in users]


@app.get("/users/{user_id}", tags=["Users"])
def get_user(user_id: int):
    with Session(engine) as db:
        user = db.get(User, user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        return {
            "user_id": user.user_id,
            "sessions": [{"session_id": s.session_id} for s in user.sessions]
        }


@app.get("/users/{user_id}/sessions", tags=["Users"])
def get_user_sessions(user_id: int):
    with Session(engine) as db:
        user = db.get(User, user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        return [{"session_id": s.session_id, "browser": s.browser, "device": s.device} for s in user.sessions]



@app.get("/sessions", tags=["Sessions"])
def get_sessions(skip: int = 0, limit: int = 20):
    with Session(engine) as db:
        sessions = db.execute(select(SessionModel).offset(skip).limit(limit)).scalars().all()
        return [{"session_id": s.session_id, "user_id": s.user_id} for s in sessions]


@app.get("/sessions/{session_id}", tags=["Sessions"])
def get_session(session_id: str):
    try:
        sid = uuid.UUID(session_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid session_id")
    with Session(engine) as db:
        session = db.get(SessionModel, sid)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        return {
            "session_id": session.session_id,
            "user_id": session.user_id,
            "browser": session.browser,
            "device": session.device,
            "events": [{"event_id": e.event_id, "type": e.type, "timestamp": e.timestamp} for e in session.events]
        }


@app.get("/sessions/user/{user_id}", tags=["Sessions"])
def get_sessions_by_user(user_id: int):
    with Session(engine) as db:
        user = db.get(User, user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        return [{"session_id": s.session_id, "browser": s.browser, "device": s.device} for s in user.sessions]



@app.get("/events", tags=["Events"])
def get_events(skip: int = 0, limit: int = 50, type: str | None = None):
    with Session(engine) as db:
        stmt = select(Event)
        if type:
            stmt = stmt.where(Event.type == type)
        events = db.execute(stmt.offset(skip).limit(limit)).scalars().all()
        return [{"event_id": e.event_id, "session_id": e.session_id, "type": e.type, "timestamp": e.timestamp} for e in events]


@app.get("/events/{event_id}", tags=["Events"])
def get_event(event_id: int):
    with Session(engine) as db:
        event = db.get(Event, event_id)
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")
        return {"event_id": event.event_id, "session_id": event.session_id, "type": event.type, "timestamp": event.timestamp}


@app.get("/events/session/{session_id}", tags=["Events"])
def get_events_by_session(session_id: str):
    try:
        sid = uuid.UUID(session_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid session_id")
    with Session(engine) as db:
        session = db.get(SessionModel, sid)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        return [{"event_id": e.event_id, "type": e.type, "timestamp": e.timestamp} for e in session.events]


@app.get("/events/user/{user_id}", tags=["Events"])
def get_events_by_user(user_id: int):
    with Session(engine) as db:
        user = db.get(User, user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        events = []
        for session in user.sessions:
            events.extend([{"event_id": e.event_id, "type": e.type, "timestamp": e.timestamp, "session_id": session.session_id} for e in session.events])
        return events