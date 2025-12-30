##############################################################################################
# session_services.py
#
# Service layer functions for Sessions, handling database operations via SQLAlchemy.
# Provides functions to fetch sessions, fetch a session by ID, and fetch sessions by user ID,
# including related events and metadata.
#
# All functions from this file are intended to be imported and used
# in user_session_event_routes.py for FastAPI endpoints.
##############################################################################################

from fastapi import APIRouter, HTTPException
from sqlalchemy import select, func
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from models import User, Session as SessionModel, Event, engine
import uuid
from typing import Dict, Any
from exceptions_logging.logger import info_logger, error_logger

def get_sessions_service(skip: int, limit: int) -> Dict[str, Any]:
    info_logger.info(
        "Fetching all sessions | skip=%s limit=%s", skip, limit,
    )
    try:
        with Session(engine) as db:
            sessions = db.execute(select(SessionModel).offset(skip).limit(limit)).scalars().all()
            total_sessions = db.execute(
                select(func.count()).select_from(SessionModel)
            ).scalar_one()

            return {
                "data": {
                    "sessions": [
                        {
                            "session_id": s.session_id,
                            "url": f"/sessions/{s.session_id}"
                        }
                        for s in sessions
                    ],
                },
                "meta": {
                    "count": total_sessions,
                    "limit": limit,
                    "offset": skip
                }
            }
    except SQLAlchemyError as e:
        error_logger.exception(
            f"Database error while fetching sessions: {e.args}"
        )
        raise HTTPException(status_code=500, detail="Database error")
    except Exception as e:
        error_logger.exception(
            f"Unexpected error in get_sessions_service: {e.args}",
        )
        raise HTTPException(status_code=500, detail="Internal server error")


def get_session_by_id_service(session_id: str) -> Dict[str, Any]:
    info_logger.info(
        "Fetching session by id"
    )
    try:
        sid = uuid.UUID(session_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid session_id")
    try:
        with Session(engine) as db:
            session = db.get(SessionModel, sid)
            if not session:
                raise HTTPException(status_code=404, detail="Session not found")

            event_timestamps = [e.timestamp for e in session.events]
            first_event_ts = min(event_timestamps, default=None)
            last_event_ts = max(event_timestamps, default=None)
            total_events = len(event_timestamps)

            return {
                "data": {
                    "session_id": session.session_id,
                    "user_id": session.user_id,
                    "browser": session.browser,
                    "device": session.device,
                    "links": {
                        "user": f"/users/{session.user_id}",
                        "events": f"/sessions/{session.session_id}/events"
                    },
                    "events": [{"event_id": e.event_id, "type": e.type, "timestamp": e.timestamp} for e in session.events]
                },
                "meta": {
                    "total_events": total_events,
                    "first_event_ts": first_event_ts,
                    "last_event_ts": last_event_ts
                },
            }
    except SQLAlchemyError as e:
        error_logger.exception(
            f"Database error while fetching session by id: {e.args}"
        )
        raise HTTPException(status_code=500, detail="Database error")
    except Exception as e:
        error_logger.exception(
            f"Unexpected error in get_session_by_id_service: {e.args}",
        )
        raise HTTPException(status_code=500, detail="Internal server error")


def get_session_by_user_id_service(user_id: int) -> Dict[str, Any]:
    info_logger.info(
        "Fetching sessions by user id"
    )
    try:
        with Session(engine) as db:
            user = db.get(User, user_id)
            if not user:
                raise HTTPException(status_code=404, detail="User not found")

            sessions_data = []
            last_event_ts_list = []

            for s in user.sessions:
                last_ts = max((e.timestamp for e in s.events), default=None)
                if last_ts is not None:
                    last_event_ts_list.append(last_ts)

                sessions_data.append({
                    "session_id": s.session_id,
                    "links": {
                        "session": f"/sessions/{s.session_id}",
                        "user": f"/users/{user.user_id}"
                    },
                    "last_event_ts": last_ts
                })

            return {
                "data": {
                    "sessions": sessions_data,
                    "meta": {
                        "total_sessions": len(sessions_data),
                        "last_event_ts": max(last_event_ts_list) if last_event_ts_list else None
                    }
                }
            }
    except SQLAlchemyError as e:
        error_logger.exception(
            f"Database error while fetching session by user id: {e.args}"
        )
        raise HTTPException(status_code=500, detail="Database error")
    except Exception as e:
        error_logger.exception(
            f"Unexpected error in get_session_by_user_id_service: {e.args}",
        )
        raise HTTPException(status_code=500, detail="Internal server error")
