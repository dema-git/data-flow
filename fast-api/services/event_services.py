##############################################################################################
# event_services.py
#
# Service layer functions for Events, handling database operations via SQLAlchemy.
# Provides functions to fetch events, fetch an event by ID, and fetch events by user or session,
# including related session and user metadata.
#
# All functions from this file are intended to be imported and used
# in user_session_event_routes.py for FastAPI endpoints.
##############################################################################################

from fastapi import APIRouter, HTTPException
from sqlalchemy import select, func
from sqlalchemy.orm import Session, selectinload
from sqlalchemy.exc import SQLAlchemyError
from models import User, Session as SessionModel, Event, engine
from typing import Dict, Any
from exceptions_logging.logger import info_logger, error_logger


def get_events_service(skip: int, limit: int) -> Dict[str, Any]:
    info_logger.info(
        "Fetching all events | skip=%s limit=%s",skip, limit,)
    try:
        with Session(engine) as db:
            events = db.execute(select(Event).offset(skip).limit(limit)).scalars().all()
            total_events = db.execute(
                select(func.count()).select_from(Event)
            ).scalar_one()

            return {
                "data": {
                    "events": [
                        {
                            "event_id": e.event_id,
                            "type": e.type,
                            "timestamp": e.timestamp,
                            "links": {
                                "user": f"/users/{e.session.user.user_id}",
                                "session": f"/sessions/{e.session.session_id}",
                            },
                        }
                        for e in events
                    ],
                },
                "meta": {
                    "count": total_events,
                    "limit": limit,
                    "offset": skip,
                },
            }
    except SQLAlchemyError as e:
        error_logger.exception(
            f"Database error while fetching events: {e.args}"
        )
        raise HTTPException(status_code=500, detail="Database error")
    except Exception as e:
        error_logger.exception(
            f"Unexpected error in get_events_service: {e.args}",
        )
        raise HTTPException(status_code=500, detail="Internal server error")


def get_event_by_id_service(event_id: int) -> Dict[str, Any]:
    info_logger.info(
        "Fetching event by id"
    )
    try:
        with Session(engine) as db:
            event = db.get(Event, event_id)
            if not event:
                raise HTTPException(status_code=404, detail="Event not found")

            return {
                "data": {
                    "event_id": event.session_id,
                    "type": event.type,
                    "timestamp": event.timestamp,
                    "links": {
                        "user": f"/users/{event.session.user_id}",
                        "session": f"/sessions/{event.session.session_id}"
                    },
                },
                "meta": {
                    "browser": event.session.browser,
                    "device": event.session.device,
                },
            }
    except SQLAlchemyError as e:
        error_logger.exception(
            f"Database error while fetching event by id: {e.args}"
        )
        raise HTTPException(status_code=500, detail="Database error")
    except Exception as e:
        error_logger.exception(
            f"Unexpected error in get_event_by_id_service: {e.args}"
        )
        raise HTTPException(status_code=500, detail="Internal server error")


def get_events_by_user_id_service(user_id: str)-> Dict[str, Any]:
    info_logger.info(
        "Fetching events by user id"
    )
    try:
        with Session(engine) as db:
            user = db.get(User, user_id)
            if not user:
                raise HTTPException(status_code=404, detail="User not found")

            events = []
            for session in user.sessions:
                events.extend(session.events)

            if events:
                timestamps = [e.timestamp for e in events]
                first_event_ts = min(timestamps)
                last_event_ts = max(timestamps)
            else:
                first_event_ts = None
                last_event_ts = None

            return {
                "data": [
                    {
                        "event_id": e.event_id,
                        "type": e.type,
                        "timestamp": e.timestamp,
                        "browser": e.session.browser if e.session else None,
                        "device": e.session.device if e.session else None,
                        "links": {
                            "user": f"/users/{user_id}",
                            "session": f"/sessions/{e.session.session_id}" if e.session else None,
                        }
                    }
                    for e in events
                ],
                "meta": {
                    "total_events": len(events),
                    "first_event_timestamp": first_event_ts,
                    "last_event_timestamp": last_event_ts,
                }
            }

    except SQLAlchemyError as e:
        error_logger.exception(
            f"Database error while fetching events by user id: {e.args}"
        )
        raise HTTPException(status_code=500, detail="Database error")
    except Exception as e:
        error_logger.exception(
            f"Unexpected error in get_events_by_user_id_service: {e.args}"
        )
        raise HTTPException(status_code=500, detail="Internal server error")


def get_events_by_session_id_service(session_id: str)-> Dict[str, Any]:
    info_logger.info(
        "Fetching events by session id"
    )
    try:
        with Session(engine) as db:
            session = db.get(SessionModel, session_id)
            if not session:
                raise HTTPException(status_code=404, detail="Session not found")

            events = db.execute(
                select(Event)
                .join(SessionModel, Event.session_id == SessionModel.session_id)
                .options(selectinload(Event.session))
                .where(SessionModel.session_id == session_id)
            ).scalars().all()

            if events:
                timestamps = [e.timestamp for e in events]
                first_event_ts = min(timestamps)
                last_event_ts = max(timestamps)
            else:
                first_event_ts = None
                last_event_ts = None

            return {
                "data": {
                    "events": [
                        {
                            "event_id": e.event_id,
                            "type": e.type,
                            "timestamp": e.timestamp,
                            "browser": e.session.browser if e.session else None,
                            "device": e.session.device if e.session else None,
                            "links": {
                                "user": f"/users/{e.session.user_id}",
                                "session": f"/sessions/{e.session.session_id}" if e.session else None,
                            }
                        }
                        for e in events
                    ],
                },
                "meta": {
                    "total_events": len(events),
                    "first_event_timestamp": first_event_ts,
                    "last_event_timestamp": last_event_ts,
                }
            }
    except SQLAlchemyError as e:
        error_logger.exception(
            f"Database error while fetching events by session id: {e.args}"
        )
        raise HTTPException(status_code=500, detail="Database error")
    except Exception as e:
        error_logger.exception(
            f"Unexpected error in get_events_by_session_id_service: {e.args}"
        )
        raise HTTPException(status_code=500, detail=f"Internal server error{e}")
