##############################################################################################
# postgresql_get_endpoints.py
#
# This module defines routes to interact with PostgreSQL models using SQLAlchemy.
# It provides endpoints to retrieve users, sessions, and events with optional
# filtering and pagination. The endpoints also include relational data, such as
# sessions belonging to a user and events belonging to a session or user.
#
# Endpoints include:
# - /users: list all users or get a specific user with sessions
# - /sessions: list all sessions or get details of a specific session
# - /events: list all events or get details of a specific event
# - Additional endpoints for retrieving sessions/events by user or session
##############################################################################################

from fastapi import APIRouter, HTTPException
from sqlalchemy import select, func
from sqlalchemy.orm import Session
from models import User, Session as SessionModel, Event
from models import engine
import uuid

router = APIRouter()


################
# USERS
################
@router.get("/users", summary="Get list of users", tags=["Users"])
def get_users(skip: int = 0, limit: int = 20):
    with Session(engine) as db:
        users = db.execute(select(User).offset(skip).limit(limit)).scalars().all()
        total_users = db.execute(
            select(func.count()).select_from(User)
        ).scalar_one()

        return {
            "data": {
                "users": [
                    {
                        "user_id": u.user_id,
                        "links": {
                            "user": f"/users/{u.user_id}",
                            "sessions": f"/sessions/user/{u.user_id}",
                        }
                    }
                for u in users],
            },
            "meta": {
                "count": total_users,
                "limit": limit,
                "offset": skip
            }
        }


@router.get("/users/{user_id}", summary="Get user details", tags=["Users"])
def get_user(user_id: int):
    with Session(engine) as db:
        user = db.get(User, user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        total_sessions = len(user.sessions)
        last_session_ts = max(
            (max((e.timestamp for e in s.events), default=0) for s in user.sessions),
            default=None
        )

        return {
            "data": {
                "user_id": user.user_id,
                "links":{
                    "sessions": f"/users/{user.user_id}/sessions"
                }
            },
            "meta": {
                "total_sessions": total_sessions,
                "last_session_ts": last_session_ts
            },
        }


# @router.get("/users/{user_id}/sessions", summary="Get all sessions of a user")
# def get_user_sessions(user_id: int):
#     with Session(engine) as db:
#         user = db.get(User, user_id)
#         if not user:
#             raise HTTPException(status_code=404, detail="User not found")
#
#         sessions_data = []
#         for s in user.sessions:
#
#             timestamps = [e.timestamp for e in s.events]
#             last_event_ts = max(timestamps) if timestamps else None
#
#             sessions_data.append({
#                 "session_id": s.session_id,
#                 "browser": s.browser,
#                 "device": s.device,
#                 "last_event_ts": last_event_ts,
#                 "links": {
#                     "session": f"sessions/{s.session_id}"
#                 }
#             })
#
#         total_sessions = len(sessions_data)
#         last_session_ts = max(s["last_event_ts"] for s in sessions_data if s["last_event_ts"] is not None) \
#             if sessions_data else None
#
#         return {
#             "data": {
#                 "user_id": user.user_id,
#                 "sessions": sessions_data,
#                 "meta": {
#                     "total_sessions": total_sessions,
#                     "last_session_ts": last_session_ts
#                 }
#             }
#         }


################
# SESSIONS
################
@router.get("/sessions", summary="Get list of sessions", tags=["Sessions"])
def get_sessions(skip: int = 0, limit: int = 20):
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


@router.get("/sessions/{session_id}", summary="Get session details", tags=["Sessions"])
def get_session(session_id: str):
    try:
        sid = uuid.UUID(session_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid session_id")
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


@router.get("/sessions/user/{user_id}", summary="Get sessions by user", tags=["Sessions"])
def get_sessions_by_user(user_id: int):
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


################
# EVENTS
################
@router.get("/events", summary="Get list of events", tags=["Events"])
def get_events(skip: int = 0, limit: int = 50, type: str | None = None):
    with Session(engine) as db:
        stmt = select(Event)
        if type:
            stmt = stmt.where(Event.type == type)
        events = db.execute(stmt.offset(skip).limit(limit)).scalars().all()
        return [{"event_id": e.event_id, "session_id": e.session_id, "type": e.type, "timestamp": e.timestamp} for e in events]


@router.get("/events/{event_id}", summary="Get event details", tags=["Events"])
def get_event(event_id: int):
    with Session(engine) as db:
        event = db.get(Event, event_id)
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")
        return {"event_id": event.event_id, "session_id": event.session_id, "type": event.type, "timestamp": event.timestamp}


@router.get("/events/session/{session_id}", summary="Get events by session", tags=["Events"])
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


@router.get("/events/user/{user_id}", summary="Get events by user", tags=["Events"])
def get_events_by_user(user_id: int):
    with Session(engine) as db:
        user = db.get(User, user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        events = []
        for session in user.sessions:
            events.extend([{"event_id": e.event_id, "type": e.type, "timestamp": e.timestamp, "session_id": session.session_id} for e in session.events])
        return events
