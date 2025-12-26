from fastapi import APIRouter, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session
from models import User, Session as SessionModel, Event
from models import engine
import uuid

router = APIRouter(tags=["PostgreSQL"])


@router.get("/users", summary="Get list of users")
def get_users(skip: int = 0, limit: int = 20):
    with Session(engine) as db:
        users = db.execute(select(User).offset(skip).limit(limit)).scalars().all()
        return [{"user_id": u.user_id} for u in users]


@router.get("/users/{user_id}", summary="Get user details")
def get_user(user_id: int):
    with Session(engine) as db:
        user = db.get(User, user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        return {
            "user_id": user.user_id,
            "sessions": [{"session_id": s.session_id} for s in user.sessions]
        }


@router.get("/users/{user_id}/sessions", summary="Get all sessions of a user")
def get_user_sessions(user_id: int):
    with Session(engine) as db:
        user = db.get(User, user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        return [{"session_id": s.session_id, "browser": s.browser, "device": s.device} for s in user.sessions]


@router.get("/sessions", summary="Get list of sessions")
def get_sessions(skip: int = 0, limit: int = 20):
    with Session(engine) as db:
        sessions = db.execute(select(SessionModel).offset(skip).limit(limit)).scalars().all()
        return [{"session_id": s.session_id, "user_id": s.user_id} for s in sessions]


@router.get("/sessions/{session_id}", summary="Get session details")
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


@router.get("/sessions/user/{user_id}", summary="Get sessions by user")
def get_sessions_by_user(user_id: int):
    with Session(engine) as db:
        user = db.get(User, user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        return [{"session_id": s.session_id, "browser": s.browser, "device": s.device} for s in user.sessions]


@router.get("/events", summary="Get list of events")
def get_events(skip: int = 0, limit: int = 50, type: str | None = None):
    with Session(engine) as db:
        stmt = select(Event)
        if type:
            stmt = stmt.where(Event.type == type)
        events = db.execute(stmt.offset(skip).limit(limit)).scalars().all()
        return [{"event_id": e.event_id, "session_id": e.session_id, "type": e.type, "timestamp": e.timestamp} for e in events]


@router.get("/events/{event_id}", summary="Get event details")
def get_event(event_id: int):
    with Session(engine) as db:
        event = db.get(Event, event_id)
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")
        return {"event_id": event.event_id, "session_id": event.session_id, "type": event.type, "timestamp": event.timestamp}


@router.get("/events/session/{session_id}", summary="Get events by session")
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


@router.get("/events/user/{user_id}", summary="Get events by user")
def get_events_by_user(user_id: int):
    with Session(engine) as db:
        user = db.get(User, user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        events = []
        for session in user.sessions:
            events.extend([{"event_id": e.event_id, "type": e.type, "timestamp": e.timestamp, "session_id": session.session_id} for e in session.events])
        return events
