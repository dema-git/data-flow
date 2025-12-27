##############################################################################################
# user_session_event_routes.py
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

# The routes use service layer functions to handle database operations and keeping the
# API layer clean
##############################################################################################

from fastapi import APIRouter, HTTPException
from models import User, Session as SessionModel, Event, engine
from services.event_services import (get_events_service, get_event_by_id_service, get_events_by_user_id_service,
                                     get_events_by_session_id_service)
from services.session_services import get_sessions_service, get_session_by_id_service, get_session_by_user_id_service
from services.user_services import get_users_service, get_user_by_id_service

router = APIRouter()


################
# USERS
################
@router.get("/users", summary="Get list of users", tags=["Users"])
def get_users(skip: int = 0, limit: int = 20):
    return get_users_service(skip=skip, limit=limit)


@router.get("/users/{user_id}", summary="Get user details", tags=["Users"])
def get_user(user_id: int):
    return get_user_by_id_service(user_id=user_id)


################
# SESSIONS
################
@router.get("/sessions", summary="Get list of sessions", tags=["Sessions"])
def get_sessions(skip: int = 0, limit: int = 20):
    return get_sessions_service(skip=skip, limit=limit)


@router.get("/sessions/{session_id}", summary="Get session details", tags=["Sessions"])
def get_session(session_id: str):
    return get_session_by_id_service(session_id=session_id)


@router.get("/sessions/user/{user_id}", summary="Get sessions by user", tags=["Sessions"])
def get_sessions_by_user(user_id: int):
    return get_session_by_user_id_service(user_id=user_id)


################
# EVENTS
################
@router.get("/events", summary="Get list of events", tags=["Events"])
def get_events(skip: int = 0, limit: int = 50):
    return get_events_service(skip=skip, limit=limit)


@router.get("/events/{event_id}", summary="Get event details", tags=["Events"])
def get_event(event_id: int):
    return get_event_by_id_service(event_id=event_id)


@router.get("/events/session/{session_id}", summary="Get events by session", tags=["Events"])
def get_events_by_session(session_id: str):
    return get_events_by_session_id_service(session_id=session_id)


@router.get("/events/user/{user_id}", summary="Get events by user", tags=["Events"])
def get_events_by_user(user_id: int):
    return get_events_by_user_id_service(user_id=user_id)

