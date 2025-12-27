##############################################################################################
# user_services.py
#
# Service layer functions for Users, handling database operations via SQLAlchemy.
# Provides functions to fetch users, fetch a user by ID, and include related sessions.
#
# All functions from this file are intended to be imported and used
# in user_session_event_routes.py for FastAPI endpoints.
##############################################################################################

from fastapi import APIRouter, HTTPException
from sqlalchemy import select, func
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from models import User, Session as SessionModel, Event, engine
from typing import Dict, Any


def get_users_service(skip: int, limit: int) -> Dict[str, Any]:
    try:
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
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail="Database error")
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")


def get_user_by_id_service(user_id: int) -> Dict[str, Any]:
    try:
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
                    "links": {
                        "sessions": f"/users/{user.user_id}/sessions"
                    }
                },
                "meta": {
                    "total_sessions": total_sessions,
                    "last_session_ts": last_session_ts
                },
            }
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail="Database error")
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")