##############################################################################################
# session_services.py
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
from services.session_services import ( get_top_landing_pages_service, get_top_products_by_revenue_service,
                        get_ab_test_summary_service, get_user_sessions_overview_service)


router = APIRouter()


@router.get(
    "/analytics/top-landing-pages",
    summary="Top landing pages with bounce rate",
    tags=["Analytics"],
)
def top_landing_pages(limit: int = 10):
    return get_top_landing_pages_service(limit=limit)


@router.get(
    "/analytics/top-products",
    summary="Top products by total revenue",
    tags=["Analytics"],
)
def top_products(limit: int = 10):
    return get_top_products_by_revenue_service(limit=limit)


@router.get(
    "/analytics/ab-test-summary",
    summary="A/B test performance summary",
    tags=["Analytics"],
)
def ab_test_summary():
    return get_ab_test_summary_service()


@router.get(
    "/analytics/user/{user_id}/sessions",
    summary="User session overview (GOLD layer)",
    tags=["Analytics"],
)
def analytics_user_sessions_overview(user_id: str, skip: int = 0, limit: int = 20):
    return get_user_sessions_overview_service(
        user_id=user_id,
        skip=skip,
        limit=limit,
    )


