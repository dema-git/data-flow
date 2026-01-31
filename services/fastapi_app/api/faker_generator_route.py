##########################################################
# api/faker_generator_route.py
#
# This module defines FastAPI endpoints for generating fake session event data
#
# !!! IMPORTANT: These endpoints are for demonstration and data structure inspection only.
# In production, all data generation is triggered through Airflow DAGs to maintain
# pipeline integrity. Manual endpoint calls may disrupt the data flow and should be
# avoided in (production) environments. !!!
##########################################################

from fastapi import APIRouter
from services.faker.generator import SessionEventFaker
from services.faker.config import FakerConfig

router = APIRouter(
    prefix="/faker",
    tags=["faker"],
)

faker = SessionEventFaker(FakerConfig())


@router.get("/sample-session")
def get_sample_session():
    """
    Generate a single user session (list of events) and return it
    """
    events = faker.generate_session_events()
    return {
        "events_count": len(events),
        "events": events,
    }


@router.get("/sample-batch")
def get_sample_batch(num_sessions: int = 3):
    """
    Generate multiple sessions and return them
    """
    events = faker.generate_batch(num_sessions=num_sessions)
    return {
        "sessions": num_sessions,
        "events_count": len(events),
        "events": events,
    }
