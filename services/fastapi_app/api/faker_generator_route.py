# api/faker_generator_route.py

from fastapi import APIRouter
from services.faker.generator import SessionEventFaker
from services.faker.config import FakerConfig

router = APIRouter(
    prefix="/faker",
    tags=["faker"],
)

# Отдельный Faker для ручных вызовов (не тот, что в background loop)
faker = SessionEventFaker(FakerConfig())


@router.get("/sample-session")
def get_sample_session():
    """
    Generate a single user session (list of events) and return it.
    This does NOT send anything to Kafka, just for debugging/demo.
    """
    events = faker.generate_session_events()
    return {
        "events_count": len(events),
        "events": events,
    }


@router.get("/sample-batch")
def get_sample_batch(num_sessions: int = 3):
    """
    Generate multiple sessions and return them.
    Again, no Kafka here – purely for inspection/debug via API.
    """
    events = faker.generate_batch(num_sessions=num_sessions)
    return {
        "sessions": num_sessions,
        "events_count": len(events),
        "events": events,
    }
