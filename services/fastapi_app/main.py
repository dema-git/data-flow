#####################################################################################
# main.py (fast-api)
#
# Main FastAPI application entry point.
#
# This file initializes the FastAPI app for the "Llama Kafka & MinIO API" project.
# It sets up the application metadata (title, description, version) and includes
# all API routers for handling integrations and PostgreSQL data retrieval.
#
# Routers included:
# - integrations_routes: Handles external integrations and data processing.
# - user_session_event_routes: Handles fetching data from PostgreSQL.
# - faker_generator_route: Faker-based session generator demo endpoints.
#####################################################################################

import asyncio
import logging

from fastapi import FastAPI

from api import integrations_routes, user_session_event_routes, faker_generator_route
from services.faker.generator import SessionEventFaker
from services.faker.config import FakerConfig
from services.kafka.producer import (
    KafkaProducerContext,
    start_producer,
    stop_producer,
    send_session_event,
)

# ------------------------------------------------------------------------------
# Logging setup
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# FastAPI app
# ------------------------------------------------------------------------------
app = FastAPI(
    title="Llama Kafka & MinIO API",
    description="API for generating sessions via Faker, sending to Kafka, and storing/retrieving from MinIO",
    version="1.0.0",
)


@app.get("/", tags=["Home/Dashboard"])
def homepage():
    return {"page": "Homepage"}


# ------------------------------------------------------------------------------
# Global services: Faker + Kafka context
# ------------------------------------------------------------------------------
faker = SessionEventFaker(FakerConfig())
kafka_ctx = KafkaProducerContext()

_background_task: asyncio.Task | None = None


async def background_loop(
    interval_seconds: int = 10,
    sessions_per_batch: int = 5,
) -> None:
    """
    Simple background loop:
    - every N seconds generate batch of sessions
    - send each event to Kafka
    """
    logger.info(
        "Background loop started (interval=%s, sessions_per_batch=%s)",
        interval_seconds,
        sessions_per_batch,
    )

    try:
        while True:
            events = faker.generate_batch(num_sessions=sessions_per_batch)
            logger.info("Generated %s events", len(events))

            for event in events:
                send_session_event(kafka_ctx, event)

            logger.info("Sent %s events to Kafka", len(events))
            await asyncio.sleep(interval_seconds)
    except asyncio.CancelledError:
        logger.info("Background loop cancelled, shutting down gracefully")
        raise
    except Exception as exc:
        logger.exception("Unexpected error in background loop: %s", exc)
    finally:
        logger.info("Background loop stopped")


# ------------------------------------------------------------------------------
# Startup / Shutdown hooks
# ------------------------------------------------------------------------------
@app.on_event("startup")
async def on_startup():
    global _background_task

    logger.info("App startup: starting Kafka producer and background loop")
    start_producer(kafka_ctx)
    _background_task = asyncio.create_task(background_loop())


@app.on_event("shutdown")
async def on_shutdown():
    global _background_task

    logger.info("App shutdown: stopping background loop and Kafka producer")

    if _background_task:
        _background_task.cancel()
        try:
            await _background_task
        except asyncio.CancelledError:
            pass

    stop_producer(kafka_ctx)


# ------------------------------------------------------------------------------
# Include routers (endpoints only)
# ------------------------------------------------------------------------------
# app.include_router(integrations_routes.router)
# app.include_router(user_session_event_routes.router)
app.include_router(faker_generator_route.router)

