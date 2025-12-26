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
# - integrations_endpoints: Handles external integrations and data processing.
# - postgresql_get_endpoints: Handles fetching data from PostgreSQL.
#####################################################################################

from fastapi import FastAPI
from api import integrations_endpoints, postgresql_get_endpoints

app = FastAPI(
    title="Llama Kafka & MinIO API",
    description="API for generating sessions via Llama, sending to Kafka, and storing/retrieving from MinIO",
    version="1.0.0"
)

@app.get("/", tags=["Home/Dashboard"])
def homepage():
    return {"page": "Homepage"}

# Include API routers for integrations (Kafka/MinIO) and PostgreSQL endpoints
app.include_router(integrations_endpoints.router)
app.include_router(postgresql_get_endpoints.router)

