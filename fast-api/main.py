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


app.include_router(integrations_endpoints.router)
app.include_router(postgresql_get_endpoints.router)

