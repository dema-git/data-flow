from fastapi import FastAPI
import pandas as pd
from kafka_consumer import start_consumer_loop, get_messages

app = FastAPI()

start_consumer_loop()

@app.get("/")
def homepage():
    return {"page": "Homepage"}


@app.get("/page/{number}")
def homepage_number(number: int):
    return {"page": f"Homepage {number}"}


@app.get("/kafka/consumer")
def kafka_consumer():
    batch = get_messages()
    return {"status": "success" ,
            "batch": batch}