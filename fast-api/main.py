from fastapi import FastAPI
import pandas as pd

app = FastAPI()


@app.get("/")
def homepage():
    return {"page": "Homepage"}


@app.get("/page/{number}")
def homepage_number(number: int):
    return {"page": f"Homepage {number}"}