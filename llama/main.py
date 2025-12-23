######################################################################
# main.py
#
# FastAPI service that generates synthetic user session events using a local LLaMA model
# and streams them to a Kafka topic. Includes a /status endpoint to check model readiness.
######################################################################

from fastapi import FastAPI
import asyncio
from llama_cpp import Llama
import os
from faker import Faker
from confluent_kafka import Producer
import json

app = FastAPI()
faker = Faker()

conf = {'bootstrap.servers': 'kafka:9092'}
producer = Producer(conf)
topic = "llama_topic"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MODEL_PATH = os.path.join(BASE_DIR, "meta-llama-3-8b-instruct.Q4_K_M.gguf")

llm = None

async def init_llama():
    global llm
    print("Initializing Llama...")
    llm = Llama(
        model_path=MODEL_PATH,
        n_ctx=512,
        n_threads=4,
        n_batch=64,
        n_gpu_layers=0,
        use_mlock=False,
        verbose=True
    )
    print("Llama initialized!")

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(init_llama())
    asyncio.create_task(send_events_to_kafka())

def generate_session():
    if llm is None:
        return "{}"
    PROMPT_TEMPLATE = """
    Generate 5 user sessions in JSON format, each with 2-5 events.
    Return ONLY valid JSON. No explanations or extra text outside JSON.
    Requirements:
    - Each "session_id" must be a unique random UUID
    - Each "user_id" must be unique across the 5 sessions, random integer between 1000 and 9999
    - Each event must have a real Unix timestamp in seconds (seconds since Jan 1, 1970 UTC)
    - Events types can be: login, logout, click, view_product, search, add_to_cart
    - Timestamps in events must be in chronological order
    Schema example:
    {
      "session_id": "uuid",
      "user_id": "int",
      "events": [
        {
          "timestamp": 123456789,
          "type": "login"
        }
      ],
      "metadata": {
         "browser": "Chrome",
         "device": "desktop"
      }
    }
    """
    result = llm(PROMPT_TEMPLATE, max_tokens=600, temperature=0.1, stop=["</s>"])
    return result["choices"][0]["text"].strip()

def extract_json_objects(text: str):
    objects, stack, start_idx = [], [], None
    for i, c in enumerate(text):
        if c == '{':
            if not stack: start_idx = i
            stack.append('{')
        elif c == '}':
            if stack:
                stack.pop()
                if not stack and start_idx is not None:
                    objects.append(text[start_idx:i+1])
                    start_idx = None
    return objects


async def send_events_to_kafka():
    while True:
        session_text = generate_session()
        json_blocks = extract_json_objects(session_text)
        sessions = []
        for block in json_blocks:
            try: sessions.append(json.loads(block))
            except json.JSONDecodeError: continue
        print(json.dumps(sessions, indent=2))
        producer.produce(topic, json.dumps(sessions, indent=2).encode('utf-8'))
        producer.flush()
        print("Sent to Kafka")
        await asyncio.sleep(2)


@app.get("/status")
def status():
    if llm is None:
        return {"status": "starting Llama..."}
    return {"status": "running"}