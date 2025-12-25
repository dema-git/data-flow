###############################################################################
# kafka_consumer.py
#
# Kafka Consumer for FastAPI + MinIO Uploader
#
# This file implements a background Kafka consumer that reads messages
# from a specified topic and stores them in a queue. The collected messages
# can then be retrieved in batches and uploaded to MinIO as Parquet files.
#
# Main components:
# - Kafka consumer with auto-commit offsets.
# - Background thread to continuously poll messages.
##########################################################

from confluent_kafka import Consumer, TopicPartition
from upload_files_to_minio import upload_batch
from collections import defaultdict
import threading
import json

TOPIC = "llama_topic"
BOOTSTRAP = "kafka:9092"

# consumer с commit offset
consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": "time-window-consumer",
    "enable.auto.commit": False,
    "auto.offset.reset": "earliest"
})

consumer.subscribe([TOPIC])

# message queue for FastAPI
message_queue = []
queue_lock = threading.Lock()


def consume_loop():
    """Background consumer: read messages and add to queue"""
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            continue

        try:
            payload = json.loads(msg.value().decode("utf-8"))
        except json.JSONDecodeError:
            continue

        with queue_lock:
            message_queue.append({
                "data": payload,
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset()
            })


def start_consumer_loop():
    """Start consumer in separate thread"""
    threading.Thread(
        target=consume_loop,
        daemon=True
    ).start()


def get_messages():

    # Safely take all messages from the queue
    with queue_lock:
        if not message_queue:
            return []

        batch = message_queue.copy()
        message_queue.clear()

    data_batch = [m["data"] for m in batch]

    # Upload batch to MinIO (durable storage)
    # Offsets must NOT be committed if this step fails
    upload_batch(data_batch)

    # Collect the highest processed offset per topic-partition
    offsets = defaultdict(lambda: -1)

    for m in batch:
        key = (m["topic"], m["partition"])
        offsets[key] = max(offsets[key], m["offset"])

    # Commit offsets AFTER successful upload
    # Kafka expects the NEXT offset to be committed
    tps = [
        TopicPartition(topic, partition, offset + 1)
        for (topic, partition), offset in offsets.items()
    ]

    consumer.commit(offsets=tps)

    return data_batch
