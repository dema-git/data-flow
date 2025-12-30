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
from minio_utils.files import upload_batch
from collections import defaultdict
import threading
import json
from exceptions_logging.logger import info_logger, error_logger

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
    # Background consumer: read messages and add to queue
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            error_logger.warning(f"Consumer error: {msg.error()}")
            continue

        try:
            payload = json.loads(msg.value().decode("utf-8"))
        except json.JSONDecodeError:
            error_logger.warning(
                "Failed to decode message: %s", msg.value()
            )
            continue

        with queue_lock:
            message_queue.append({
                "data": payload,
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset()
            })


def start_consumer_loop():
    # Start consumer in separate thread
    info_logger.info(
        f"Starting Kafka consumer loop for topic {TOPIC}")
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
    try:
        upload_batch(data_batch)
    except Exception as e:
        error_logger.exception(
            f"Failed to upload batch of {len(data_batch)} messages"
        )
        raise

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
    info_logger.info(
        f"Committed offsets for {len(batch)} messages: {tps}",
    )

    return data_batch
