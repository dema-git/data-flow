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

from confluent_kafka import Consumer, KafkaError, KafkaException
import threading
import json
import time
import os
from typing import Any, Dict, List
from collections import defaultdict

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from services.medallion_pipeline.bronze_writer import save_bronze_events
from services.medallion_models.bronze_model import BronzeWebEvent

KAFKA_TOPICS = ["sessions_raw"]
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BROKERCONNECT", "kafka:9092")
KAFKA_GROUP_ID = "kafka-consumer"


print(
    f"[KAFKA_CONSUMER] BOOTSTRAP={KAFKA_BOOTSTRAP!r}, "
    f"TOPICS={KAFKA_TOPICS}, GROUP_ID={KAFKA_GROUP_ID}",
    flush=True,
)


message_queue: List[Dict[str, Any]] = []
queue_lock = threading.Lock()


def create_consumer() -> Consumer:
    """
    Create and configure Kafka consumer
    """
    return Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": KAFKA_GROUP_ID,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "session.timeout.ms": 10000,
            "heartbeat.interval.ms": 3000,
        }
    )


def _decode_message(msg) -> Dict[str, Any] | None:
    """
    Decode Kafka message value as JSON
    """
    try:
        payload = json.loads(msg.value().decode("utf-8"))
    except Exception:
        return None

    return {
        "key": msg.key(),
        "data": payload,
        "topic": msg.topic(),
        "partition": msg.partition(),
        "offset": msg.offset(),
    }


def _commit_offsets(consumer: Consumer, batch: List[Dict[str, Any]]) -> None:
    """
    Commit next offsets for all topic-partitions in batch
    """
    if not batch:
        return

    max_offsets: dict[tuple[str, int], int] = defaultdict(lambda: -1)

    for m in batch:
        key = (m["topic"], m["partition"])
        max_offsets[key] = max(max_offsets[key], m["offset"])

    tps = [
        TopicPartition(topic, partition, offset + 1)
        for (topic, partition), offset in max_offsets.items()
    ]

    try:
        consumer.commit(offsets=tps)
    except KafkaException:
        pass


def _flush_bronze_batch(consumer: Consumer, pending_batch: List[Dict[str, Any]]) -> None:
    if not pending_batch:
        print("[BRONZE] Flush called but batch empty → skip", flush=True)
        return

    print(f"[BRONZE] Flushing {len(pending_batch)} messages to Bronze...", flush=True)

    try:
        bronze_events = [BronzeWebEvent(**msg["data"]) for msg in pending_batch]
        print(f"[BRONZE] Bronze events built: {len(bronze_events)}", flush=True)

        # Upload to MinIO
        save_bronze_events(bronze_events)
        print("[BRONZE] Bronze upload SUCCESS", flush=True)

        # Commit offsets
        _commit_offsets(consumer, pending_batch)
        print("[BRONZE] Kafka offsets committed", flush=True)

    except Exception as e:
        print(f"[BRONZE] ERROR during flush: {e}", flush=True)
        return

    pending_batch.clear()
    print("[BRONZE] Batch cleared", flush=True)


def _enqueue_message(msg_dict: Dict[str, Any]) -> None:
    """
    Put decoded Kafka message into queue
    """
    with queue_lock:
        message_queue.append(msg_dict)


def drain_queue() -> List[Dict[str, Any]]:
    """
    Take all messages from queue
    """
    with queue_lock:
        if not message_queue:
            return []
        batch = message_queue.copy()
        message_queue.clear()
        return batch


def consume_loop() -> None:
    """
    Kafka consumer:
    - polls messages
    - builds Bronze batch
    - saves Bronze to MinIO
    - commits offsets
    """
    BATCH_SIZE = 100     # flush every 100 messages
    FLUSH_SECONDS = 5.0  # or every 5 seconds

    print("[KAFKA] Consumer loop started", flush=True)

    while True:
        consumer: Consumer | None = None

        try:
            consumer = create_consumer()
            consumer.subscribe(KAFKA_TOPICS)

            print(f"[KAFKA] Subscribed to topics: {KAFKA_TOPICS}", flush=True)

            pending_bronze: List[Dict[str, Any]] = []
            last_flush_time = time.time()

            while True:
                msg = consumer.poll(1.0)

                if msg is None:
                    if pending_bronze and (time.time() - last_flush_time >= FLUSH_SECONDS):
                        print(f"[KAFKA] Time flush triggered. Pending batch: {len(pending_bronze)}", flush=True)
                        _flush_bronze_batch(consumer, pending_bronze)
                        last_flush_time = time.time()
                    continue

                if msg.error():
                    print(f"[KAFKA] ERROR msg.error(): {msg.error()}", flush=True)
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    continue

                decoded = _decode_message(msg)
                if decoded is None:
                    print("[KAFKA] Message decode FAILED → skipped", flush=True)
                    continue

                print(f"[KAFKA] Received message: topic={msg.topic()}, partition={msg.partition()}, offset={msg.offset()}", flush=True)
                _enqueue_message(decoded)

                pending_bronze.append(decoded)

                print(f"[KAFKA] Added to pending_bronze → total={len(pending_bronze)}", flush=True)

                if len(pending_bronze) >= BATCH_SIZE:
                    print(f"[KAFKA] Batch size flush triggered. Pending batch: {len(pending_bronze)}", flush=True)
                    _flush_bronze_batch(consumer, pending_bronze)
                    last_flush_time = time.time()

        except Exception as e:
            print(f"[KAFKA] Consumer crashed: {e}", flush=True)

        finally:
            if consumer:
                try:
                    consumer.close()
                    print("[KAFKA] Consumer closed", flush=True)
                except KafkaException:
                    print("[KAFKA] Consumer close failed", flush=True)

            time.sleep(3)


def start_consumer_loop() -> None:
    """
    Start Kafka consumer in background thread
    """
    t = threading.Thread(target=consume_loop, daemon=True)
    t.start()