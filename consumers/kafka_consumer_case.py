"""
kafka_consumer_case.py

Consume JSON messages from a Kafka topic and insert into SQLite in real time.
"""

# -----------------------------
# Imports
# -----------------------------
import json
import os
import pathlib
import sys
import time
from typing import Optional

from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable

# local
import utils.utils_config as config
from utils.utils_logger import logger

# import DB functions (SQLite by default)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from consumers.sqlite_consumer_case import init_db, insert_message  # noqa: E402


# -----------------------------
# Helpers
# -----------------------------
def _create_consumer(
    topic: str,
    bootstrap_servers: str,
    group_id: str,
    client_id: str = "buzzline-consumer",
) -> KafkaConsumer:
    """
    Build a KafkaConsumer configured for real-time streaming.
    """
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        client_id=client_id,
        enable_auto_commit=True,
        auto_offset_reset="latest",  # start at new messages
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        consumer_timeout_ms=0,       # never stop iterating
        request_timeout_ms=30000,
        session_timeout_ms=20000,
        max_poll_interval_ms=300000,
    )


def _wait_for_broker(bootstrap_servers: str, max_wait_s: int = 30) -> None:
    """
    Wait for Kafka broker to be reachable (simple retry loop).
    """
    logger.info(f"Kafka broker address: {bootstrap_servers}")
    start = time.time()
    while True:
        try:
            # A quick probe: create and close a temporary consumer
            tmp = KafkaConsumer(bootstrap_servers=bootstrap_servers)
            tmp.close()
            logger.info("Kafka is ready.")
            return
        except NoBrokersAvailable:
            if time.time() - start > max_wait_s:
                raise
            time.sleep(2)


def _process_message(record_value: dict) -> Optional[dict]:
    """
    Normalize a single JSON message (dict in, dict out).
    """
    try:
        return {
            "message": record_value.get("message"),
            "author": record_value.get("author"),
            "timestamp": record_value.get("timestamp"),
            "category": record_value.get("category"),
            "sentiment": float(record_value.get("sentiment", 0.0)),
            "keyword_mentioned": record_value.get("keyword_mentioned"),
            "message_length": int(record_value.get("message_length", 0)),
        }
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None


# -----------------------------
# Consume loop
# -----------------------------
def consume_messages_from_kafka(
    topic: str,
    kafka_url: str,
    group: str,
    sql_path: pathlib.Path,
) -> None:
    logger.info("Called consume_messages_from_kafka() with:")
    logger.info(f"   topic={topic!r}")
    logger.info(f"   group={group!r}")
    logger.info(f"   sql_path={sql_path!r}")

    # 1) Ensure DB exists and is ready for concurrent writes
    init_db(sql_path)

    # 2) Ensure broker reachable
    try:
        _wait_for_broker(kafka_url)
    except Exception as e:
        logger.error(f"Kafka broker not reachable at {kafka_url}: {e}")
        sys.exit(11)

    # 3) Create consumer
    try:
        consumer = _create_consumer(topic, kafka_url, group_id=group)
        logger.info("Kafka consumer created successfully.")
    except KafkaError as e:
        logger.error(f"Could not create Kafka consumer: {e}")
        sys.exit(12)

    # 4) Stream forever
    logger.info("Starting real-time consume loop...")
    try:
        for msg in consumer:
            processed = _process_message(msg.value)
            if processed:
                insert_message(processed, sql_path)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error in consume loop: {e}")
        raise
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        logger.info("Consumer shutting down.")


# -----------------------------
# Main
# -----------------------------
def main() -> None:
    logger.info("Starting Consumer to run continuously.")
    logger.info("Reading configuration...")

    try:
        topic = config.get_kafka_topic()
        kafka_url = config.get_kafka_broker_address() or "localhost:9092"
        group_id = config.get_kafka_consumer_group_id()
        sqlite_path: pathlib.Path = config.get_sqlite_path()
        logger.info("SUCCESS: Read environment variables.")
        logger.info(f"BUZZ_TOPIC: {topic}")
        logger.info(f"BUZZ_CONSUMER_GROUP_ID: {group_id}")
        logger.info(f"SQLITE_PATH: {sqlite_path}")
    except Exception as e:
        logger.error(f"Failed to read environment variables: {e}")
        sys.exit(1)

    # Do NOT delete the DB file here (prevents Windows file-lock issues)
    consume_messages_from_kafka(topic, kafka_url, group_id, sqlite_path)


if __name__ == "__main__":
    main()
