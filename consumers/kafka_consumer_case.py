"""
kafka_consumer_case.py

Consume json messages from a Kafka topic in real time.
Insert the processed messages into a database (SQLite).

Relies on:
- utils.utils_config for env
- utils.utils_consumer.create_kafka_consumer
- consumers.sqlite_consumer_case.init_db, insert_message
"""

# --------------------------
# Imports
# --------------------------
import json
import os
import pathlib
import sys
from typing import Optional

from kafka import KafkaConsumer

import utils.utils_config as config
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

# verify_services may exist in your utils; if not, we gracefully skip it
try:
    from utils.utils_producer import verify_services  # type: ignore
except Exception:  # pragma: no cover
    verify_services = None  # type: ignore

# Ensure the parent directory is in sys.path (for local package imports)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from consumers.sqlite_consumer_case import init_db, insert_message  # noqa: E402


# --------------------------
# Message processor
# --------------------------
def process_message(message: dict) -> Optional[dict]:
    """Validate/transform one message dict."""
    try:
        processed = {
            "message": message.get("message"),
            "author": message.get("author"),
            "timestamp": message.get("timestamp"),
            "category": message.get("category"),
            "sentiment": float(message.get("sentiment", 0.0)),
            "keyword_mentioned": message.get("keyword_mentioned"),
            "message_length": int(message.get("message_length", 0)),
        }
        logger.info(f"Processed message: {processed}")
        return processed
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None


# --------------------------
# Helpers
# --------------------------
def _topic_exists(consumer: KafkaConsumer, topic: str) -> bool:
    """Best-effort topic existence check; returns True if callable fails."""
    try:
        topics = consumer.topics()
        return (topic in topics) if isinstance(topics, set) else True
    except Exception:
        # If the broker disallows metadata or the call fails, don't block streaming.
        return True


# --------------------------
# Main consume loop
# --------------------------
def consume_messages_from_kafka(
    topic: str,
    group: str,
    sql_path: pathlib.Path,
) -> None:
    logger.info("Called consume_messages_from_kafka() with:")
    logger.info(f"   topic={topic!r}")
    logger.info(f"   group={group!r}")
    logger.info(f"   sql_path={sql_path!r}")

    # 1) Optional: verify local services (if function exists)
    if verify_services:
        try:
            verify_services()
        except Exception as e:
            logger.warning(f"verify_services() failed or unavailable: {e}")

    # 2) Build consumer (use your utils.create_kafka_consumer signature)
    try:
        consumer: KafkaConsumer = create_kafka_consumer(
            topic,
            group,
            value_deserializer_provided=lambda x: json.loads(x.decode("utf-8")),
        )
        logger.info("KafkaConsumer created.")
    except Exception as e:
        logger.error(f"ERROR: Could not create Kafka consumer: {e}")
        sys.exit(11)

    # 3) Best-effort topic check
    if not _topic_exists(consumer, topic):
        logger.error(
            f"ERROR: Kafka topic '{topic}' does not exist or metadata unavailable."
        )
        sys.exit(13)

    logger.info("Starting real-time consume loop...")
    try:
        for record in consumer:
            # record.value is already a dict because of the value_deserializer
            processed = process_message(record.value)
            if processed:
                insert_message(processed, sql_path)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"ERROR: Could not consume messages from Kafka: {e}")
        raise
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        logger.info("Consumer shutting down.")


# --------------------------
# Script entry
# --------------------------
def main() -> None:
    logger.info("Starting Consumer to run continuously.")
    logger.info("Things can fail or get interrupted, so use a try block.")
    logger.info("Moved .env variables into a utils config module.")

    # Read env
    try:
        topic = config.get_kafka_topic()
        group_id = config.get_kafka_consumer_group_id()
        sqlite_path: pathlib.Path = config.get_sqlite_path()
        logger.info("SUCCESS: Read environment variables.")
        logger.info(f"BUZZ_TOPIC: {topic}")
        logger.info(f"BUZZ_CONSUMER_GROUP_ID: {group_id}")
        logger.info(f"SQLITE_PATH: {sqlite_path}")
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    # Fresh-start handling (optional)
    fresh = os.getenv("FRESH_START_DB", "false").strip().lower() in ("1", "true", "yes")
    if fresh and sqlite_path.exists():
        try:
            sqlite_path.unlink()
            logger.info(f"Deleted prior database at {sqlite_path}")
        except PermissionError as e:
            # Don't exit—this is exactly the Windows lock you hit. Reuse the DB instead.
            logger.warning(
                f"DB file is in use; continuing without delete: {sqlite_path} ({e})"
            )
        except Exception as e:
            logger.warning(f"Could not delete DB; continuing: {e}")

    # Initialize the DB (should NOT drop the table unless your init does so explicitly)
    try:
        init_db(sqlite_path)
        logger.info(f"Database initialized/ready at {sqlite_path}.")
    except Exception as e:
        logger.error(f"ERROR: Failed to initialize database: {e}")
        sys.exit(3)

    # Start streaming
    consume_messages_from_kafka(topic, group_id, sqlite_path)


if __name__ == "__main__":
    main()
