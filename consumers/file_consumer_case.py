# consumers/file_consumer_case.py

"""
Consume json messages from a live data file.
Insert the processed messages into a database.
"""

# -----------------------------
# Imports
# -----------------------------
import os
import json
import pathlib
import sys
import time
from typing import Optional

import utils.utils_config as config
from utils.utils_logger import logger
from .sqlite_consumer_case import init_db, insert_message


# -----------------------------
# Message processing
# -----------------------------
def process_message(message: dict) -> Optional[dict]:
    """
    Process and transform a single JSON message (dict in, dict out).

    Args:
        message (dict): The JSON message as a Python dictionary.
    """
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


# -----------------------------
# File tailer (real-time)
# -----------------------------
def consume_messages_from_file(
    live_data_path: pathlib.Path,
    sql_path: pathlib.Path,
    interval_secs: int,
    last_position: int = 0,
) -> None:
    """
    Tail a JSONL file in real time and insert each parsed message into SQLite.

    - Keeps reading as new lines are appended.
    - If the file is truncated or recreated, it resets to the start.
    """
    logger.info("Called consume_messages_from_file() with:")
    logger.info(f"   live_data_path={live_data_path}")
    logger.info(f"   sql_path={sql_path}")
    logger.info(f"   interval_secs={interval_secs}")
    logger.info(f"   last_position={last_position}")

    # Do NOT init the DB here; main() already does that.

    pos = int(last_position)

    while True:
        try:
            if not live_data_path.exists():
                logger.warning(f"Live data file not found at {live_data_path}. Retrying...")
                time.sleep(interval_secs)
                continue

            with live_data_path.open("r", encoding="utf-8") as f:
                # Handle truncation/recreation
                f.seek(0, os.SEEK_END)
                filesize = f.tell()
                if pos > filesize:
                    logger.info("Detected file truncation/recreate. Resetting position to 0.")
                    pos = 0

                # Seek to last known good position
                f.seek(pos)

                while True:
                    line = f.readline()
                    if not line:
                        # No new data right now; remember where we are and break to sleep
                        pos = f.tell()
                        break

                    line = line.strip()
                    if not line:
                        continue

                    try:
                        message = json.loads(line)
                    except json.JSONDecodeError as e:
                        logger.warning(f"Skipping bad JSON line: {e} | line={line!r}")
                        continue

                    processed = process_message(message)
                    if processed:
                        insert_message(processed, sql_path)

        except Exception as e:
            logger.error(f"ERROR while tailing {live_data_path}: {e}")
            # Don’t exit; keep trying so it stays real-time
        finally:
            time.sleep(interval_secs)


# -----------------------------
# Main
# -----------------------------
def main():
    """
    Main function to run the consumer process.
    """
    logger.info("Starting Consumer to run continuously.")
    logger.info("Things can fail or get interrupted, so use a try block.")
    logger.info("Moved .env variables into a utils config module.")

    # STEP 1. Read env
    try:
        interval_secs: int = config.get_message_interval_seconds_as_int()
        live_data_path: pathlib.Path = config.get_live_data_path()
        sqlite_path: pathlib.Path = config.get_sqlite_path()
        logger.info("SUCCESS: Read environment variables.")
        logger.info(f"LIVE_DATA_PATH: {live_data_path}")
        logger.info(f"SQLITE_PATH: {sqlite_path}")
        logger.info(f"MESSAGE_INTERVAL_SECONDS: {interval_secs}")
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    # STEP 2. Fresh DB
    if sqlite_path.exists():
        try:
            sqlite_path.unlink()
            logger.info("SUCCESS: Deleted database file.")
        except Exception as e:
            logger.error(f"ERROR: Failed to delete DB file: {e}")
            sys.exit(2)

    # STEP 3. Init DB
    try:
        init_db(sqlite_path)
    except Exception as e:
        logger.error(f"ERROR: Failed to create db table: {e}")
        sys.exit(3)

    # STEP 4. Tail the file forever
    try:
        consume_messages_from_file(live_data_path, sqlite_path, interval_secs, 0)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"ERROR: Unexpected error: {e}")
    finally:
        logger.info("TRY/FINALLY: Consumer shutting down.")


if __name__ == "__main__":
    main()
