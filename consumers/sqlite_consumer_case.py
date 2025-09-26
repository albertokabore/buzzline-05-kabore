# consumers/sqlite_consumer_case.py

"""
SQLite helpers for streaming inserts.
Functions:
- init_db(db_path, recreate=False): create DB/table; WAL mode for concurrency.
- insert_message(message, db_path): robust insert with retry on 'database is locked'.
- delete_message(message_id, db_path): remove a row by id.
"""

from __future__ import annotations

# stdlib
import time
import pathlib
import sqlite3
from typing import Mapping, Any, Optional

# local
import utils.utils_config as config
from utils.utils_logger import logger


# -----------------------------
# Initialization
# -----------------------------
def init_db(db_path: pathlib.Path, *, recreate: bool = False) -> None:
    """
    Initialize the SQLite database and ensure the 'streamed_messages' table exists.

    Args:
        db_path: Path to the SQLite database file.
        recreate: Drop and recreate the table (use True only for a fresh start).
    """
    logger.info(f"Calling SQLite init_db() with db_path={db_path}")
    try:
        db_path.parent.mkdir(parents=True, exist_ok=True)

        # Open connection with sane defaults for streaming
        with sqlite3.connect(db_path) as conn:
            # Improve concurrency: WAL allows readers while we write
            conn.execute("PRAGMA journal_mode=WAL;")
            # Good durability + performance tradeoff
            conn.execute("PRAGMA synchronous=NORMAL;")
            # Extra safety if a connection races us
            conn.execute("PRAGMA busy_timeout=5000;")

            cur = conn.cursor()
            if recreate:
                cur.execute("DROP TABLE IF EXISTS streamed_messages;")

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS streamed_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message TEXT,
                    author TEXT,
                    timestamp TEXT,
                    category TEXT,
                    sentiment REAL,
                    keyword_mentioned TEXT,
                    message_length INTEGER
                )
                """
            )
            conn.commit()

        logger.info(f"SUCCESS: Database initialized and table ready at {db_path}.")
    except Exception as e:
        logger.error(f"ERROR: Failed to initialize SQLite at {db_path}: {e}")
        raise


# -----------------------------
# Insert
# -----------------------------
def insert_message(
    message: Mapping[str, Any],
    db_path: pathlib.Path,
    *,
    max_retries: int = 6,
    base_sleep: float = 0.05,
) -> None:
    """
    Insert a single processed message into the SQLite database with retry.

    Args:
        message: Processed message dict.
        db_path: SQLite file path.
        max_retries: Retries on 'database is locked'.
        base_sleep: Initial backoff in seconds.
    """
    logger.info("Calling SQLite insert_message()")
    logger.info(f"message={message}")
    logger.info(f"db_path={db_path}")

    # Defensive casting in case upstream missed it
    payload = (
        str(message.get("message")),
        str(message.get("author")),
        str(message.get("timestamp")),
        str(message.get("category")),
        float(message.get("sentiment", 0.0)),
        str(message.get("keyword_mentioned")),
        int(message.get("message_length", 0)),
    )

    attempt = 0
    while True:
        try:
            with sqlite3.connect(db_path, timeout=5.0) as conn:
                conn.execute("PRAGMA busy_timeout=5000;")
                conn.execute(
                    """
                    INSERT INTO streamed_messages (
                        message, author, timestamp, category, sentiment, keyword_mentioned, message_length
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    payload,
                )
                conn.commit()
            logger.info("Inserted one message into the database.")
            return

        except sqlite3.OperationalError as e:
            # Handle transient locking during real-time streaming
            msg = str(e).lower()
            if ("locked" in msg or "busy" in msg) and attempt < max_retries:
                sleep_for = base_sleep * (2 ** attempt)
                logger.warning(
                    f"SQLite locked/busy (attempt {attempt+1}/{max_retries}); retrying in {sleep_for:.3f}s..."
                )
                time.sleep(sleep_for)
                attempt += 1
                continue
            logger.error(f"ERROR: SQLite insert failed (no more retries): {e}")
            raise
        except Exception as e:
            logger.error(f"ERROR: Failed to insert message: {e}")
            raise


# -----------------------------
# Delete
# -----------------------------
def delete_message(message_id: int, db_path: pathlib.Path) -> None:
    """
    Delete a message by id.
    """
    try:
        with sqlite3.connect(db_path, timeout=5.0) as conn:
            conn.execute("PRAGMA busy_timeout=5000;")
            conn.execute("DELETE FROM streamed_messages WHERE id = ?", (int(message_id),))
            conn.commit()
        logger.info(f"Deleted message id={message_id}")
    except Exception as e:
        logger.error(f"ERROR: Failed to delete message id={message_id}: {e}")
        raise


# -----------------------------
# Self-test (optional)
# -----------------------------
def main() -> None:
    logger.info("Starting sqlite_consumer_case self-test.")
    # Use a separate test DB next to your main DB path
    try:
        base_dir: pathlib.Path = config.get_sqlite_path().parent
    except Exception:
        # Fallback if config function differs
        base_dir = pathlib.Path("data")
    test_db = base_dir / "test_buzz.sqlite"

    init_db(test_db, recreate=True)

    sample = {
        "message": "I just shared a meme! It was amazing.",
        "author": "Charlie",
        "timestamp": "2025-01-29 14:35:20",
        "category": "humor",
        "sentiment": 0.87,
        "keyword_mentioned": "meme",
        "message_length": 42,
    }
    insert_message(sample, test_db)
    logger.info("Finished sqlite_consumer_case self-test.")


if __name__ == "__main__":
    main()
