"""
sqlite_consumer_case.py

Functions:
- init_db(db_path: Path): Initialize the SQLite database and ensure the 'streamed_messages' table exists.
- insert_message(message: dict, db_path: Path): Insert a single processed message.
- delete_message(message_id: int, db_path: Path): Delete by id.
"""

# -----------------------------
# Imports
# -----------------------------
import pathlib
import sqlite3
from typing import Optional

from utils.utils_logger import logger


# -----------------------------
# Connection helper (WAL + busy timeout)
# -----------------------------
def _connect(db_path: pathlib.Path) -> sqlite3.Connection:
    """
    Connect with settings that work well for real-time streaming on Windows.
    """
    conn = sqlite3.connect(str(db_path), timeout=5.0, isolation_level=None)  # autocommit
    cur = conn.cursor()
    # Make concurrent inserts reliable
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute("PRAGMA busy_timeout=5000;")
    cur.execute("PRAGMA foreign_keys=ON;")
    return conn


# -----------------------------
# Init
# -----------------------------
def init_db(db_path: pathlib.Path, recreate: bool = False) -> None:
    """
    Ensure DB file and streamed_messages table exist.
    Set recreate=True if you want a fresh table each run.
    """
    logger.info(f"Calling SQLite init_db() with db_path={db_path}")
    try:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = _connect(db_path)
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
            );
            """
        )
        logger.info(f"SUCCESS: Database initialized and table ready at {db_path}.")
        conn.close()
    except Exception as e:
        logger.error(f"ERROR: Failed to initialize SQLite at {db_path}: {e}")


# -----------------------------
# Insert
# -----------------------------
def insert_message(message: dict, db_path: pathlib.Path) -> None:
    """
    Insert one processed message.
    """
    try:
        conn = _connect(db_path)
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO streamed_messages
                (message, author, timestamp, category, sentiment, keyword_mentioned, message_length)
            VALUES (?, ?, ?, ?, ?, ?, ?);
            """,
            (
                message.get("message"),
                message.get("author"),
                message.get("timestamp"),
                message.get("category"),
                float(message.get("sentiment", 0.0)),
                message.get("keyword_mentioned"),
                int(message.get("message_length", 0)),
            ),
        )
        logger.info("Inserted one message into SQLite.")
        conn.close()
    except Exception as e:
        logger.error(f"ERROR: Failed to insert message into SQLite: {e}")


# -----------------------------
# Delete
# -----------------------------
def delete_message(message_id: int, db_path: pathlib.Path) -> None:
    """
    Delete a message by id.
    """
    try:
        conn = _connect(db_path)
        cur = conn.cursor()
        cur.execute("DELETE FROM streamed_messages WHERE id = ?;", (message_id,))
        logger.info(f"Deleted message with id {message_id} from SQLite.")
        conn.close()
    except Exception as e:
        logger.error(f"ERROR: Failed to delete message from SQLite: {e}")


# -----------------------------
# Local test
# -----------------------------
def main() -> None:
    logger.info("Starting SQLite db testing.")
    # simple local test path
    test_path = pathlib.Path("data/test_buzz.sqlite")
    init_db(test_path, recreate=True)

    sample = {
        "message": "Hello, SQLite!",
        "author": "Tester",
        "timestamp": "2025-01-01 00:00:00",
        "category": "test",
        "sentiment": 0.5,
        "keyword_mentioned": "hello",
        "message_length": 14,
    }
    insert_message(sample, test_path)

    # Confirm one row exists
    try:
        conn = _connect(test_path)
        cur = conn.cursor()
        row = cur.execute(
            "SELECT id, message FROM streamed_messages ORDER BY id DESC LIMIT 1;"
        ).fetchone()
        logger.info(f"Latest row: {row}")
        conn.close()
    except Exception as e:
        logger.error(f"Verification SELECT failed: {e}")

    logger.info("Finished SQLite db testing.")


if __name__ == "__main__":
    main()
