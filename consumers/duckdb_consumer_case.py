# consumers/duckdb_consumer_case.py
"""
DuckDB helpers for streaming inserts.

Functions:
- init_db(db_path: Path, recreate: bool = False) -> None
- insert_message(message: dict, db_path: Path, ...) -> None
- delete_message(message_id: int, db_path: Path) -> None
"""

from __future__ import annotations

# stdlib
import os
import time
import pathlib
from typing import Mapping, Any, Dict

# external
import duckdb

# local
from utils.utils_logger import logger


# -----------------------------------------------------------------------------
# Module-level connection cache (one connection per DuckDB file for streaming)
# -----------------------------------------------------------------------------
_CONNECTIONS: Dict[pathlib.Path, duckdb.DuckDBPyConnection] = {}


def _get_conn(db_path: pathlib.Path) -> duckdb.DuckDBPyConnection:
    """Return a cached connection to DuckDB for this path; recreate if needed."""
    conn = _CONNECTIONS.get(db_path)
    try:
        if conn is not None:
            # Simple health check; will raise if closed/invalid
            conn.execute("SELECT 1;").fetchone()
            return conn
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        conn = None

    # Create a new connection
    logger.info(f"Connecting to DuckDB at {db_path}")
    conn = duckdb.connect(str(db_path), read_only=False)
    # Optional performance/concurrency knobs:
    # - threads: let DuckDB use multiple threads internally
    # - busy_wait_timeout: wait a bit if file is locked by another process (DuckDB >=0.10)
    try:
        conn.execute("PRAGMA threads=4;")
    except Exception:
        pass
    try:
        conn.execute("PRAGMA busy_wait_timeout=5000;")
    except Exception:
        # Older DuckDB versions may not support this PRAGMA; retry backoff will handle contention.
        pass

    _CONNECTIONS[db_path] = conn
    return conn


# -----------------------------------------------------------------------------
# Initialize DuckDB database / table
# -----------------------------------------------------------------------------
def init_db(db_path: pathlib.Path, *, recreate: bool = False) -> None:
    """
    Ensure the 'streamed_messages' table exists.

    Args:
        db_path: Path to the DuckDB file (e.g., Path('data/buzz.duckdb'))
        recreate: If True, drop and recreate the table (fresh start). Default False.
    """
    logger.info(f"Calling DuckDB init_db() with db_path={db_path}, recreate={recreate}")
    try:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        con = _get_conn(db_path)

        if recreate:
            con.execute("DROP TABLE IF EXISTS streamed_messages;")

        con.execute(
            """
            CREATE TABLE IF NOT EXISTS streamed_messages (
                id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                message TEXT,
                author TEXT,
                timestamp TEXT,
                category TEXT,
                sentiment DOUBLE,
                keyword_mentioned TEXT,
                message_length INTEGER
            );
            """
        )
        logger.info(f"SUCCESS: Table 'streamed_messages' ready at {db_path}.")
    except Exception as e:
        logger.error(f"ERROR: Failed to initialize DuckDB at {db_path}: {e}")
        raise


# -----------------------------------------------------------------------------
# Insert one message (robust for real-time streaming)
# -----------------------------------------------------------------------------
def insert_message(
    message: Mapping[str, Any],
    db_path: pathlib.Path,
    *,
    max_retries: int = 6,
    base_sleep: float = 0.05,
) -> None:
    """
    Insert a single processed message into DuckDB with retry on transient locks.

    Args:
        message: dict with keys matching table columns
        db_path: Path to the DuckDB file
        max_retries: Number of retries on lock/busy errors
        base_sleep: Initial backoff (seconds); grows exponentially
    """
    logger.info("Calling DuckDB insert_message()")
    logger.info(f"message={message}")
    logger.info(f"db_path={db_path}")

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
            con = _get_conn(db_path)
            con.execute(
                """
                INSERT INTO streamed_messages
                    (message, author, timestamp, category, sentiment, keyword_mentioned, message_length)
                VALUES (?, ?, ?, ?, ?, ?, ?);
                """,
                payload,
            )
            logger.info("Inserted one message into DuckDB.")
            return

        except Exception as e:
            msg = str(e).lower()
            # DuckDB can raise duckdb.IOException / duckdb.Error with lock/busy wording on Windows.
            locked = any(
                s in msg
                for s in [
                    "locked",                 # generic
                    "busy",                   # generic
                    "another process",        # windows file lock wording
                    "database is in use",     # variant
                ]
            )
            if locked and attempt < max_retries:
                sleep_for = base_sleep * (2 ** attempt)
                logger.warning(
                    f"DuckDB locked/busy (attempt {attempt+1}/{max_retries}); "
                    f"retrying in {sleep_for:.3f}s... Details: {e}"
                )
                time.sleep(sleep_for)
                attempt += 1
                continue

            logger.error(f"ERROR: DuckDB insert failed (no more retries): {e}")
            raise


# -----------------------------------------------------------------------------
# Delete by ID
# -----------------------------------------------------------------------------
def delete_message(message_id: int, db_path: pathlib.Path) -> None:
    """
    Delete a message from DuckDB by id.
    """
    try:
        con = _get_conn(db_path)
        con.execute("DELETE FROM streamed_messages WHERE id = ?;", [int(message_id)])
        logger.info(f"Deleted message with id {message_id} from DuckDB.")
    except Exception as e:
        logger.error(f"ERROR: Failed to delete message from DuckDB: {e}")
        raise


# -----------------------------------------------------------------------------
# Local smoke test (optional)
# -----------------------------------------------------------------------------
def _resolve_duckdb_path() -> pathlib.Path:
    """
    Resolve a DuckDB file path using utils_config if present,
    else fallback to BASE_DATA_DIR/DUCKDB_DB_FILE_NAME.
    """
    try:
        import utils.utils_config as _cfg  # type: ignore

        if hasattr(_cfg, "get_duckdb_path"):
            return _cfg.get_duckdb_path()
    except Exception:
        pass

    base_dir = os.getenv("BASE_DATA_DIR", "data")
    file_name = os.getenv("DUCKDB_DB_FILE_NAME", "buzz.duckdb")
    return pathlib.Path(base_dir) / file_name


def main() -> None:
    logger.info("Starting DuckDB db testing.")
    db_path = _resolve_duckdb_path()
    logger.info(f"Using DuckDB file at: {db_path}")

    # Fresh table for the smoke test
    init_db(db_path, recreate=True)

    sample = {
        "message": "I just shared a meme! It was amazing.",
        "author": "Charlie",
        "timestamp": "2025-01-29 14:35:20",
        "category": "humor",
        "sentiment": 0.87,
        "keyword_mentioned": "meme",
        "message_length": 42,
    }
    insert_message(sample, db_path)

    # Verify:
    try:
        con = _get_conn(db_path)
        row = con.execute(
            """
            SELECT id, message, author
            FROM streamed_messages
            WHERE message = ? AND author = ?
            ORDER BY id DESC
            LIMIT 1;
            """,
            [sample["message"], sample["author"]],
        ).fetchone()

        if row:
            inserted_id = row[0]
            logger.info(f"Verified insert. Latest row id={inserted_id}")
            delete_message(inserted_id, db_path)
        else:
            logger.warning("Test row not found; nothing to delete.")
    except Exception as e:
        logger.error(f"ERROR: Verification SELECT failed: {e}")

    logger.info("Finished DuckDB db testing.")


if __name__ == "__main__":
    main()
