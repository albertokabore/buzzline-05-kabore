"""
duckdb_consumer_case.py

Functions:
- init_db(db_path: Path): Initialize (or recreate) the 'streamed_messages' table.
- insert_message(message: dict, db_path: Path): Insert one processed message.
- delete_message(message_id: int, db_path: Path): Delete by id.
"""

# -----------------------------
# Imports
# -----------------------------
import pathlib

import duckdb
from utils.utils_logger import logger


# -----------------------------
# Init
# -----------------------------
def init_db(db_path: pathlib.Path, recreate: bool = False) -> None:
    """
    Create (or recreate) the streamed_messages table in a DuckDB database.
    """
    logger.info(f"Calling DuckDB init_db() with db_path={db_path}")
    try:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(db_path))

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
        con.close()
    except Exception as e:
        logger.error(f"ERROR: Failed to initialize DuckDB at {db_path}: {e}")


# -----------------------------
# Insert
# -----------------------------
def insert_message(message: dict, db_path: pathlib.Path) -> None:
    """
    Insert a single processed message into DuckDB.
    """
    try:
        con = duckdb.connect(str(db_path))
        con.execute(
            """
            INSERT INTO streamed_messages
                (message, author, timestamp, category, sentiment, keyword_mentioned, message_length)
            VALUES (?, ?, ?, ?, ?, ?, ?);
            """,
            [
                message.get("message"),
                message.get("author"),
                message.get("timestamp"),
                message.get("category"),
                float(message.get("sentiment", 0.0)),
                message.get("keyword_mentioned"),
                int(message.get("message_length", 0)),
            ],
        )
        logger.info("Inserted one message into DuckDB.")
        con.close()
    except Exception as e:
        logger.error(f"ERROR: Failed to insert message into DuckDB: {e}")


# -----------------------------
# Delete
# -----------------------------
def delete_message(message_id: int, db_path: pathlib.Path) -> None:
    """
    Delete a message from DuckDB by id.
    """
    try:
        con = duckdb.connect(str(db_path))
        con.execute("DELETE FROM streamed_messages WHERE id = ?;", [message_id])
        logger.info(f"Deleted message with id {message_id} from DuckDB.")
        con.close()
    except Exception as e:
        logger.error(f"ERROR: Failed to delete message from DuckDB: {e}")


# -----------------------------
# Local test
# -----------------------------
def main() -> None:
    logger.info("Starting DuckDB db testing.")
    db_path = pathlib.Path("data/buzz.duckdb")
    init_db(db_path, recreate=True)

    test_message = {
        "message": "Hello, DuckDB!",
        "author": "Tester",
        "timestamp": "2025-01-01 00:00:00",
        "category": "test",
        "sentiment": 0.75,
        "keyword_mentioned": "hello",
        "message_length": 14,
    }
    insert_message(test_message, db_path)

    try:
        con = duckdb.connect(str(db_path))
        row = con.execute(
            """
            SELECT id, message, author
            FROM streamed_messages
            ORDER BY id DESC
            LIMIT 1;
            """
        ).fetchone()
        logger.info(f"Latest row: {row}")
        con.close()
    except Exception as e:
        logger.error(f"Verification SELECT failed: {e}")

    logger.info("Finished DuckDB db testing.")


if __name__ == "__main__":
    main()
