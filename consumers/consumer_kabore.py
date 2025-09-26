"""
consumer_kabore.py

Custom real-time consumer (Kabore).
- Ingests messages from the live data file (JSON Lines; one JSON object per line).
- Processes JUST ONE MESSAGE AT A TIME as it arrives.
- Stores the processed result in SQLite (table: streamed_messages).

Expected JSON per line:
{
    "message": "I just shared a meme! It was amazing.",
    "author": "Charlie",
    "timestamp": "2025-01-29 14:35:20",
    "category": "humor",
    "sentiment": 0.87,
    "keyword_mentioned": "meme",
    "message_length": 42
}

Derived checks (logged but not stored to keep schema compatibility):
- sentiment_label: "positive" | "neutral" | "negative"
- exclamation_flag: True if message contains "!"
- word_count: len(message.split())

Run:
    py -m consumers.consumer_kabore
"""

from __future__ import annotations

# Standard library
import json
import pathlib
import time
from typing import Any, Dict, Optional

# Local modules
import utils.utils_config as config
from utils.utils_logger import logger
from consumers.sqlite_consumer_case import init_db, insert_message


# -------------------------------
# Helpers
# -------------------------------

def _coerce_float(val: Any, default: float = 0.0) -> float:
    try:
        return float(val)
    except Exception:
        return default


def _coerce_int(val: Any, default: int = 0) -> int:
    try:
        return int(val)
    except Exception:
        return default


def _sentiment_label(score: float) -> str:
    if score > 0.6:
        return "positive"
    if score < 0.4:
        return "negative"
    return "neutral"


def process_message(msg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Normalize and validate one message to match the streamed_messages schema.
    Returns a dict with exactly the 7 expected keys or None if invalid.
    """
    try:
        raw_message = (msg.get("message") or "").strip()
        author = (msg.get("author") or "").strip()
        timestamp = (msg.get("timestamp") or "").strip()
        category = (msg.get("category") or "").strip()
        sentiment = _coerce_float(msg.get("sentiment"), 0.0)
        keyword_mentioned = (msg.get("keyword_mentioned") or "").strip()

        # If message_length is missing or invalid, compute from text
        message_length = msg.get("message_length")
        if isinstance(message_length, int):
            ml = message_length
        else:
            ml = _coerce_int(message_length, 0)
        if ml <= 0 and raw_message:
            ml = len(raw_message)

        # Quick derived insights (logged only so we don't break your current table schema)
        label = _sentiment_label(sentiment)
        exclamation = "!" in raw_message
        word_count = len(raw_message.split()) if raw_message else 0
        logger.info(
            f"Derived insights -> sentiment_label='{label}', "
            f"exclamation_flag={exclamation}, word_count={word_count}"
        )

        processed = {
            "message": raw_message,
            "author": author,
            "timestamp": timestamp,
            "category": category,
            "sentiment": sentiment,
            "keyword_mentioned": keyword_mentioned,
            "message_length": ml,
        }
        logger.info(f"Processed message: {processed}")
        return processed
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None


def _safe_json_loads(line: str) -> Optional[Dict[str, Any]]:
    """
    Parse a single JSON object from a line.
    Returns None for blank/invalid lines instead of raising.
    """
    s = line.strip()
    if not s:
        return None
    # Require object-looking lines to reduce noise
    if not (s.startswith("{") and s.endswith("}")):
        return None
    try:
        return json.loads(s)
    except Exception as e:
        logger.warning(f"Skipping invalid JSON line: {e} | line={s[:200]}")
        return None


# -------------------------------
# Real-time tailing loop
# -------------------------------

def tail_file_realtime(live_path: pathlib.Path, sqlite_path: pathlib.Path, interval_secs: int = 1) -> None:
    """
    Tail the live data file in real time and write each valid message to SQLite.
    This uses a simple 'tail -f' style loop:
      - read from the last known position
      - if no new data, sleep briefly and retry
      - if file is truncated/rotated, seek to start
    """
    logger.info(f"Starting real-time tail on {live_path}")
    # Ensure DB exists with the correct table. Do NOT drop existing data here.
    try:
        init_db(sqlite_path)
        logger.info(f"Database initialized/ready at {sqlite_path}.")
    except Exception as e:
        logger.error(f"Failed to init database at {sqlite_path}: {e}")

    last_pos = 0
    # Optional: start from end if you only want new lines going forward.
    start_from_end = False  # set True if needed
    try:
        if start_from_end and live_path.exists():
            last_pos = live_path.stat().st_size
    except Exception:
        last_pos = 0

    while True:
        try:
            if not live_path.exists():
                logger.warning(f"Live data file not found: {live_path}. Retrying...")
                time.sleep(interval_secs)
                continue

            file_size = live_path.stat().st_size
            # Handle truncation / rotation
            if file_size < last_pos:
                logger.info("Detected file truncation/rotation. Resetting position to 0.")
                last_pos = 0

            with live_path.open("r", encoding="utf-8", errors="ignore") as f:
                f.seek(last_pos)
                new_data = f.read()

                if not new_data:
                    # No new data yet, sleep and continue
                    time.sleep(interval_secs)
                    continue

                # Process complete lines only; keep partial tail (if any) for next read
                lines = new_data.splitlines()
                # If the file did not end with newline, the last line might be incomplete.
                has_trailing_newline = new_data.endswith("\n") or new_data.endswith("\r")
                complete_count = len(lines) if has_trailing_newline else max(len(lines) - 1, 0)

                for i in range(complete_count):
                    msg_dict = _safe_json_loads(lines[i])
                    if msg_dict is None:
                        continue
                    processed = process_message(msg_dict)
                    if processed:
                        insert_message(processed, sqlite_path)

                # Advance file position by only the bytes we consumed
                if complete_count > 0:
                    consumed_text = "\n".join(lines[:complete_count]) + "\n"
                    last_pos += len(consumed_text.encode("utf-8", errors="ignore"))
                else:
                    # No complete lines, don't move the pointer; wait for more bytes
                    time.sleep(interval_secs)

        except KeyboardInterrupt:
            logger.warning("Consumer interrupted by user.")
            break
        except Exception as e:
            logger.error(f"Unexpected error in tail loop: {e}")
            time.sleep(interval_secs)  # brief backoff and continue


# -------------------------------
# Main
# -------------------------------

def main() -> None:
    """
    Read config, then stream live file -> SQLite in real time.
    """
    logger.info("Kabore consumer starting (file -> SQLite, real-time).")

    try:
        interval_secs: int = config.get_message_interval_seconds_as_int()
        live_data_path: pathlib.Path = config.get_live_data_path()
        sqlite_path: pathlib.Path = config.get_sqlite_path()
        logger.info("Read environment configuration successfully.")
        logger.info(f"   MESSAGE_INTERVAL_SECONDS: {interval_secs}")
        logger.info(f"   LIVE_DATA_PATH: {live_data_path}")
        logger.info(f"   SQLITE_PATH: {sqlite_path}")
    except Exception as e:
        logger.error(f"Failed to read config: {e}")
        return

    # Start real-time streaming
    tail_file_realtime(live_data_path, sqlite_path, interval_secs)


if __name__ == "__main__":
    main()
