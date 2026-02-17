"""
Database Setup
==============
Loads CSV data into an in-memory SQLite database and creates
the current_songs deduplication view.
"""

import csv
import os
import sqlite3
import logging

from wcm_agent.config import DATA_DIR

logger = logging.getLogger(__name__)


def init_database():
    """
    Create an in-memory SQLite database and load all 3 CSVs.

    Returns the database connection with row_factory = sqlite3.Row.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    # Set a busy timeout to avoid immediate locking errors
    conn.execute("PRAGMA busy_timeout = 5000")

    # ── Create tables ────────────────────────────────────
    conn.execute("""
        CREATE TABLE dim_writer (
            writer_id INTEGER PRIMARY KEY,
            writer_name TEXT NOT NULL
        )
    """)

    conn.execute("""
        CREATE TABLE dim_song (
            song_id INTEGER,
            title TEXT NOT NULL,
            writer_id INTEGER,
            etl_date TEXT,
            FOREIGN KEY (writer_id) REFERENCES dim_writer(writer_id)
        )
    """)

    conn.execute("""
        CREATE TABLE fact_royalties (
            transaction_id TEXT PRIMARY KEY,
            song_id INTEGER,
            amount_usd REAL,
            FOREIGN KEY (song_id) REFERENCES dim_song(song_id)
        )
    """)

    # ── Load CSV data ────────────────────────────────────
    for table_name in ["dim_writer", "dim_song", "fact_royalties"]:
        file_path = os.path.join(DATA_DIR, f"{table_name}.csv")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Data file not found: {file_path}")

        with open(file_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            columns = reader.fieldnames
            placeholders = ", ".join(["?"] * len(columns))
            col_names = ", ".join(columns)
            row_count = 0
            for row in reader:
                values = [row[col] for col in columns]
                conn.execute(
                    f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})",
                    values,
                )
                row_count += 1

        logger.info("Loaded %s: %d rows", table_name, row_count)

    conn.commit()
    return conn


def create_current_songs_view(conn):
    """
    Create a view that returns only the most recent title per song.

    WHY: dim_song has historical records (e.g., song_id 1 has both
    "Starlight (Draft)" and "Starlight"). If we join dim_song directly
    to fact_royalties, each transaction for song 1 would appear TWICE,
    doubling the revenue. This view solves that.

    HOW: For each song_id, pick the row with the latest etl_date.
    """
    conn.execute("""
        CREATE VIEW IF NOT EXISTS current_songs AS
        SELECT song_id, title, writer_id
        FROM dim_song
        WHERE rowid IN (
            SELECT rowid FROM dim_song d1
            WHERE d1.etl_date = (
                SELECT MAX(d2.etl_date) FROM dim_song d2
                WHERE d2.song_id = d1.song_id
            )
        )
    """)
    conn.commit()
    logger.info("Created current_songs deduplication view")
