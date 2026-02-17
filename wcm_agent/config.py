"""
Configuration & Constants
=========================
Centralizes all configuration values, schema descriptions,
and startup validation for the WCM Revenue Agent.
"""

import os
import logging

logger = logging.getLogger(__name__)

# ── Paths ────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
LOG_DIR = os.path.join(BASE_DIR, "logs")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

# ── LLM Settings ────────────────────────────────────────
DEFAULT_MODEL = "gpt-4o-mini"
LLM_TEMPERATURE = 0.0

# ── Retry / Resilience ──────────────────────────────────
MAX_RETRIES = 3
INITIAL_BACKOFF_SECONDS = 1.0  # doubles each retry

# ── Safety ──────────────────────────────────────────────
MAX_RESULT_ROWS = 1000
QUERY_TIMEOUT_SECONDS = 30
MAX_QUESTION_LENGTH = 500

# ── Required Data Files ─────────────────────────────────
REQUIRED_DATA_FILES = ["dim_writer.csv", "dim_song.csv", "fact_royalties.csv"]

# ── Schema Description (sent to the LLM) ────────────────
SCHEMA_DESCRIPTION = """
You have access to a music publishing royalties database with these tables:

TABLE: dim_writer
- writer_id (INTEGER, PRIMARY KEY) — Unique ID for each songwriter
- writer_name (TEXT) — Full name of the songwriter

TABLE: dim_song
- song_id (INTEGER) — Unique ID for each song (NOTE: a song_id may appear multiple times due to historical title changes)
- title (TEXT) — Song title (may have changed over time)
- writer_id (INTEGER, FOREIGN KEY → dim_writer.writer_id) — The songwriter who wrote this song
- etl_date (TEXT) — Date this record was loaded. Use the row with the LATEST etl_date per song_id to get the current title.

TABLE: fact_royalties
- transaction_id (TEXT, PRIMARY KEY) — Unique transaction ID
- song_id (INTEGER, FOREIGN KEY → dim_song.song_id) — The song this royalty is for
- amount_usd (REAL) — Revenue amount in USD

VIEW: current_songs
- A pre-built view that returns only the LATEST title for each song_id.
- Columns: song_id, title, writer_id
- USE THIS VIEW instead of dim_song when joining to fact_royalties to avoid double-counting.

RELATIONSHIPS:
- dim_writer.writer_id → dim_song.writer_id (one writer has many songs)
- dim_song.song_id → fact_royalties.song_id (one song has many royalty transactions)
- Use current_songs instead of dim_song for accurate revenue calculations.
"""


def validate_config():
    """
    Validate that all required configuration is present at startup.

    Checks:
      1. OPENAI_API_KEY environment variable is set
      2. All required data files exist

    Raises RuntimeError on failure with an actionable message.
    """
    errors = []

    # Check API key
    if not os.getenv("OPENAI_API_KEY"):
        errors.append(
            "OPENAI_API_KEY not set. Copy .env.example to .env and add your key."
        )

    # Check data files
    for filename in REQUIRED_DATA_FILES:
        filepath = os.path.join(DATA_DIR, filename)
        if not os.path.exists(filepath):
            errors.append(f"Data file not found: {filepath}")

    if errors:
        for err in errors:
            logger.error("Config validation failed: %s", err)
        raise RuntimeError(
            "Startup validation failed:\n  - " + "\n  - ".join(errors)
        )

    logger.info("Configuration validated successfully")
