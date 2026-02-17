"""
WCM Revenue Insights Agent
============================
A Text-to-SQL agent that accepts natural language questions
and returns revenue insights from a music publishing database.

Usage:
    python main.py
"""

import os
import sys
import logging

from dotenv import load_dotenv

load_dotenv()

from wcm_agent.logging_config import setup_logging  # noqa: E402
from wcm_agent.config import validate_config, OUTPUT_DIR  # noqa: E402
from wcm_agent.db import init_database, create_current_songs_view  # noqa: E402
from wcm_agent.agent import ask_database  # noqa: E402

logger = logging.getLogger(__name__)


def main():
    setup_logging()

    print("=" * 50)
    print("  WCM Revenue Insights Agent")
    print("=" * 50)

    # ── Validate configuration ───────────────────────────
    try:
        validate_config()
    except RuntimeError as e:
        logger.critical("Startup failed: %s", e)
        print(f"\n  FATAL: {e}")
        sys.exit(1)

    # ── Initialize database ──────────────────────────────
    logger.info("Setting up database...")
    print("\nSetting up database...")
    try:
        conn = init_database()
    except FileNotFoundError as e:
        logger.critical("Data file missing: %s", e)
        print(f"  FATAL: {e}")
        sys.exit(1)

    create_current_songs_view(conn)
    print("  Database ready.\n")

    # ── Verify the view works ────────────────────────────
    print("  Current songs (via view):")
    rows = conn.execute("SELECT * FROM current_songs ORDER BY song_id").fetchall()
    for row in rows:
        print(f"    Song {row[0]}: '{row[1]}' (writer: {row[2]})")

    # ── Run the required test question ───────────────────
    print("\n" + "=" * 50)
    test_question = "What is the total revenue for Alex Park?"
    answer = ask_database(test_question, conn)
    print(f"\n  Answer: {answer}")
    print("=" * 50)

    # ── Save output ──────────────────────────────────────
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, "alex_park_result.txt")
    with open(output_path, "w") as f:
        f.write(f"Question: {test_question}\n")
        f.write(f"Answer: {answer}\n")
    logger.info("Output saved to: %s", output_path)
    print(f"\n  Output saved to: {output_path}")

    # ── Bonus questions ──────────────────────────────────
    bonus_questions = [
        "Which writer has the highest total revenue?",
        "What are the top 3 songs by total revenue?",
        "How many songs does each writer have?",
    ]

    print("\n" + "=" * 50)
    print("  Bonus Questions")
    print("=" * 50)
    for q in bonus_questions:
        answer = ask_database(q, conn)
        print(f"\n  Answer: {answer}")
        print("-" * 50)

    conn.close()
    logger.info("Agent finished successfully")


if __name__ == "__main__":
    main()
