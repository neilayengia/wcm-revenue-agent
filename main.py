"""
WCM Revenue Insights Agent
============================
A Text-to-SQL agent that accepts natural language questions
and returns revenue insights from a music publishing database.

Usage:
    python main.py
"""

import sqlite3
import csv
import os
import json
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

# ──────────────────────────────────────────────────────────
# PART 1: DATABASE SETUP
# Load the 3 CSV files into an in-memory SQLite database.
#
# WHY in-memory? The prompt says "temporary in-memory SQL database."
# This means the DB exists only while the script runs — no file saved.
#
# IMPORTANT: dim_song has duplicate song_ids (title changes over time).
# We need to handle this to avoid double-counting revenue.
# ──────────────────────────────────────────────────────────

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


def init_database():
    """
    Create an in-memory SQLite database and load all 3 CSVs.

    Returns the database connection.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    # Create tables
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

    # Load CSV data into tables
    for table_name in ["dim_writer", "dim_song", "fact_royalties"]:
        file_path = os.path.join(DATA_DIR, f"{table_name}.csv")
        with open(file_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            columns = reader.fieldnames
            placeholders = ", ".join(["?"] * len(columns))
            col_names = ", ".join(columns)
            for row in reader:
                values = [row[col] for col in columns]
                conn.execute(
                    f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})",
                    values,
                )

    conn.commit()

    # Verify: print row counts
    for table in ["dim_writer", "dim_song", "fact_royalties"]:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  Loaded {table}: {count} rows")

    return conn


# ──────────────────────────────────────────────────────────
# PART 2: THE AGENT FUNCTION
#
# This is the core of the assessment. The function:
#   1. Takes a natural language question
#   2. Sends the database SCHEMA to the LLM (not the data!)
#   3. LLM generates a SQL query
#   4. We VALIDATE the SQL (no destructive commands)
#   5. Execute and return the result
#
# KEY DESIGN DECISION: We create a VIEW that handles the
# dim_song deduplication. This way the LLM doesn't need to
# figure out the historical records problem — we solve it
# in the schema itself.
# ──────────────────────────────────────────────────────────

# The schema description we send to the LLM.
# This is what teaches the AI about our database structure.
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
        CREATE VIEW current_songs AS
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


def validate_sql(sql):
    """
    Safety check: block any destructive SQL commands.

    In a real production system, you'd also use a read-only
    database connection. This is an additional layer of defense.

    Returns (is_safe, reason)
    """
    blocked_keywords = ["DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE", "TRUNCATE"]
    sql_upper = sql.upper().strip()
    for keyword in blocked_keywords:
        if keyword in sql_upper:
            return False, f"Blocked: SQL contains '{keyword}' which is not allowed."
    if not sql_upper.startswith("SELECT"):
        return False, "Blocked: Only SELECT queries are allowed."
    return True, "OK"


def ask_database(question, conn):
    """
    The Text-to-SQL agent.

    Takes a natural language question, uses an LLM to generate SQL,
    validates and executes it, then returns a human-readable answer.
    """
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    # Step 1: Ask the LLM to generate SQL
    system_prompt = f"""You are a SQL expert for a music publishing company.
Given the following database schema, generate a SQLite-compatible SQL query
to answer the user's question.

{SCHEMA_DESCRIPTION}

RULES:
- Return ONLY the SQL query, nothing else.
- Do NOT wrap it in markdown code blocks.
- Use the current_songs VIEW (not dim_song directly) when calculating revenue to avoid double-counting from historical title records.
- Always use ROUND() for monetary amounts to 2 decimal places.
- Use SUM() for total revenue calculations.
"""

    print(f"\n  Question: {question}")
    print("  Generating SQL...")

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": question},
        ],
        temperature=0.0,
    )

    generated_sql = response.choices[0].message.content.strip()

    # Clean up in case LLM wraps in code blocks anyway
    if generated_sql.startswith("```"):
        generated_sql = generated_sql.split("\n", 1)[1]
    if generated_sql.endswith("```"):
        generated_sql = generated_sql.rsplit("```", 1)[0]
    generated_sql = generated_sql.strip()

    print(f"  Generated SQL: {generated_sql}")

    # Step 2: Validate — no destructive commands
    is_safe, reason = validate_sql(generated_sql)
    if not is_safe:
        return f"SAFETY ERROR: {reason}"

    # Step 3: Execute the query
    try:
        cursor = conn.execute(generated_sql)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
    except Exception as e:
        return f"SQL EXECUTION ERROR: {e}"

    if not rows:
        return "No results found."

    # Step 4: Format the result
    result_lines = []
    for row in rows:
        row_dict = dict(zip(columns, row))
        result_lines.append(row_dict)

    # Step 5: Use LLM to generate a human-readable answer
    answer_prompt = f"""The user asked: "{question}"

The SQL query returned this data:
{json.dumps(result_lines, indent=2)}

Provide a clear, concise answer to the user's question based on this data.
Include the specific numbers. Be brief — 1-2 sentences."""

    answer_response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a helpful financial analyst. Give clear, data-backed answers."},
            {"role": "user", "content": answer_prompt},
        ],
        temperature=0.0,
    )

    answer = answer_response.choices[0].message.content.strip()
    return answer


# ──────────────────────────────────────────────────────────
# PART 3: RUN THE TEST
# ──────────────────────────────────────────────────────────

def main():
    print("=" * 50)
    print("  WCM Revenue Insights Agent")
    print("=" * 50)

    # Initialize database
    print("\nSetting up database...")
    conn = init_database()
    create_current_songs_view(conn)
    print("  Database ready.\n")

    # Verify the view works (debug check)
    print("  Current songs (via view):")
    rows = conn.execute("SELECT * FROM current_songs ORDER BY song_id").fetchall()
    for row in rows:
        print(f"    Song {row[0]}: '{row[1]}' (writer: {row[2]})")

    # Run the required test question
    print("\n" + "=" * 50)
    test_question = "What is the total revenue for Alex Park?"
    answer = ask_database(test_question, conn)
    print(f"\n  Answer: {answer}")
    print("=" * 50)

    # Save output
    output_path = os.path.join(os.path.dirname(__file__), "output", "alex_park_result.txt")
    with open(output_path, "w") as f:
        f.write(f"Question: {test_question}\n")
        f.write(f"Answer: {answer}\n")
    print(f"\n  Output saved to: {output_path}")

    # Bonus: test with additional questions to show robustness
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


if __name__ == "__main__":
    main()
