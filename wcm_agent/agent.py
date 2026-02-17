"""
Text-to-SQL Agent
=================
Core agent that translates natural language questions into SQL,
executes them, and returns formatted answers.
"""

import json
import time
import logging
import os

from openai import OpenAI

from wcm_agent.config import (
    SCHEMA_DESCRIPTION,
    DEFAULT_MODEL,
    LLM_TEMPERATURE,
    MAX_RETRIES,
    INITIAL_BACKOFF_SECONDS,
    QUERY_TIMEOUT_SECONDS,
)
from wcm_agent.safety import validate_sql, sanitize_input, enforce_limit
from wcm_agent.formatters import format_result_deterministic

logger = logging.getLogger(__name__)


def _clean_sql_response(raw_sql):
    """Strip markdown code fences the LLM sometimes wraps SQL in."""
    sql = raw_sql.strip()
    if sql.startswith("```"):
        sql = sql.split("\n", 1)[1] if "\n" in sql else sql[3:]
    if sql.endswith("```"):
        sql = sql.rsplit("```", 1)[0]
    return sql.strip()


def ask_database(question, conn, max_retries=None):
    """
    The Text-to-SQL agent.

    Pipeline:
      1. Sanitise input
      2. LLM generates SQL (with exponential-backoff retry)
      3. Validate SQL safety
      4. Enforce row LIMIT
      5. Execute query (with timeout)
      6. Format result (LLM with deterministic fallback)

    Returns a human-readable answer string.
    """
    if max_retries is None:
        max_retries = MAX_RETRIES

    # ── Sanitise ─────────────────────────────────────────
    question = sanitize_input(question)
    if not question:
        return "ERROR: Empty question provided."

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return "ERROR: OPENAI_API_KEY not set. Copy .env.example to .env and add your key."

    client = OpenAI(api_key=api_key)

    # ── Step 1: Generate SQL via LLM ─────────────────────
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

    logger.info("Question: %s", question)

    generated_sql = None
    last_error = None
    for attempt in range(max_retries + 1):
        try:
            response = client.chat.completions.create(
                model=DEFAULT_MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": question},
                ],
                temperature=LLM_TEMPERATURE,
            )
            generated_sql = response.choices[0].message.content.strip()
            logger.debug("LLM response (attempt %d): %s", attempt + 1, generated_sql)
            break
        except Exception as e:
            last_error = e
            if attempt < max_retries:
                backoff = INITIAL_BACKOFF_SECONDS * (2 ** attempt)
                logger.warning(
                    "API error (attempt %d/%d), retrying in %.1fs: %s",
                    attempt + 1, max_retries + 1, backoff, e,
                )
                time.sleep(backoff)
            else:
                logger.error("API failed after %d attempts: %s", max_retries + 1, e)

    if generated_sql is None:
        return f"API ERROR: Could not generate SQL after {max_retries + 1} attempts: {last_error}"

    # Clean markdown fences
    generated_sql = _clean_sql_response(generated_sql)
    logger.info("Generated SQL: %s", generated_sql)

    # ── Step 2: Validate safety ──────────────────────────
    is_safe, reason = validate_sql(generated_sql)
    if not is_safe:
        logger.error("SQL rejected: %s", reason)
        return f"SAFETY ERROR: {reason}"

    # ── Step 3: Enforce LIMIT ────────────────────────────
    generated_sql = enforce_limit(generated_sql)

    # ── Step 4: Execute ──────────────────────────────────
    try:
        # Set timeout on the connection
        conn.execute(f"PRAGMA busy_timeout = {QUERY_TIMEOUT_SECONDS * 1000}")
        cursor = conn.execute(generated_sql)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
    except Exception as e:
        logger.error("SQL execution error: %s", e)
        return f"SQL EXECUTION ERROR: {e}"

    if not rows:
        logger.info("Query returned no results")
        return "No results found."

    # ── Step 5: Format results ───────────────────────────
    result_data = [dict(zip(columns, row)) for row in rows]
    deterministic_answer = format_result_deterministic(question, result_data)
    logger.info("Raw result: %s", json.dumps(result_data))

    # ── Step 6: LLM-formatted answer (with fallback) ────
    answer_prompt = f"""The user asked: "{question}"

The SQL query returned this data:
{json.dumps(result_data, indent=2)}

Provide a clear, concise answer to the user's question based on this data.
Include the specific numbers. Be brief — 1-2 sentences."""

    try:
        answer_response = client.chat.completions.create(
            model=DEFAULT_MODEL,
            messages=[
                {"role": "system", "content": "You are a helpful financial analyst. Give clear, data-backed answers."},
                {"role": "user", "content": answer_prompt},
            ],
            temperature=LLM_TEMPERATURE,
        )
        llm_answer = answer_response.choices[0].message.content.strip()
        logger.info("LLM-formatted answer: %s", llm_answer)
    except Exception as e:
        logger.warning("Answer formatting failed (%s), using deterministic fallback", e)
        llm_answer = deterministic_answer

    return llm_answer
