"""
SQL Safety & Input Sanitisation
================================
Validates generated SQL, sanitises user input, and enforces
query limits for defense-in-depth security.
"""

import re
import logging

from wcm_agent.config import MAX_QUESTION_LENGTH, MAX_RESULT_ROWS

logger = logging.getLogger(__name__)


def validate_sql(sql):
    """
    Safety check: block any destructive or non-SELECT SQL commands.

    Three-layer defense:
      1. Comment stripping — removes SQL comments before validation
      2. SELECT whitelist — query must start with SELECT
      3. Word-boundary blocklist — rejects destructive keywords
      4. Multi-statement blocking — no piggybacked commands

    Returns (is_safe: bool, reason: str)
    """
    # Strip SQL comments (-- and /* */)
    cleaned = re.sub(r"--.*$", "", sql, flags=re.MULTILINE)
    cleaned = re.sub(r"/\*.*?\*/", "", cleaned, flags=re.DOTALL)
    cleaned = cleaned.strip()

    # Layer 1: Must start with SELECT
    if not cleaned.upper().startswith("SELECT"):
        logger.warning("SQL blocked — does not start with SELECT: %s", sql[:80])
        return False, "Blocked: Only SELECT queries are allowed."

    # Layer 2: No destructive keywords as standalone words
    blocked_keywords = [
        "DROP", "DELETE", "INSERT", "UPDATE",
        "ALTER", "CREATE", "TRUNCATE", "EXEC", "EXECUTE",
    ]
    for keyword in blocked_keywords:
        if re.search(rf"\b{keyword}\b", cleaned, re.IGNORECASE):
            logger.warning("SQL blocked — contains '%s': %s", keyword, sql[:80])
            return False, f"Blocked: SQL contains '{keyword}' which is not allowed."

    # Layer 3: Block multiple statements (semicolons followed by more SQL)
    if re.search(r";\s*\S", cleaned):
        logger.warning("SQL blocked — multiple statements: %s", sql[:80])
        return False, "Blocked: Multiple SQL statements are not allowed."

    return True, "OK"


def sanitize_input(question):
    """
    Sanitise user input before sending to the LLM.

    - Strips leading/trailing whitespace
    - Removes control characters (except newlines)
    - Truncates to MAX_QUESTION_LENGTH
    """
    if not question:
        return ""

    # Remove control characters (keep newlines and tabs)
    cleaned = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", question)
    cleaned = cleaned.strip()

    if len(cleaned) > MAX_QUESTION_LENGTH:
        logger.info(
            "Input truncated from %d to %d characters",
            len(cleaned), MAX_QUESTION_LENGTH,
        )
        cleaned = cleaned[:MAX_QUESTION_LENGTH]

    return cleaned


def enforce_limit(sql, max_rows=None):
    """
    Append a LIMIT clause if one is not already present.

    Prevents accidental full-table dumps from LLM-generated queries.
    """
    if max_rows is None:
        max_rows = MAX_RESULT_ROWS

    # Check if LIMIT already exists (case-insensitive)
    if re.search(r"\bLIMIT\b", sql, re.IGNORECASE):
        return sql

    # Strip trailing semicolon, add LIMIT, re-add semicolon
    stripped = sql.rstrip().rstrip(";")
    limited = f"{stripped} LIMIT {max_rows}"
    logger.debug("Auto-appended LIMIT %d to query", max_rows)
    return limited
