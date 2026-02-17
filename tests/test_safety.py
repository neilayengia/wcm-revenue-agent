"""
Unit tests for SQL validation, input sanitisation, and LIMIT enforcement.
"""

import pytest

from wcm_agent.safety import validate_sql, sanitize_input, enforce_limit


# ── validate_sql ─────────────────────────────────────────


class TestValidateSQL:
    """Tests for the SQL safety validator."""

    def test_valid_select(self):
        is_safe, reason = validate_sql("SELECT * FROM dim_writer")
        assert is_safe is True
        assert reason == "OK"

    def test_valid_select_with_joins(self):
        sql = """
            SELECT dw.writer_name, ROUND(SUM(fr.amount_usd), 2)
            FROM fact_royalties fr
            JOIN current_songs cs ON fr.song_id = cs.song_id
            JOIN dim_writer dw ON cs.writer_id = dw.writer_id
            WHERE dw.writer_name = 'Alex Park'
        """
        is_safe, _ = validate_sql(sql)
        assert is_safe is True

    def test_block_drop(self):
        is_safe, reason = validate_sql("DROP TABLE dim_writer")
        assert is_safe is False
        assert "SELECT" in reason

    def test_block_delete(self):
        is_safe, reason = validate_sql("DELETE FROM dim_writer")
        assert is_safe is False
        assert "SELECT" in reason

    def test_block_insert(self):
        is_safe, reason = validate_sql(
            "INSERT INTO dim_writer VALUES (999, 'Hacker')"
        )
        assert is_safe is False

    def test_block_update(self):
        is_safe, reason = validate_sql(
            "UPDATE dim_writer SET writer_name = 'Hacker' WHERE writer_id = 101"
        )
        assert is_safe is False

    def test_block_alter(self):
        is_safe, reason = validate_sql("ALTER TABLE dim_writer ADD COLUMN age INTEGER")
        assert is_safe is False

    def test_block_truncate(self):
        is_safe, reason = validate_sql("TRUNCATE TABLE dim_writer")
        assert is_safe is False

    def test_block_select_then_drop_multistatement(self):
        """Semicolon-injection: SELECT followed by DROP."""
        is_safe, reason = validate_sql(
            "SELECT * FROM dim_writer; DROP TABLE dim_writer"
        )
        assert is_safe is False
        assert "Multiple" in reason or "DROP" in reason

    def test_block_comment_injection(self):
        """Destructive SQL hidden after a comment on the same line."""
        sql = "SELECT 1 -- innocuous\n; DROP TABLE dim_writer"
        is_safe, _ = validate_sql(sql)
        assert is_safe is False

    def test_allow_column_with_keyword_name(self):
        """Column names like 'updated_at' should NOT trigger 'UPDATE' block."""
        sql = "SELECT updated_at, created_date FROM some_table"
        is_safe, _ = validate_sql(sql)
        assert is_safe is True

    def test_block_create(self):
        is_safe, _ = validate_sql("CREATE TABLE evil (id INTEGER)")
        assert is_safe is False

    def test_empty_query(self):
        is_safe, _ = validate_sql("")
        assert is_safe is False

    def test_whitespace_only(self):
        is_safe, _ = validate_sql("   \n\t  ")
        assert is_safe is False


# ── sanitize_input ───────────────────────────────────────


class TestSanitizeInput:
    """Tests for user input sanitisation."""

    def test_normal_input(self):
        result = sanitize_input("What is the total revenue for Alex Park?")
        assert result == "What is the total revenue for Alex Park?"

    def test_strips_whitespace(self):
        result = sanitize_input("  hello  ")
        assert result == "hello"

    def test_truncates_long_input(self):
        long_input = "a" * 1000
        result = sanitize_input(long_input)
        assert len(result) == 500

    def test_removes_control_characters(self):
        result = sanitize_input("hello\x00world\x07test")
        assert result == "helloworldtest"

    def test_preserves_newlines(self):
        result = sanitize_input("line 1\nline 2")
        assert "\n" in result

    def test_empty_input(self):
        assert sanitize_input("") == ""
        assert sanitize_input(None) == ""


# ── enforce_limit ────────────────────────────────────────


class TestEnforceLimit:
    """Tests for automatic LIMIT clause enforcement."""

    def test_adds_limit_when_missing(self):
        sql = "SELECT * FROM dim_writer"
        result = enforce_limit(sql, max_rows=100)
        assert "LIMIT 100" in result

    def test_preserves_existing_limit(self):
        sql = "SELECT * FROM dim_writer LIMIT 10"
        result = enforce_limit(sql, max_rows=100)
        assert "LIMIT 100" not in result
        assert "LIMIT 10" in result

    def test_preserves_existing_limit_lowercase(self):
        sql = "SELECT * FROM dim_writer limit 5"
        result = enforce_limit(sql, max_rows=100)
        assert result == sql  # unchanged

    def test_strips_trailing_semicolon(self):
        sql = "SELECT * FROM dim_writer;"
        result = enforce_limit(sql, max_rows=50)
        assert "LIMIT 50" in result
        assert ";;" not in result
