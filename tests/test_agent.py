"""
Integration tests for the Text-to-SQL agent with mocked OpenAI API.
"""

import json
from unittest.mock import patch, MagicMock

import pytest

from wcm_agent.agent import ask_database


def _mock_openai_response(content):
    """Create a mock OpenAI chat completion response."""
    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    mock_response.choices[0].message.content = content
    return mock_response


class TestAskDatabaseMocked:
    """Integration tests with mocked LLM calls."""

    @patch.dict("os.environ", {"OPENAI_API_KEY": "test-key"})
    @patch("wcm_agent.agent.OpenAI")
    def test_full_pipeline_mocked(self, mock_openai_cls, db_conn):
        """End-to-end: mocked LLM returns known SQL, verify correct result."""
        mock_client = MagicMock()
        mock_openai_cls.return_value = mock_client

        # First call: SQL generation
        sql_response = _mock_openai_response(
            "SELECT ROUND(SUM(fr.amount_usd), 2) AS total_revenue "
            "FROM fact_royalties fr "
            "JOIN current_songs cs ON fr.song_id = cs.song_id "
            "JOIN dim_writer dw ON cs.writer_id = dw.writer_id "
            "WHERE dw.writer_name = 'Alex Park'"
        )
        # Second call: answer formatting
        answer_response = _mock_openai_response(
            "The total revenue for Alex Park is $4,644.75."
        )
        mock_client.chat.completions.create.side_effect = [
            sql_response, answer_response
        ]

        result = ask_database("What is the total revenue for Alex Park?", db_conn)
        assert "4,644.75" in result

    @patch.dict("os.environ", {"OPENAI_API_KEY": "test-key"})
    @patch("wcm_agent.agent.OpenAI")
    def test_unsafe_sql_rejected(self, mock_openai_cls, db_conn):
        """LLM generates destructive SQL → should be blocked."""
        mock_client = MagicMock()
        mock_openai_cls.return_value = mock_client

        sql_response = _mock_openai_response("DROP TABLE dim_writer")
        mock_client.chat.completions.create.return_value = sql_response

        result = ask_database("Delete everything", db_conn, max_retries=0)
        assert "SAFETY ERROR" in result

    @patch.dict("os.environ", {"OPENAI_API_KEY": "test-key"})
    @patch("wcm_agent.agent.OpenAI")
    def test_fallback_formatter_on_llm_failure(self, mock_openai_cls, db_conn):
        """Second LLM call fails → should use deterministic formatter."""
        mock_client = MagicMock()
        mock_openai_cls.return_value = mock_client

        sql_response = _mock_openai_response(
            "SELECT ROUND(SUM(fr.amount_usd), 2) AS total_revenue "
            "FROM fact_royalties fr "
            "JOIN current_songs cs ON fr.song_id = cs.song_id "
            "JOIN dim_writer dw ON cs.writer_id = dw.writer_id "
            "WHERE dw.writer_name = 'Alex Park'"
        )
        mock_client.chat.completions.create.side_effect = [
            sql_response,
            Exception("API rate limit exceeded"),
        ]

        result = ask_database("What is the total revenue for Alex Park?", db_conn)
        assert "4,644.75" in result

    @patch.dict("os.environ", {"OPENAI_API_KEY": "test-key"})
    @patch("wcm_agent.agent.OpenAI")
    @patch("wcm_agent.agent.time.sleep")  # Don't actually sleep in tests
    def test_api_failure_retries(self, mock_sleep, mock_openai_cls, db_conn):
        """API fails on first attempt, succeeds on retry."""
        mock_client = MagicMock()
        mock_openai_cls.return_value = mock_client

        sql_response = _mock_openai_response(
            "SELECT COUNT(*) AS total FROM dim_writer"
        )
        answer_response = _mock_openai_response("There are 5 writers.")

        mock_client.chat.completions.create.side_effect = [
            Exception("Temporary API error"),
            sql_response,
            answer_response,
        ]

        result = ask_database("How many writers?", db_conn, max_retries=2)
        assert "5" in result
        # Verify sleep was called for exponential backoff
        mock_sleep.assert_called()

    @patch.dict("os.environ", {"OPENAI_API_KEY": "test-key"})
    @patch("wcm_agent.agent.OpenAI")
    @patch("wcm_agent.agent.time.sleep")
    def test_api_all_retries_exhausted(self, mock_sleep, mock_openai_cls, db_conn):
        """All API attempts fail → returns error message."""
        mock_client = MagicMock()
        mock_openai_cls.return_value = mock_client

        mock_client.chat.completions.create.side_effect = Exception("API down")

        result = ask_database("anything", db_conn, max_retries=2)
        assert "API ERROR" in result

    @patch.dict("os.environ", {}, clear=True)
    def test_missing_api_key(self, db_conn):
        """No API key → returns clear error message."""
        # Remove OPENAI_API_KEY if present
        import os
        os.environ.pop("OPENAI_API_KEY", None)

        result = ask_database("test", db_conn)
        assert "OPENAI_API_KEY" in result

    @patch.dict("os.environ", {"OPENAI_API_KEY": "test-key"})
    @patch("wcm_agent.agent.OpenAI")
    def test_empty_question(self, mock_openai_cls, db_conn):
        """Empty question → returns error without calling the API."""
        result = ask_database("", db_conn)
        assert "ERROR" in result

    @patch.dict("os.environ", {"OPENAI_API_KEY": "test-key"})
    @patch("wcm_agent.agent.OpenAI")
    def test_sql_with_markdown_fences_cleaned(self, mock_openai_cls, db_conn):
        """LLM wraps SQL in markdown code fences → should be cleaned."""
        mock_client = MagicMock()
        mock_openai_cls.return_value = mock_client

        sql_response = _mock_openai_response(
            "```sql\nSELECT COUNT(*) AS total FROM dim_writer\n```"
        )
        answer_response = _mock_openai_response("There are 5 writers.")

        mock_client.chat.completions.create.side_effect = [
            sql_response, answer_response
        ]

        result = ask_database("How many writers?", db_conn)
        assert "5" in result

    @patch.dict("os.environ", {"OPENAI_API_KEY": "test-key"})
    @patch("wcm_agent.agent.OpenAI")
    def test_no_results_query(self, mock_openai_cls, db_conn):
        """Query returns zero rows → returns 'No results found.'"""
        mock_client = MagicMock()
        mock_openai_cls.return_value = mock_client

        sql_response = _mock_openai_response(
            "SELECT * FROM dim_writer WHERE writer_name = 'Nobody'"
        )
        mock_client.chat.completions.create.return_value = sql_response

        result = ask_database("Find Nobody", db_conn)
        assert "No results" in result
