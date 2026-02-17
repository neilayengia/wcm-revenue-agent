"""
Unit tests for the deterministic result formatter.
"""

from wcm_agent.formatters import format_result_deterministic


class TestFormatResultDeterministic:
    """Tests for deterministic (non-LLM) result formatting."""

    def test_single_currency_result(self):
        data = [{"total_revenue": 4644.75}]
        result = format_result_deterministic("total revenue?", data)
        assert result == "total_revenue: $4,644.75"

    def test_single_integer_result(self):
        data = [{"count": 5}]
        result = format_result_deterministic("how many?", data)
        assert result == "count: $5.00"  # integers also get currency format

    def test_single_text_result(self):
        data = [{"writer_name": "Alex Park"}]
        result = format_result_deterministic("who?", data)
        assert result == "writer_name: Alex Park"

    def test_multi_row_result(self):
        data = [
            {"writer_name": "Alex Park", "total": 4644.75},
            {"writer_name": "Jane Miller", "total": 1799.50},
        ]
        result = format_result_deterministic("revenue by writer?", data)
        lines = result.split("\n")
        assert len(lines) == 2
        assert "Alex Park" in lines[0]
        assert "$4,644.75" in lines[0]
        assert "Jane Miller" in lines[1]

    def test_empty_result(self):
        result = format_result_deterministic("anything?", [])
        assert result == "No results found."

    def test_none_result(self):
        result = format_result_deterministic("anything?", None)
        assert result == "No results found."

    def test_multi_column_single_row(self):
        data = [{"name": "Starlight", "revenue": 1663.25}]
        result = format_result_deterministic("top song?", data)
        # Multi-column single row uses the multi-row pipe format
        assert "Starlight" in result
        assert "$1,663.25" in result
