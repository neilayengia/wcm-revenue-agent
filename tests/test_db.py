"""
Unit tests for database initialisation and the current_songs view.
"""

import pytest


class TestDatabaseInit:
    """Tests for database setup and CSV loading."""

    def test_tables_exist(self, db_conn):
        """All three tables should be created."""
        tables = db_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        table_names = [t[0] for t in tables]
        assert "dim_writer" in table_names
        assert "dim_song" in table_names
        assert "fact_royalties" in table_names

    def test_dim_writer_row_count(self, db_conn):
        count = db_conn.execute("SELECT COUNT(*) FROM dim_writer").fetchone()[0]
        assert count == 5

    def test_dim_song_row_count(self, db_conn):
        """dim_song has 22 rows (20 unique songs + 2 historical duplicates)."""
        count = db_conn.execute("SELECT COUNT(*) FROM dim_song").fetchone()[0]
        assert count == 22

    def test_fact_royalties_row_count(self, db_conn):
        count = db_conn.execute("SELECT COUNT(*) FROM fact_royalties").fetchone()[0]
        assert count == 100


class TestCurrentSongsView:
    """Tests for the deduplication view."""

    def test_view_exists(self, db_conn):
        views = db_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='view'"
        ).fetchall()
        view_names = [v[0] for v in views]
        assert "current_songs" in view_names

    def test_deduplication_unique_song_ids(self, db_conn):
        """View should return exactly 20 unique songs (no duplicates)."""
        count = db_conn.execute("SELECT COUNT(*) FROM current_songs").fetchone()[0]
        assert count == 20

    def test_song_1_has_latest_title(self, db_conn):
        """Song 1 should be 'Starlight' (not 'Starlight (Draft)')."""
        row = db_conn.execute(
            "SELECT title FROM current_songs WHERE song_id = 1"
        ).fetchone()
        assert row[0] == "Starlight"

    def test_song_6_has_latest_title(self, db_conn):
        """Song 6 should be 'Static Dreams' (not 'Static Dreams (Original)')."""
        row = db_conn.execute(
            "SELECT title FROM current_songs WHERE song_id = 6"
        ).fetchone()
        assert row[0] == "Static Dreams"

    def test_alex_park_revenue_correct(self, db_conn):
        """Core verification: Alex Park's revenue via the view = $4,644.75."""
        result = db_conn.execute("""
            SELECT ROUND(SUM(fr.amount_usd), 2) AS total_revenue
            FROM fact_royalties fr
            JOIN current_songs cs ON fr.song_id = cs.song_id
            JOIN dim_writer dw ON cs.writer_id = dw.writer_id
            WHERE dw.writer_name = 'Alex Park'
        """).fetchone()
        assert result[0] == 4644.75

    def test_alex_park_revenue_naive_is_wrong(self, db_conn):
        """
        Verify that joining through dim_song directly gives the WRONG
        answer ($6,308.00) — proving the view is necessary.
        """
        result = db_conn.execute("""
            SELECT ROUND(SUM(fr.amount_usd), 2) AS total_revenue
            FROM fact_royalties fr
            JOIN dim_song ds ON fr.song_id = ds.song_id
            JOIN dim_writer dw ON ds.writer_id = dw.writer_id
            WHERE dw.writer_name = 'Alex Park'
        """).fetchone()
        # This should NOT equal the correct answer
        assert result[0] != 4644.75
        # It should be higher due to double-counting
        assert result[0] > 4644.75

    def test_all_writers_have_songs(self, db_conn):
        """Every writer in dim_writer should have at least one song in the view."""
        result = db_conn.execute("""
            SELECT dw.writer_name
            FROM dim_writer dw
            LEFT JOIN current_songs cs ON dw.writer_id = cs.writer_id
            GROUP BY dw.writer_id
            HAVING COUNT(cs.song_id) = 0
        """).fetchall()
        assert len(result) == 0, "Some writers have no songs in current_songs"
