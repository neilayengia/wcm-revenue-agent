"""
Shared test fixtures for the WCM Revenue Agent test suite.
"""

import pytest

from wcm_agent.db import init_database, create_current_songs_view


@pytest.fixture
def db_conn():
    """
    Create a fully initialised in-memory database with the
    current_songs deduplication view.

    Yields the connection and closes it after the test.
    """
    conn = init_database()
    create_current_songs_view(conn)
    yield conn
    conn.close()
