"""
Database configuration and connection utilities for ClearLine Pipeline.
"""
import psycopg2
import psycopg2.extras
from typing import Optional


class DatabaseConnection:
    """Manages PostgreSQL database connections."""

    def __init__(self, host: str = "localhost", port: int = 5432,
                 database: str = "Clearline",
                 username: str = "postgres", password: Optional[str] = None):
        """
        Initialize database connection parameters.

        Args:
            host: PostgreSQL host (default: localhost)
            port: PostgreSQL port (default: 5432)
            database: Database name (default: Clearline)
            username: PostgreSQL username (default: postgres)
            password: PostgreSQL password
        """
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self._connection = None

    def connect(self):
        """Establish database connection."""
        if self._connection is None or self._connection.closed:
            self._connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.database,
                user=self.username,
                password=self.password,
                cursor_factory=psycopg2.extras.NamedTupleCursor
            )
            self._connection.autocommit = False
        return self._connection

    def close(self):
        """Close database connection."""
        if self._connection and not self._connection.closed:
            self._connection.close()
            self._connection = None

    def __enter__(self):
        """Context manager entry."""
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


def get_default_connection():
    """Get a connection using default settings (local PostgreSQL)."""
    return DatabaseConnection(password="#Huskies2016")
