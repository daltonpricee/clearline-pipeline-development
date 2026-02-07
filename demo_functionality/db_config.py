"""
Database configuration and connection utilities for ClearLine Pipeline.
"""
import pyodbc
from typing import Optional

class DatabaseConnection:
    """Manages SQL Server database connections."""

    def __init__(self, server: str = r".\SQLEXPRESS", database: str = "ClearLinePipeline",
                 use_windows_auth: bool = True, username: Optional[str] = None,
                 password: Optional[str] = None):
        """
        Initialize database connection parameters.

        Args:
            server: SQL Server instance (default: .\\SQLEXPRESS for local SQL Server Express)
            database: Database name (default: ClearLinePipeline)
            use_windows_auth: Use Windows Authentication (default: True)
            username: SQL Server username (if not using Windows Auth)
            password: SQL Server password (if not using Windows Auth)
        """
        self.server = server
        self.database = database
        self.use_windows_auth = use_windows_auth
        self.username = username
        self.password = password
        self._connection = None

    def get_connection_string(self) -> str:
        """Build the connection string for SQL Server."""
        if self.use_windows_auth:
            return (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"Trusted_Connection=yes;"
            )
        else:
            return (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"UID={self.username};"
                f"PWD={self.password};"
            )

    def connect(self):
        """Establish database connection."""
        if self._connection is None:
            conn_str = self.get_connection_string()
            self._connection = pyodbc.connect(conn_str)
        return self._connection

    def close(self):
        """Close database connection."""
        if self._connection:
            self._connection.close()
            self._connection = None

    def __enter__(self):
        """Context manager entry."""
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


def get_default_connection():
    """Get a connection using default settings (Windows Auth to local SQL Express)."""
    return DatabaseConnection()
