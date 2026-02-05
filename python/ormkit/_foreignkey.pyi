"""Type stubs for the Rust extension module."""

from collections.abc import Iterator
from typing import Any

class ConnectionPool:
    """A database connection pool."""

    @property
    def url(self) -> str:
        """Get the database URL (with password masked)."""
        ...

    def is_postgres(self) -> bool:
        """Check if this is a PostgreSQL connection."""
        ...

    def is_sqlite(self) -> bool:
        """Check if this is a SQLite connection."""
        ...

    async def execute(self, sql: str, params: list[Any] | None = None) -> QueryResult:
        """Execute a SQL query and return results."""
        ...

    async def execute_statement_py(self, sql: str, params: list[Any] | None = None) -> int:
        """Execute a statement that doesn't return rows. Returns rows affected."""
        ...

    async def close(self) -> None:
        """Close the connection pool."""
        ...

class QueryResult:
    """Result from executing a SQL query."""

    @property
    def columns(self) -> list[str]:
        """Get column names."""
        ...

    @property
    def rowcount(self) -> int:
        """Get the number of rows returned."""
        ...

    def all(self) -> list[dict[str, Any]]:
        """Get all rows as a list of dictionaries."""
        ...

    def first(self) -> dict[str, Any] | None:
        """Get the first row, or None if empty."""
        ...

    def one(self) -> dict[str, Any]:
        """Get a single row, raising error if not exactly one row."""
        ...

    def one_or_none(self) -> dict[str, Any] | None:
        """Get a single row or None."""
        ...

    def is_empty(self) -> bool:
        """Check if result is empty."""
        ...

    def __len__(self) -> int: ...
    def __iter__(self) -> Iterator[dict[str, Any]]: ...

async def create_pool(
    url: str,
    min_connections: int = 1,
    max_connections: int = 10,
) -> ConnectionPool:
    """Create a new database connection pool."""
    ...
