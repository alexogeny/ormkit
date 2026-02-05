"""OrmKit - A blazingly fast Python ORM powered by Rust."""

from __future__ import annotations

from ormkit._ormkit import ColumnInfo as RustColumnInfo
from ormkit._ormkit import ConnectionPool, QueryResult, create_pool
from ormkit._ormkit import ConstraintInfo as RustConstraintInfo
from ormkit._ormkit import IndexInfo as RustIndexInfo
from ormkit._ormkit import TableInfo as RustTableInfo
from ormkit.base import Base
from ormkit.fields import JSON, ForeignKey, Mapped, mapped_column
from ormkit.mixins import SoftDeleteMixin
from ormkit.query import delete, insert, select, update
from ormkit.relationships import joinedload, lazyload, noload, relationship, selectinload
from ormkit.session import AsyncSession, Q, Query, Transaction, create_session, session_context

__version__ = "0.1.0"

__all__ = [
    # Core
    "create_engine",
    "create_pool",
    "create_session",
    "session_context",
    "ConnectionPool",
    "QueryResult",
    "AsyncSession",
    "Transaction",
    "Query",
    # Model definition
    "Base",
    "Mapped",
    "mapped_column",
    "ForeignKey",
    "JSON",
    "SoftDeleteMixin",
    "relationship",
    # Query building
    "select",
    "insert",
    "update",
    "delete",
    "Q",
    # Eager loading
    "selectinload",
    "joinedload",
    "lazyload",
    "noload",
    # Schema introspection (from Rust)
    "RustColumnInfo",
    "RustIndexInfo",
    "RustConstraintInfo",
    "RustTableInfo",
]


async def create_engine(
    url: str,
    *,
    min_connections: int = 1,
    max_connections: int = 10,
) -> ConnectionPool:
    """Create a database connection pool.

    Args:
        url: Database connection URL.
            - PostgreSQL: postgresql://user:pass@host:port/dbname
            - SQLite: sqlite:///path/to/db.sqlite or sqlite::memory:
        min_connections: Minimum number of connections to maintain.
        max_connections: Maximum number of connections in the pool.

    Returns:
        A ConnectionPool instance.

    Example:
        >>> engine = await create_engine("postgresql://localhost/mydb")
        >>> engine = await create_engine("sqlite:///app.db")
    """
    return await create_pool(url, min_connections, max_connections)
