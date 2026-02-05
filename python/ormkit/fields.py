"""Column and field definitions for ORM models."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Any, Generic, TypeVar, Union

T = TypeVar("T")


class JSON:
    """Marker class for JSON/JSONB column types.

    When used with mapped_column, indicates the column should store
    JSON data. PostgreSQL uses JSONB for efficient storage and querying,
    SQLite stores as TEXT (JSON string).

    The Rust layer handles serialization/deserialization automatically:
    - Python dict/list → JSON string (on insert/update)
    - JSON string → Python dict/list (on select)

    Example:
        >>> class Product(Base):
        ...     id: Mapped[int] = mapped_column(primary_key=True)
        ...     metadata: Mapped[dict] = mapped_column(JSON)  # Explicit JSON type
        ...     tags: Mapped[list] = mapped_column(JSON)  # Can also be list
    """

    pass


# Type alias for Mapped - indicates a database column
class Mapped(Generic[T]):
    """Type annotation wrapper indicating a database-mapped column.

    Example:
        >>> class User(Base):
        ...     id: Mapped[int] = mapped_column(primary_key=True)
        ...     name: Mapped[str] = mapped_column(max_length=100)
        ...     age: Mapped[int | None] = mapped_column(nullable=True)
    """

    pass


@dataclass
class ForeignKey:
    """Defines a foreign key reference to another table.

    Args:
        target: The target column in format "table.column"
        ondelete: Action on delete (CASCADE, SET NULL, RESTRICT, NO ACTION)
        onupdate: Action on update (CASCADE, SET NULL, RESTRICT, NO ACTION)

    Example:
        >>> class Post(Base):
        ...     author_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    """

    target: str
    ondelete: str | None = None
    onupdate: str | None = None

    @property
    def table(self) -> str:
        """Get the target table name."""
        return self.target.split(".")[0]

    @property
    def column(self) -> str:
        """Get the target column name."""
        parts = self.target.split(".")
        return parts[1] if len(parts) > 1 else "id"


@dataclass
class ColumnInfo:
    """Stores metadata about a database column."""

    name: str | None = None
    python_type: type | None = None
    primary_key: bool = False
    nullable: bool = False
    unique: bool = False
    index: bool = False
    default: Any = None
    server_default: str | None = None
    max_length: int | None = None
    foreign_key: ForeignKey | None = None
    autoincrement: bool | None = None
    is_json: bool = False  # Whether this column stores JSON data

    def sql_type(self, dialect: str = "postgresql") -> str:
        """Get the SQL type for this column."""
        # JSON columns have their own type mapping
        if self.is_json:
            return "JSONB" if dialect == "postgresql" else "TEXT"

        if self.python_type is None:
            return "TEXT"

        # Handle Optional types
        origin = getattr(self.python_type, "__origin__", None)
        if origin is Union:
            args = getattr(self.python_type, "__args__", ())
            non_none = [a for a in args if a is not type(None)]
            if non_none:
                actual_type = non_none[0]
            else:
                actual_type = str
        else:
            actual_type = self.python_type

        # Check if the type is dict or list (JSON types)
        if actual_type is dict or actual_type is list:
            return "JSONB" if dialect == "postgresql" else "TEXT"

        if dialect == "postgresql":
            return self._pg_type(actual_type)
        elif dialect == "sqlite":
            return self._sqlite_type(actual_type)
        else:
            return self._pg_type(actual_type)

    def _pg_type(self, python_type: type) -> str:
        """Get PostgreSQL type for a Python type."""
        if python_type is int:
            if self.primary_key:
                return "SERIAL" if self.autoincrement is not False else "INTEGER"
            return "INTEGER"
        elif python_type is str:
            if self.max_length:
                return f"VARCHAR({self.max_length})"
            return "TEXT"
        elif python_type is float:
            return "DOUBLE PRECISION"
        elif python_type is bool:
            return "BOOLEAN"
        elif python_type is bytes:
            return "BYTEA"
        elif python_type is datetime:
            return "TIMESTAMP"
        elif python_type is date:
            return "DATE"
        elif python_type is time:
            return "TIME"
        else:
            return "TEXT"

    def _sqlite_type(self, python_type: type) -> str:
        """Get SQLite type for a Python type."""
        if python_type is int:
            return "INTEGER"
        elif python_type is str:
            return "TEXT"
        elif python_type is float:
            return "REAL"
        elif python_type is bool:
            return "INTEGER"  # SQLite uses 0/1 for bool
        elif python_type is bytes:
            return "BLOB"
        elif python_type in (datetime, date, time):
            return "TEXT"  # SQLite stores dates as text
        else:
            return "TEXT"


def mapped_column(
    type_or_fk: type | ForeignKey | None = None,
    /,
    *,
    primary_key: bool = False,
    nullable: bool = False,
    unique: bool = False,
    index: bool = False,
    default: Any = None,
    server_default: str | None = None,
    max_length: int | None = None,
    autoincrement: bool | None = None,
) -> Any:
    """Define a database column.

    Args:
        type_or_fk: Optional ForeignKey or JSON marker for this column
        primary_key: Whether this is a primary key column
        nullable: Whether NULL values are allowed
        unique: Whether values must be unique
        index: Whether to create an index on this column
        default: Default value (can be callable)
        server_default: SQL expression for server-side default
        max_length: Maximum length for string columns
        autoincrement: Whether to auto-increment (for integer PKs)

    Returns:
        A ColumnInfo descriptor

    Example:
        >>> id: Mapped[int] = mapped_column(primary_key=True)
        >>> name: Mapped[str] = mapped_column(max_length=100, index=True)
        >>> email: Mapped[str] = mapped_column(unique=True)
        >>> author_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
        >>> metadata: Mapped[dict] = mapped_column(JSON)  # JSON column
    """
    foreign_key = None
    is_json = False

    if isinstance(type_or_fk, ForeignKey):
        foreign_key = type_or_fk
    elif type_or_fk is JSON or (isinstance(type_or_fk, type) and issubclass(type_or_fk, JSON)):
        is_json = True

    # Primary keys are not nullable by default
    if primary_key:
        nullable = False
        if autoincrement is None:
            autoincrement = True

    return ColumnInfo(
        primary_key=primary_key,
        nullable=nullable,
        unique=unique,
        index=index,
        default=default,
        server_default=server_default,
        max_length=max_length,
        foreign_key=foreign_key,
        autoincrement=autoincrement,
        is_json=is_json,
    )
