"""Query builder for constructing SQL statements."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    from ormkit.base import Base
    from ormkit.relationships import LoadOption

T = TypeVar("T", bound="Base")


@dataclass
class SelectStatement[T: "Base"]:
    """Represents a SELECT query."""

    model: type[T]
    _where_clauses: list[WhereClause] = field(default_factory=list)
    _order_by: list[tuple[str, str]] = field(default_factory=list)
    _limit: int | None = None
    _offset: int | None = None
    _load_options: list[LoadOption] = field(default_factory=list)

    def where(self, *conditions: WhereClause) -> SelectStatement[T]:
        """Add WHERE conditions.

        Example:
            >>> select(User).where(User.name == "Alice")
            >>> select(User).where(User.age > 18, User.active == True)
        """
        new_stmt = SelectStatement(
            model=self.model,
            _where_clauses=self._where_clauses + list(conditions),
            _order_by=self._order_by,
            _limit=self._limit,
            _offset=self._offset,
            _load_options=self._load_options,
        )
        return new_stmt

    def filter_by(self, **kwargs: Any) -> SelectStatement[T]:
        """Add WHERE conditions using keyword arguments.

        Example:
            >>> select(User).filter_by(name="Alice", active=True)
        """
        conditions = []
        for col_name, value in kwargs.items():
            conditions.append(WhereClause(col_name, "=", value))
        return self.where(*conditions)

    def order_by(self, *columns: Any, desc: bool = False) -> SelectStatement[T]:
        """Add ORDER BY clause.

        Example:
            >>> select(User).order_by(User.created_at, desc=True)
        """
        direction = "DESC" if desc else "ASC"
        new_order = [(self._get_col_name(c), direction) for c in columns]
        return SelectStatement(
            model=self.model,
            _where_clauses=self._where_clauses,
            _order_by=self._order_by + new_order,
            _limit=self._limit,
            _offset=self._offset,
            _load_options=self._load_options,
        )

    def limit(self, n: int) -> SelectStatement[T]:
        """Limit the number of results."""
        return SelectStatement(
            model=self.model,
            _where_clauses=self._where_clauses,
            _order_by=self._order_by,
            _limit=n,
            _offset=self._offset,
            _load_options=self._load_options,
        )

    def offset(self, n: int) -> SelectStatement[T]:
        """Skip the first n results."""
        return SelectStatement(
            model=self.model,
            _where_clauses=self._where_clauses,
            _order_by=self._order_by,
            _limit=self._limit,
            _offset=n,
            _load_options=self._load_options,
        )

    def options(self, *opts: LoadOption) -> SelectStatement[T]:
        """Add relationship loading options.

        Example:
            >>> select(User).options(selectinload(User.posts))
        """
        return SelectStatement(
            model=self.model,
            _where_clauses=self._where_clauses,
            _order_by=self._order_by,
            _limit=self._limit,
            _offset=self._offset,
            _load_options=self._load_options + list(opts),
        )

    def to_sql(self, dialect: str = "postgresql") -> tuple[str, list[Any]]:
        """Generate SQL string and parameters."""
        table = self.model.__tablename__
        columns = ", ".join(self.model.__columns__.keys())

        sql = f"SELECT {columns} FROM {table}"
        params: list[Any] = []

        if self._where_clauses:
            where_parts = []
            for clause in self._where_clauses:
                param_placeholder = "$" + str(len(params) + 1) if dialect == "postgresql" else "?"
                where_parts.append(f"{clause.column} {clause.operator} {param_placeholder}")
                params.append(clause.value)
            sql += " WHERE " + " AND ".join(where_parts)

        if self._order_by:
            order_parts = [f"{col} {direction}" for col, direction in self._order_by]
            sql += " ORDER BY " + ", ".join(order_parts)

        if self._limit is not None:
            sql += f" LIMIT {self._limit}"

        if self._offset is not None:
            sql += f" OFFSET {self._offset}"

        return sql, params

    def _get_col_name(self, col: Any) -> str:
        """Extract column name from various inputs."""
        if isinstance(col, str):
            return col
        if hasattr(col, "name"):
            return col.name
        return str(col)


@dataclass
class InsertStatement[T: "Base"]:
    """Represents an INSERT query with optional ON CONFLICT (upsert) support."""

    model: type[T]
    _values: list[dict[str, Any]] = field(default_factory=list)
    _returning: list[str] | None = None
    # ON CONFLICT fields for upsert
    _conflict_target: str | list[str] | None = None
    _conflict_action: str | None = None  # "update" or "nothing"
    _conflict_update_cols: dict[str, Any] | None = None

    def values(self, *rows: dict[str, Any], **single_row: Any) -> InsertStatement[T]:
        """Specify values to insert.

        Example:
            >>> insert(User).values(name="Alice", email="alice@example.com")
            >>> insert(User).values({"name": "Alice"}, {"name": "Bob"})
        """
        new_values = list(self._values)
        if rows:
            new_values.extend(rows)
        if single_row:
            new_values.append(single_row)
        return InsertStatement(
            model=self.model,
            _values=new_values,
            _returning=self._returning,
            _conflict_target=self._conflict_target,
            _conflict_action=self._conflict_action,
            _conflict_update_cols=self._conflict_update_cols,
        )

    def returning(self, *columns: str) -> InsertStatement[T]:
        """Return specified columns after insert (PostgreSQL)."""
        return InsertStatement(
            model=self.model,
            _values=self._values,
            _returning=list(columns) if columns else ["*"],
            _conflict_target=self._conflict_target,
            _conflict_action=self._conflict_action,
            _conflict_update_cols=self._conflict_update_cols,
        )

    def on_conflict_do_update(
        self,
        target: str | list[str],
        set_: dict[str, Any] | None = None,
    ) -> InsertStatement[T]:
        """Add ON CONFLICT ... DO UPDATE clause for upsert behavior.

        Args:
            target: Column(s) that define the conflict (unique constraint).
                   Can be a single column name or list of column names.
            set_: Columns to update on conflict. If None, updates all non-PK columns
                  from the inserted values using EXCLUDED reference.

        Example:
            >>> # Update specific columns on conflict
            >>> insert(User).values(email="a@b.com", name="A") \\
            ...     .on_conflict_do_update("email", set_={"name": "Updated"})
            >>>
            >>> # Update all provided columns on conflict
            >>> insert(User).values(email="a@b.com", name="A") \\
            ...     .on_conflict_do_update("email")
            >>>
            >>> # Composite unique key
            >>> insert(TeamMember).values(team_id=1, user_id=1, role="admin") \\
            ...     .on_conflict_do_update(["team_id", "user_id"], set_={"role": "admin"})
        """
        return InsertStatement(
            model=self.model,
            _values=self._values,
            _returning=self._returning,
            _conflict_target=target,
            _conflict_action="update",
            _conflict_update_cols=set_,
        )

    def on_conflict_do_nothing(
        self,
        target: str | list[str] | None = None,
    ) -> InsertStatement[T]:
        """Add ON CONFLICT ... DO NOTHING clause to ignore conflicts.

        Args:
            target: Column(s) that define the conflict. If None, any conflict triggers DO NOTHING.

        Example:
            >>> insert(User).values(email="a@b.com", name="A") \\
            ...     .on_conflict_do_nothing("email")
        """
        return InsertStatement(
            model=self.model,
            _values=self._values,
            _returning=self._returning,
            _conflict_target=target,
            _conflict_action="nothing",
            _conflict_update_cols=None,
        )

    def to_sql(self, dialect: str = "postgresql") -> tuple[str, list[Any]]:
        """Generate SQL string and parameters."""
        if not self._values:
            raise ValueError("No values specified for INSERT")

        table = self.model.__tablename__
        columns = list(self._values[0].keys())
        params: list[Any] = []

        if len(self._values) == 1:
            # Single row insert
            placeholders = []
            for i, col in enumerate(columns):
                if dialect == "postgresql":
                    placeholders.append(f"${i + 1}")
                else:
                    placeholders.append("?")
                params.append(self._values[0][col])

            sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
        else:
            # Multi-row insert
            value_groups = []
            param_idx = 1
            for row in self._values:
                placeholders = []
                for col in columns:
                    if dialect == "postgresql":
                        placeholders.append(f"${param_idx}")
                    else:
                        placeholders.append("?")
                    params.append(row.get(col))
                    param_idx += 1
                value_groups.append(f"({', '.join(placeholders)})")

            sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES {', '.join(value_groups)}"

        # Add ON CONFLICT clause for upsert
        if self._conflict_action:
            sql += self._build_conflict_clause(columns, dialect)

        if self._returning and dialect == "postgresql":
            sql += f" RETURNING {', '.join(self._returning)}"

        return sql, params

    def _build_conflict_clause(self, insert_columns: list[str], dialect: str) -> str:
        """Build the ON CONFLICT clause."""
        # Build conflict target
        if self._conflict_target:
            if isinstance(self._conflict_target, list):
                target_str = ", ".join(self._conflict_target)
            else:
                target_str = self._conflict_target
            conflict_part = f" ON CONFLICT ({target_str})"
        else:
            conflict_part = " ON CONFLICT"

        if self._conflict_action == "nothing":
            return f"{conflict_part} DO NOTHING"

        # DO UPDATE
        if self._conflict_update_cols:
            # Specific columns to update
            update_cols = list(self._conflict_update_cols.keys())
        else:
            # Update all non-PK columns from inserted values
            pk = self.model.__primary_key__
            update_cols = [c for c in insert_columns if c != pk]

        # Build SET clause using EXCLUDED reference
        # PostgreSQL and SQLite both use "excluded" (SQLite is case-insensitive)
        excluded_prefix = "EXCLUDED" if dialect == "postgresql" else "excluded"
        set_parts = [f"{col} = {excluded_prefix}.{col}" for col in update_cols]

        return f"{conflict_part} DO UPDATE SET {', '.join(set_parts)}"


@dataclass
class UpdateStatement[T: "Base"]:
    """Represents an UPDATE query."""

    model: type[T]
    _set_values: dict[str, Any] = field(default_factory=dict)
    _where_clauses: list[WhereClause] = field(default_factory=list)

    def values(self, **kwargs: Any) -> UpdateStatement[T]:
        """Specify values to update.

        Example:
            >>> update(User).values(name="Alice").where(User.id == 1)
        """
        return UpdateStatement(
            model=self.model,
            _set_values={**self._set_values, **kwargs},
            _where_clauses=self._where_clauses,
        )

    def where(self, *conditions: WhereClause) -> UpdateStatement[T]:
        """Add WHERE conditions."""
        return UpdateStatement(
            model=self.model,
            _set_values=self._set_values,
            _where_clauses=self._where_clauses + list(conditions),
        )

    def to_sql(self, dialect: str = "postgresql") -> tuple[str, list[Any]]:
        """Generate SQL string and parameters."""
        table = self.model.__tablename__
        params: list[Any] = []

        set_parts = []
        for col, value in self._set_values.items():
            if dialect == "postgresql":
                set_parts.append(f"{col} = ${len(params) + 1}")
            else:
                set_parts.append(f"{col} = ?")
            params.append(value)

        sql = f"UPDATE {table} SET {', '.join(set_parts)}"

        if self._where_clauses:
            where_parts = []
            for clause in self._where_clauses:
                if dialect == "postgresql":
                    where_parts.append(f"{clause.column} {clause.operator} ${len(params) + 1}")
                else:
                    where_parts.append(f"{clause.column} {clause.operator} ?")
                params.append(clause.value)
            sql += " WHERE " + " AND ".join(where_parts)

        return sql, params


@dataclass
class DeleteStatement[T: "Base"]:
    """Represents a DELETE query."""

    model: type[T]
    _where_clauses: list[WhereClause] = field(default_factory=list)

    def where(self, *conditions: WhereClause) -> DeleteStatement[T]:
        """Add WHERE conditions."""
        return DeleteStatement(
            model=self.model,
            _where_clauses=self._where_clauses + list(conditions),
        )

    def to_sql(self, dialect: str = "postgresql") -> tuple[str, list[Any]]:
        """Generate SQL string and parameters."""
        table = self.model.__tablename__
        params: list[Any] = []

        sql = f"DELETE FROM {table}"

        if self._where_clauses:
            where_parts = []
            for clause in self._where_clauses:
                if dialect == "postgresql":
                    where_parts.append(f"{clause.column} {clause.operator} ${len(params) + 1}")
                else:
                    where_parts.append(f"{clause.column} {clause.operator} ?")
                params.append(clause.value)
            sql += " WHERE " + " AND ".join(where_parts)

        return sql, params


@dataclass
class WhereClause:
    """Represents a WHERE condition."""

    column: str
    operator: str
    value: Any


def select[T: "Base"](model: type[T]) -> SelectStatement[T]:
    """Create a SELECT statement for a model.

    Example:
        >>> stmt = select(User).where(User.name == "Alice")
        >>> result = await session.execute(stmt)
    """
    return SelectStatement(model=model)


def insert[T: "Base"](model: type[T]) -> InsertStatement[T]:
    """Create an INSERT statement for a model.

    Example:
        >>> stmt = insert(User).values(name="Alice", email="alice@example.com")
        >>> await session.execute(stmt)
    """
    return InsertStatement(model=model)


def update[T: "Base"](model: type[T]) -> UpdateStatement[T]:
    """Create an UPDATE statement for a model.

    Example:
        >>> stmt = update(User).values(name="Bob").where(User.id == 1)
        >>> await session.execute(stmt)
    """
    return UpdateStatement(model=model)


def delete[T: "Base"](model: type[T]) -> DeleteStatement[T]:
    """Create a DELETE statement for a model.

    Example:
        >>> stmt = delete(User).where(User.id == 1)
        >>> await session.execute(stmt)
    """
    return DeleteStatement(model=model)
