"""Async session for database operations with Unit of Work pattern."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC
from typing import TYPE_CHECKING, Any, TypeVar

from ormkit._ormkit import ConnectionPool, QueryResult

if TYPE_CHECKING:
    from ormkit.base import Base
    from ormkit.query import DeleteStatement, InsertStatement, SelectStatement, UpdateStatement

T = TypeVar("T", bound="Base")


# ========== Q Objects for Complex Conditions ==========

@dataclass
class Q:
    """Django-style Q object for complex query conditions.

    Supports AND (&) and OR (|) operations for building complex WHERE clauses.

    Example:
        >>> # OR condition
        >>> query.filter(Q(age__gt=18) | Q(vip=True))

        >>> # AND condition (explicit)
        >>> query.filter(Q(age__gt=18) & Q(active=True))

        >>> # Combined
        >>> query.filter((Q(age__gt=18) | Q(vip=True)) & Q(active=True))

        >>> # Negation
        >>> query.filter(~Q(banned=True))
    """

    _filters: list[tuple[str, str, Any]] = field(default_factory=list)
    _children: list[tuple[str, Q]] = field(default_factory=list)  # ("AND"/"OR", child_q)
    _negated: bool = False

    def __init__(self, **kwargs: Any) -> None:
        self._filters = []
        self._children = []
        self._negated = False

        for key, value in kwargs.items():
            col, op = _parse_filter_key(key)
            self._filters.append((col, op, value))

    def __or__(self, other: Q) -> Q:
        """Combine with OR."""
        result = Q()
        result._children = [("OR", self), ("OR", other)]
        return result

    def __and__(self, other: Q) -> Q:
        """Combine with AND."""
        result = Q()
        result._children = [("AND", self), ("AND", other)]
        return result

    def __invert__(self) -> Q:
        """Negate the condition."""
        result = Q()
        result._filters = self._filters.copy()
        result._children = self._children.copy()
        result._negated = not self._negated
        return result

    def to_sql(self, dialect: str, param_offset: int = 0) -> tuple[str, list[Any]]:
        """Convert to SQL WHERE clause fragment."""
        params: list[Any] = []

        if self._children:
            # Complex expression with children
            parts = []
            for _join_type, child in self._children:
                child_sql, child_params = child.to_sql(dialect, param_offset + len(params))
                if child_sql:
                    parts.append(child_sql)
                    params.extend(child_params)

            if not parts:
                return "", []

            # Determine connector
            connector = " OR " if self._children[0][0] == "OR" else " AND "
            sql = f"({connector.join(parts)})"

        elif self._filters:
            # Simple filter expression
            filter_parts = []
            for col, op, value in self._filters:
                sql_part, filter_params = _build_filter_sql(col, op, value, dialect, param_offset + len(params))
                filter_parts.append(sql_part)
                params.extend(filter_params)

            sql = " AND ".join(filter_parts)
            if len(filter_parts) > 1:
                sql = f"({sql})"
        else:
            return "", []

        if self._negated:
            sql = f"NOT {sql}"

        return sql, params


@dataclass
class JoinInfo:
    """Information about a JOIN clause for eager loading."""

    rel_name: str  # Relationship attribute name
    target_model: type  # The model being joined
    join_type: str  # "LEFT" or "INNER"
    local_col: str  # Column on the main table (FK or PK)
    remote_col: str  # Column on the joined table (PK or FK)
    alias: str  # Table alias for the joined table


def _parse_filter_key(key: str) -> tuple[str, str]:
    """Parse Django-style filter key into column and operator.

    Supports:
    - Standard operators: field__gt, field__in, etc.
    - JSON path operators: metadata__key, metadata__key__subkey
    - JSON special operators: metadata__has_key, metadata__contains
    """
    operators = {
        "gt": ">",
        "gte": ">=",
        "lt": "<",
        "lte": "<=",
        "ne": "!=",
        "like": "LIKE",
        "ilike": "ILIKE",
        "in": "IN",
        "notin": "NOT IN",
        "isnull": "IS NULL",
        "isnotnull": "IS NOT NULL",
        "contains": "LIKE",  # Will add % wildcards (or JSON contains for JSON fields)
        "icontains": "ILIKE",  # Will add % wildcards
        "startswith": "LIKE",
        "istartswith": "ILIKE",
        "endswith": "LIKE",
        "iendswith": "ILIKE",
        # JSON-specific operators
        "has_key": "JSON_HAS_KEY",
        "json_contains": "JSON_CONTAINS",  # PostgreSQL @> operator
    }

    if "__" in key:
        parts = key.rsplit("__", 1)
        if len(parts) == 2 and parts[1] in operators:
            return parts[0], parts[1]  # Return the operator NAME, not SQL

    return key, "eq"


def _build_json_path_sql(col: str, path: list[str], dialect: str) -> str:
    """Build SQL for JSON path access.

    PostgreSQL: col->'key1'->'key2'->>'key3' (->>' for final text extraction)
    SQLite: json_extract(col, '$.key1.key2.key3')
    """
    if dialect == "postgresql":
        # Use -> for intermediate keys, ->> for final key (text extraction)
        result = col
        for i, key in enumerate(path):
            # Use ->> for the last key to extract as text for comparison
            arrow = "->>" if i == len(path) - 1 else "->"
            result = f"{result}{arrow}'{key}'"
        return result
    else:
        # SQLite uses json_extract
        json_path = "$." + ".".join(path)
        return f"json_extract({col}, '{json_path}')"


def _build_filter_sql(col: str, op: str, value: Any, dialect: str, param_offset: int) -> tuple[str, list[Any]]:
    """Build SQL for a single filter condition.

    Supports:
    - Standard filters: col__gt=value
    - JSON path access: metadata__key__subkey=value
    - JSON operators: metadata__has_key="key", metadata__json_contains={"key": "value"}
    """

    def placeholder(offset: int = 0) -> str:
        return f"${param_offset + offset + 1}" if dialect == "postgresql" else "?"

    # Check if this is a JSON path access (column__key__subkey)
    # We detect this by checking if col contains __ (indicating nested path)
    json_path = None
    if "__" in col:
        parts = col.split("__")
        col = parts[0]  # Base column name
        json_path = parts[1:]  # Path into JSON

    # Handle JSON-specific operators first
    if op == "has_key":
        if dialect == "postgresql":
            return f"{col} ? {placeholder()}", [value]
        else:
            # SQLite: check if json_extract returns non-null
            return f"json_extract({col}, '$.{value}') IS NOT NULL", []

    elif op == "json_contains":
        if dialect == "postgresql":
            # PostgreSQL @> containment operator
            # Value should be a dict that we serialize to JSON
            import json
            return f"{col} @> {placeholder()}::jsonb", [json.dumps(value)]
        else:
            # SQLite doesn't have a direct containment operator
            # We'd need to check each key-value pair individually
            # For now, return a best-effort check
            import json
            return f"json({col}) = json({placeholder()})", [json.dumps(value)]

    # If we have a JSON path, modify the column reference
    col_ref = _build_json_path_sql(col, json_path, dialect) if json_path else col

    if op == "eq":
        if value is None:
            return f"{col_ref} IS NULL", []
        return f"{col_ref} = {placeholder()}", [value]

    elif op == "in":
        if not value:
            return "1 = 0", []  # Empty IN -> always false
        placeholders = ", ".join(placeholder(i) for i in range(len(value)))
        return f"{col_ref} IN ({placeholders})", list(value)

    elif op == "notin":
        if not value:
            return "1 = 1", []  # Empty NOT IN -> always true
        placeholders = ", ".join(placeholder(i) for i in range(len(value)))
        return f"{col_ref} NOT IN ({placeholders})", list(value)

    elif op == "isnull":
        return f"{col_ref} IS NULL" if value else f"{col_ref} IS NOT NULL", []

    elif op == "isnotnull":
        return f"{col_ref} IS NOT NULL" if value else f"{col_ref} IS NULL", []

    elif op == "contains":
        # For JSON arrays, check if element is in array
        if json_path:
            if dialect == "postgresql":
                # PostgreSQL: Check if JSON array contains element
                return f"{col_ref} @> {placeholder()}", [value]
            else:
                # SQLite: Use json_each to check array containment
                # This is a subquery check
                json_path_str = "$." + ".".join(json_path)
                return (
                    f"EXISTS (SELECT 1 FROM json_each({col}, '{json_path_str}') WHERE value = {placeholder()})",
                    [value]
                )
        else:
            # Regular string contains
            return f"{col_ref} LIKE {placeholder()}", [f"%{value}%"]

    elif op == "icontains":
        op_sql = "ILIKE" if dialect == "postgresql" else "LIKE"
        return f"{col_ref} {op_sql} {placeholder()}", [f"%{value}%"]

    elif op == "startswith":
        return f"{col_ref} LIKE {placeholder()}", [f"{value}%"]

    elif op == "istartswith":
        op_sql = "ILIKE" if dialect == "postgresql" else "LIKE"
        return f"{col_ref} {op_sql} {placeholder()}", [f"{value}%"]

    elif op == "endswith":
        return f"{col_ref} LIKE {placeholder()}", [f"%{value}"]

    elif op == "iendswith":
        op_sql = "ILIKE" if dialect == "postgresql" else "LIKE"
        return f"{col_ref} {op_sql} {placeholder()}", [f"%{value}"]

    else:
        # Standard comparison operators
        op_sql = {
            "gt": ">",
            "gte": ">=",
            "lt": "<",
            "lte": "<=",
            "ne": "!=",
            "like": "LIKE",
            "ilike": "ILIKE" if dialect == "postgresql" else "LIKE",
        }.get(op, "=")
        return f"{col_ref} {op_sql} {placeholder()}", [value]


class AsyncSession:
    """Async database session with Unit of Work pattern.

    Supports multiple usage patterns from simple to advanced:

    Simple (auto-commit):
        >>> async with session.begin() as tx:
        ...     tx.add(User(name="Alice"))
        ...     # auto-commits on exit, auto-rollback on exception

    Fluent API:
        >>> user = await session.insert(User(name="Alice"))
        >>> users = await session.query(User).filter(name="Alice").all()
        >>> await session.update(user, name="Bob")
        >>> await session.remove(user)

    Traditional:
        >>> session.add(user)
        >>> await session.commit()
    """

    def __init__(self, pool: ConnectionPool, *, autoflush: bool = True) -> None:
        self._pool = pool
        self._pending_new: list[Base] = []
        self._pending_dirty: list[Base] = []
        self._pending_delete: list[Base] = []
        self._identity_map: dict[tuple[type, Any], Base] = {}
        self._autoflush = autoflush
        self._dialect = "postgresql" if pool.is_postgres() else "sqlite"

    async def __aenter__(self) -> AsyncSession:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        if exc_type is not None:
            await self.rollback()

    # ========== Transaction Context Managers ==========

    @asynccontextmanager
    async def begin(self) -> AsyncIterator[Transaction]:
        """Begin a transaction that auto-commits on success.

        Example:
            >>> async with session.begin() as tx:
            ...     tx.add(User(name="Alice"))
            ...     tx.add(Post(title="Hello"))
            ...     # commits automatically on exit
        """
        tx = Transaction(self)
        try:
            yield tx
            await self.commit()
        except Exception:
            await self.rollback()
            raise

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncSession]:
        """Alias for begin() - transaction context with auto-commit.

        Example:
            >>> async with session.transaction():
            ...     session.add(user)
            ...     # commits automatically
        """
        try:
            yield self
            await self.commit()
        except Exception:
            await self.rollback()
            raise

    # ========== Fluent/Simple API ==========

    async def insert(self, instance: T) -> T:
        """Insert a model and return it with generated ID.

        Example:
            >>> user = await session.insert(User(name="Alice"))
            >>> print(user.id)  # Has the generated ID
        """
        self.add(instance)
        await self.commit()
        # Attach session for M2M operations
        instance._session = self  # type: ignore[attr-defined]
        return instance

    async def insert_all(self, instances: list[T]) -> list[T]:
        """Insert multiple models and return them with generated IDs.

        Example:
            >>> users = await session.insert_all([
            ...     User(name="Alice"),
            ...     User(name="Bob"),
            ... ])
        """
        self.add_all(instances)
        await self.commit()
        # Attach session for M2M operations
        for instance in instances:
            instance._session = self  # type: ignore[attr-defined]
        return instances

    async def get(
        self, model: type[T], id: Any, *, include_deleted: bool = False
    ) -> T | None:
        """Get a model by primary key.

        For models with SoftDeleteMixin, soft-deleted records are excluded
        by default. Use include_deleted=True to include them.

        Example:
            >>> user = await session.get(User, 1)
            >>> # Include soft-deleted
            >>> article = await session.get(Article, 1, include_deleted=True)
        """
        # Check identity map first
        key = (model, id)
        if key in self._identity_map:
            instance = self._identity_map[key]
            # Check soft delete status
            if (
                not include_deleted
                and getattr(model, "__soft_delete__", False)
                and getattr(instance, "deleted_at", None) is not None
            ):
                return None
            return instance  # type: ignore

        pk = model.__primary_key__
        if pk is None:
            raise ValueError(f"{model.__name__} has no primary key")

        # Use Query to respect soft delete filtering
        query = self.query(model).filter(**{pk: id})
        if include_deleted:
            query = query.with_deleted()

        instance = await query.first()

        if instance is not None:
            self._identity_map[key] = instance

        return instance

    async def get_or_raise(self, model: type[T], id: Any) -> T:
        """Get a model by primary key, raise if not found.

        Example:
            >>> user = await session.get_or_raise(User, 1)
        """
        instance = await self.get(model, id)
        if instance is None:
            raise LookupError(f"{model.__name__} with id={id} not found")
        return instance

    def query(self, model: type[T]) -> Query[T]:
        """Create a fluent query for a model.

        Example:
            >>> users = await session.query(User).filter(age__gt=18).all()
            >>> user = await session.query(User).filter(email="alice@example.com").first()
        """
        return Query(self, model)

    async def update(self, instance: T, **values: Any) -> T:
        """Update a model instance with new values.

        Example:
            >>> user = await session.update(user, name="Bob", age=30)
        """
        for key, value in values.items():
            setattr(instance, key, value)

        cls = type(instance)
        pk_col = cls.__primary_key__
        if pk_col is None:
            raise ValueError(f"Cannot update {cls.__name__}: no primary key")

        pk_value = getattr(instance, pk_col)
        table = cls.__tablename__

        set_parts = []
        params: list[Any] = []
        for key, value in values.items():
            if self._dialect == "postgresql":
                set_parts.append(f"{key} = ${len(params) + 1}")
            else:
                set_parts.append(f"{key} = ?")
            params.append(value)

        if self._dialect == "postgresql":
            params.append(pk_value)
            sql = f"UPDATE {table} SET {', '.join(set_parts)} WHERE {pk_col} = ${len(params)}"
        else:
            params.append(pk_value)
            sql = f"UPDATE {table} SET {', '.join(set_parts)} WHERE {pk_col} = ?"

        await self._pool.execute_statement_py(sql, params)
        return instance

    async def bulk_update(
        self,
        model: type[T],
        values: dict[str, Any],
        *conditions: Q,
        **filters: Any,
    ) -> int:
        """Bulk update rows matching conditions.

        Example:
            >>> # Update all users over 18 to be active
            >>> count = await session.bulk_update(User, {"active": True}, age__gt=18)

            >>> # With Q objects for complex conditions
            >>> count = await session.bulk_update(
            ...     User,
            ...     {"status": "archived"},
            ...     Q(last_login__lt=cutoff_date) | Q(deleted=True)
            ... )
        """
        table = model.__tablename__

        set_parts = []
        params: list[Any] = []
        for key, value in values.items():
            if self._dialect == "postgresql":
                set_parts.append(f"{key} = ${len(params) + 1}")
            else:
                set_parts.append(f"{key} = ?")
            params.append(value)

        sql = f"UPDATE {table} SET {', '.join(set_parts)}"

        # Build WHERE clause
        where_parts = []

        # Handle Q objects
        for q in conditions:
            q_sql, q_params = q.to_sql(self._dialect, len(params))
            if q_sql:
                where_parts.append(q_sql)
                params.extend(q_params)

        # Handle keyword filters
        for key, value in filters.items():
            col, op = _parse_filter_key(key)
            filter_sql, filter_params = _build_filter_sql(col, op, value, self._dialect, len(params))
            where_parts.append(filter_sql)
            params.extend(filter_params)

        if where_parts:
            sql += " WHERE " + " AND ".join(where_parts)

        return await self._pool.execute_statement_py(sql, params)

    async def remove(self, instance: T) -> None:
        """Delete a model instance immediately.

        Example:
            >>> await session.remove(user)
        """
        self.delete(instance)
        await self.commit()

    async def remove_all(self, instances: list[T]) -> None:
        """Delete multiple model instances immediately.

        Example:
            >>> await session.remove_all(inactive_users)
        """
        for instance in instances:
            self.delete(instance)
        await self.commit()

    # ========== Soft Delete API ==========

    async def soft_delete(self, instance: T) -> T:
        """Soft delete a model instance (sets deleted_at timestamp).

        Only works on models that use SoftDeleteMixin.

        Example:
            >>> await session.soft_delete(article)
            >>> assert article.is_deleted
        """
        if not getattr(type(instance), "__soft_delete__", False):
            raise TypeError(
                f"{type(instance).__name__} doesn't support soft delete. "
                "Add SoftDeleteMixin to enable soft delete."
            )

        from datetime import datetime

        deleted_at = datetime.now(UTC)
        instance.deleted_at = deleted_at  # type: ignore[attr-defined]
        await self.update(instance, deleted_at=deleted_at)
        return instance

    async def restore(self, instance: T) -> T:
        """Restore a soft-deleted model instance.

        Example:
            >>> await session.restore(deleted_article)
            >>> assert not article.is_deleted
        """
        if not getattr(type(instance), "__soft_delete__", False):
            raise TypeError(
                f"{type(instance).__name__} doesn't support soft delete. "
                "Add SoftDeleteMixin to enable soft delete."
            )

        instance.deleted_at = None  # type: ignore[attr-defined]
        await self.update(instance, deleted_at=None)
        return instance

    async def force_delete(self, instance: T) -> None:
        """Permanently delete a model instance (bypass soft delete).

        This removes the record from the database entirely,
        even if the model uses SoftDeleteMixin.

        Example:
            >>> await session.force_delete(article)
        """
        await self.remove(instance)

    # ========== Upsert API ==========

    async def upsert(
        self,
        instance: T,
        conflict_target: str | list[str],
        update_fields: list[str] | None = None,
        do_nothing: bool = False,
    ) -> T:
        """Insert or update a single model instance (upsert).

        If a record with the same conflict_target value(s) exists, it will be
        updated. Otherwise, a new record will be inserted.

        Args:
            instance: The model instance to upsert
            conflict_target: Column(s) that define uniqueness (e.g., "email" or ["team_id", "user_id"])
            update_fields: Columns to update on conflict. If None, updates all non-PK columns.
            do_nothing: If True, ignore conflicts instead of updating

        Returns:
            The upserted instance with generated ID

        Example:
            >>> user = await session.upsert(
            ...     User(email="alice@example.com", name="Alice"),
            ...     conflict_target="email",
            ...     update_fields=["name"]
            ... )
        """
        cls = type(instance)
        table = cls.__tablename__
        pk_col = cls.__primary_key__

        # Get columns to insert (exclude autoincrement PK)
        insert_cols = []
        for col_name, col_info in cls.__columns__.items():
            if col_info.primary_key and col_info.autoincrement:
                continue
            insert_cols.append(col_name)

        # Build values dict - get instance attributes, skipping ColumnInfo class attrs
        from ormkit.fields import ColumnInfo

        values: dict[str, Any] = {}
        for col in insert_cols:
            try:
                val = object.__getattribute__(instance, col)
                # Skip if it's still the ColumnInfo class attribute
                if not isinstance(val, ColumnInfo):
                    values[col] = val
            except AttributeError:
                pass

        # Determine update columns
        if do_nothing:
            update_cols = None
        elif update_fields:
            update_cols = update_fields
        else:
            # Update all non-PK columns
            update_cols = [c for c in insert_cols if c != pk_col]

        # Build SQL
        col_str = ", ".join(values.keys())
        params = list(values.values())

        if self._dialect == "postgresql":
            placeholders = ", ".join(f"${i+1}" for i in range(len(params)))
        else:
            placeholders = ", ".join("?" for _ in params)

        sql = f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})"

        # Add ON CONFLICT clause
        conflict_cols = [conflict_target] if isinstance(conflict_target, str) else conflict_target
        conflict_str = ", ".join(conflict_cols)

        if do_nothing:
            sql += f" ON CONFLICT ({conflict_str}) DO NOTHING"
        else:
            excluded_prefix = "EXCLUDED" if self._dialect == "postgresql" else "excluded"
            if update_cols:
                set_parts = [f"{col} = {excluded_prefix}.{col}" for col in update_cols]
            else:
                set_parts = [f"{col} = {excluded_prefix}.{col}" for col in values if col != pk_col]
            sql += f" ON CONFLICT ({conflict_str}) DO UPDATE SET {', '.join(set_parts)}"

        # Add RETURNING for PostgreSQL
        if pk_col and self._dialect == "postgresql":
            sql += f" RETURNING *"
            result = await self._pool.execute(sql, params)
            row = result.first()
            if row:
                # Update all columns from the returned row
                for col in cls.__columns__:
                    if col in row:
                        setattr(instance, col, row[col])
        else:
            await self._pool.execute_statement_py(sql, params)
            # For SQLite, we need to query back the row to get all values
            # This handles both insert (new ID) and update (existing ID) cases
            if pk_col and not do_nothing:
                # Query by conflict target to get the actual row
                conflict_cols = [conflict_target] if isinstance(conflict_target, str) else conflict_target
                where_parts = []
                query_params = []
                for col in conflict_cols:
                    if col in values:
                        where_parts.append(f"{col} = ?")
                        query_params.append(values[col])

                if where_parts:
                    query_sql = f"SELECT * FROM {table} WHERE {' AND '.join(where_parts)}"
                    result = await self._pool.execute(query_sql, query_params)
                    row = result.first()
                    if row:
                        # Update all columns from the returned row
                        for col in cls.__columns__:
                            if col in row:
                                setattr(instance, col, row[col])

        # Update identity map - this ensures session.get() returns
        # the upserted instance (not a stale cached one)
        if pk_col:
            pk_value = getattr(instance, pk_col, None)
            # Check that pk_value is not a ColumnInfo (can happen with do_nothing when
            # the record was not inserted due to conflict)
            if pk_value is not None and not isinstance(pk_value, ColumnInfo):
                self._identity_map[(cls, pk_value)] = instance

        return instance

    async def upsert_all(
        self,
        instances: list[T],
        conflict_target: str | list[str],
        update_fields: list[str] | None = None,
        do_nothing: bool = False,
    ) -> list[T]:
        """Bulk upsert multiple model instances.

        Args:
            instances: List of model instances to upsert
            conflict_target: Column(s) that define uniqueness
            update_fields: Columns to update on conflict. If None, updates all non-PK columns.
            do_nothing: If True, ignore conflicts instead of updating

        Returns:
            The upserted instances

        Example:
            >>> users = await session.upsert_all(
            ...     [User(email="a@b.com", name="A"), User(email="b@b.com", name="B")],
            ...     conflict_target="email"
            ... )
        """
        if not instances:
            return []

        # For now, upsert one at a time
        # TODO: Optimize with batch upsert for PostgreSQL using unnest()
        results = []
        for instance in instances:
            result = await self.upsert(
                instance,
                conflict_target=conflict_target,
                update_fields=update_fields,
                do_nothing=do_nothing,
            )
            results.append(result)

        return results

    # ========== Traditional Unit of Work API ==========

    def add(self, instance: Base) -> None:
        """Add a model instance to be inserted on commit."""
        self._pending_new.append(instance)

    def add_all(self, instances: list[Base]) -> None:
        """Add multiple model instances to be inserted on commit."""
        self._pending_new.extend(instances)

    def delete(self, instance: Base) -> None:
        """Mark a model instance for deletion on commit."""
        self._pending_delete.append(instance)

    async def commit(self) -> None:
        """Commit all pending changes to the database."""
        if self._pending_new:
            await self._flush_inserts()
        if self._pending_delete:
            await self._flush_deletes()

    async def rollback(self) -> None:
        """Discard all pending changes."""
        self._pending_new.clear()
        self._pending_dirty.clear()
        self._pending_delete.clear()

    async def flush(self) -> None:
        """Flush pending changes without committing (same as commit for now)."""
        await self.commit()

    # ========== Query Execution ==========

    async def execute(
        self,
        statement: SelectStatement[T] | InsertStatement[T] | UpdateStatement[T] | DeleteStatement[T],
    ) -> ExecuteResult[T]:
        """Execute a query statement."""
        if self._autoflush and self._pending_new:
            await self._flush_inserts()

        sql, params = statement.to_sql(self._dialect)
        result = await self._pool.execute(sql, params)
        return ExecuteResult(result, getattr(statement, "model", None))

    async def execute_raw(self, sql: str, params: list[Any] | None = None) -> QueryResult:
        """Execute raw SQL and return results."""
        return await self._pool.execute(sql, params or [])

    # ========== Internal Methods ==========

    async def _flush_inserts(self) -> None:
        """Insert all pending new objects."""
        by_class: dict[type[Base], list[Base]] = {}
        for obj in self._pending_new:
            cls = type(obj)
            if cls not in by_class:
                by_class[cls] = []
            by_class[cls].append(obj)

        for model_cls, instances in by_class.items():
            await self._batch_insert(model_cls, instances)

        self._pending_new.clear()

    async def _batch_insert(self, model_cls: type[Base], instances: list[Base]) -> None:
        """Perform batch insert for a single model class."""
        if not instances:
            return

        table = model_cls.__tablename__
        columns = [
            col_name
            for col_name, col_info in model_cls.__columns__.items()
            if not (col_info.primary_key and col_info.autoincrement)
        ]

        if not columns:
            return

        # SQLite has a limit of ~999 variables, batch accordingly
        max_params = 900 if self._dialect == "sqlite" else 30000
        batch_size = max_params // len(columns)

        for batch_start in range(0, len(instances), batch_size):
            batch = instances[batch_start:batch_start + batch_size]
            await self._insert_batch(model_cls, batch, columns, table)

    async def _insert_batch(
        self,
        model_cls: type[Base],
        instances: list[Base],
        columns: list[str],
        table: str,
    ) -> None:
        """Insert a single batch of instances."""
        params: list[Any] = []
        value_groups = []

        for instance in instances:
            placeholders = []
            for col in columns:
                if self._dialect == "postgresql":
                    placeholders.append(f"${len(params) + 1}")
                else:
                    placeholders.append("?")

                value = getattr(instance, col, None)
                params.append(value)
            value_groups.append(f"({', '.join(placeholders)})")

        sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES {', '.join(value_groups)}"

        pk_col = model_cls.__primary_key__
        if pk_col:
            # Use RETURNING to get generated IDs (works in PostgreSQL and SQLite 3.35+)
            sql += f" RETURNING {pk_col}"
            result = await self._pool.execute(sql, params)
            rows = result.all()
            for i, instance in enumerate(instances):
                if i < len(rows):
                    setattr(instance, pk_col, rows[i][pk_col])
                    # Add to identity map
                    self._identity_map[(model_cls, rows[i][pk_col])] = instance
        else:
            await self._pool.execute_statement_py(sql, params)

    async def _flush_deletes(self) -> None:
        """Delete all pending delete objects."""
        for obj in self._pending_delete:
            cls = type(obj)
            pk_col = cls.__primary_key__
            if pk_col is None:
                raise ValueError(f"Cannot delete {cls.__name__}: no primary key defined")

            pk_value = getattr(obj, pk_col, None)
            if pk_value is None:
                continue

            table = cls.__tablename__
            if self._dialect == "postgresql":
                sql = f"DELETE FROM {table} WHERE {pk_col} = $1"
            else:
                sql = f"DELETE FROM {table} WHERE {pk_col} = ?"

            await self._pool.execute_statement_py(sql, [pk_value])

            # Remove from identity map
            key = (cls, pk_value)
            self._identity_map.pop(key, None)

        self._pending_delete.clear()


class Transaction:
    """Transaction context for batch operations.

    Provides a cleaner interface for adding multiple objects in a transaction.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def add(self, instance: Base) -> Transaction:
        """Add a model to the transaction (chainable)."""
        self._session.add(instance)
        return self

    def add_all(self, instances: list[Base]) -> Transaction:
        """Add multiple models to the transaction (chainable)."""
        self._session.add_all(instances)
        return self

    def delete(self, instance: Base) -> Transaction:
        """Mark a model for deletion (chainable)."""
        self._session.delete(instance)
        return self


class Query[T]:
    """Fluent query builder for a model.

    Supports Django-style filter kwargs with operators:
        - field=value: Exact match
        - field__gt=value: Greater than
        - field__gte=value: Greater than or equal
        - field__lt=value: Less than
        - field__lte=value: Less than or equal
        - field__ne=value: Not equal
        - field__like=value: SQL LIKE pattern
        - field__ilike=value: Case-insensitive LIKE (PostgreSQL)
        - field__in=[values]: IN clause
        - field__notin=[values]: NOT IN clause
        - field__isnull=True/False: IS NULL / IS NOT NULL
        - field__contains=value: LIKE %value%
        - field__icontains=value: ILIKE %value% (case-insensitive)
        - field__startswith=value: LIKE value%
        - field__endswith=value: LIKE %value

    Soft Delete:
        Models using SoftDeleteMixin automatically exclude soft-deleted records.
        Use with_deleted() to include them or only_deleted() to query only deleted.
    """

    def __init__(self, session: AsyncSession, model: type[T]) -> None:
        self._session = session
        self._model = model
        self._filters: list[tuple[str, str, Any]] = []
        self._q_objects: list[Q] = []
        self._order: list[tuple[str, str]] = []
        self._limit_val: int | None = None
        self._offset_val: int | None = None
        self._load_options: list[Any] = []
        self._distinct: bool = False
        self._group_by: list[str] = []
        self._having: list[tuple[str, str, Any]] = []
        self._joins: list[JoinInfo] = []  # For proper JOIN support
        # Soft delete handling
        self._include_deleted: bool = False
        self._only_deleted: bool = False

    def filter(self, *q_objects: Q, **kwargs: Any) -> Query[T]:
        """Add filter conditions using Django-style kwargs or Q objects.

        Example:
            >>> query.filter(name="Alice", age__gt=18)
            >>> query.filter(Q(age__gt=18) | Q(vip=True))
            >>> query.filter(id__in=[1, 2, 3])
        """
        # Add Q objects
        self._q_objects.extend(q_objects)

        # Add keyword filters
        for key, value in kwargs.items():
            col, op = _parse_filter_key(key)
            self._filters.append((col, op, value))
        return self

    def filter_by(self, **kwargs: Any) -> Query[T]:
        """Alias for filter() with exact matches only."""
        for key, value in kwargs.items():
            self._filters.append((key, "eq", value))
        return self

    def distinct(self) -> Query[T]:
        """Add DISTINCT to the query.

        Example:
            >>> query.distinct().all()
        """
        self._distinct = True
        return self

    def group_by(self, *columns: str) -> Query[T]:
        """Add GROUP BY clause.

        Example:
            >>> query.group_by("status").count_by("status")
        """
        self._group_by.extend(columns)
        return self

    def having(self, **kwargs: Any) -> Query[T]:
        """Add HAVING clause (requires GROUP BY).

        Example:
            >>> query.group_by("status").having(count__gt=5)
        """
        for key, value in kwargs.items():
            col, op = _parse_filter_key(key)
            self._having.append((col, op, value))
        return self

    def order_by(self, *columns: str, desc: bool = False) -> Query[T]:
        """Add ORDER BY clause."""
        direction = "DESC" if desc else "ASC"
        for col in columns:
            if col.startswith("-"):
                self._order.append((col[1:], "DESC"))
            else:
                self._order.append((col, direction))
        return self

    def limit(self, n: int) -> Query[T]:
        """Limit results."""
        self._limit_val = n
        return self

    def offset(self, n: int) -> Query[T]:
        """Offset results."""
        self._offset_val = n
        return self

    def options(self, *opts: Any) -> Query[T]:
        """Add loading options for relationships.

        Example:
            >>> from ormkit import selectinload, joinedload
            >>> query.options(selectinload("posts"), joinedload("profile"))
        """
        self._load_options.extend(opts)
        return self

    def with_deleted(self) -> Query[T]:
        """Include soft-deleted records in results.

        Only affects models using SoftDeleteMixin.

        Example:
            >>> all_articles = await session.query(Article).with_deleted().all()
        """
        self._include_deleted = True
        return self

    def only_deleted(self) -> Query[T]:
        """Return only soft-deleted records.

        Only affects models using SoftDeleteMixin.

        Example:
            >>> deleted = await session.query(Article).only_deleted().all()
        """
        self._include_deleted = True  # Must include deleted to query them
        self._only_deleted = True
        return self

    async def all(self) -> list[T]:
        """Execute query and return all results."""
        result = await self._execute()
        instances = result.scalars().all()
        await self._apply_load_options(instances)
        return instances

    async def stream(self, batch_size: int = 1000) -> AsyncIterator[T]:
        """Stream results in batches to reduce memory usage.

        Use this for large result sets where you don't need all rows in memory at once.

        Example:
            >>> async for user in session.query(User).filter(active=True).stream():
            ...     process_user(user)

            >>> # With custom batch size
            >>> async for user in session.query(User).stream(batch_size=500):
            ...     process_user(user)
        """
        offset = 0
        while True:
            # Fetch a batch
            batch_query = Query(self._session, self._model)
            batch_query._filters = self._filters.copy()
            batch_query._q_objects = self._q_objects.copy()
            batch_query._order = self._order.copy()
            batch_query._distinct = self._distinct
            batch_query._group_by = self._group_by.copy()
            batch_query._having = self._having.copy()
            batch_query._load_options = self._load_options.copy()
            batch_query._limit_val = batch_size
            batch_query._offset_val = offset

            result = await batch_query._execute()
            instances = result.scalars().all()

            if not instances:
                break

            await batch_query._apply_load_options(instances)

            for instance in instances:
                yield instance

            if len(instances) < batch_size:
                break

            offset += batch_size

    def __aiter__(self) -> AsyncIterator[T]:
        """Allow using the query directly as an async iterator.

        Example:
            >>> async for user in session.query(User).filter(active=True):
            ...     process_user(user)
        """
        return self.stream()

    async def first(self) -> T | None:
        """Execute query and return first result."""
        self._limit_val = 1
        result = await self._execute()
        instance = result.scalars().first()
        if instance:
            await self._apply_load_options([instance])
        return instance

    async def one(self) -> T:
        """Execute query and return exactly one result."""
        result = await self._execute()
        instance = result.scalars().one()
        await self._apply_load_options([instance])
        return instance

    async def one_or_none(self) -> T | None:
        """Execute query and return one result or None."""
        result = await self._execute()
        instance = result.scalars().one_or_none()
        if instance:
            await self._apply_load_options([instance])
        return instance

    async def count(self) -> int:
        """Return count of matching rows."""
        sql, params = self._build_aggregate_sql("COUNT(*)", "count")
        result = await self._session._pool.execute(sql, params)
        row = result.first()
        return row["count"] if row else 0

    async def sum(self, column: str) -> float | None:
        """Return sum of a column."""
        sql, params = self._build_aggregate_sql(f"SUM({column})", "sum")
        result = await self._session._pool.execute(sql, params)
        row = result.first()
        return row["sum"] if row else None

    async def avg(self, column: str) -> float | None:
        """Return average of a column."""
        sql, params = self._build_aggregate_sql(f"AVG({column})", "avg")
        result = await self._session._pool.execute(sql, params)
        row = result.first()
        return row["avg"] if row else None

    async def min(self, column: str) -> Any:
        """Return minimum value of a column."""
        sql, params = self._build_aggregate_sql(f"MIN({column})", "min")
        result = await self._session._pool.execute(sql, params)
        row = result.first()
        return row["min"] if row else None

    async def max(self, column: str) -> Any:
        """Return maximum value of a column."""
        sql, params = self._build_aggregate_sql(f"MAX({column})", "max")
        result = await self._session._pool.execute(sql, params)
        row = result.first()
        return row["max"] if row else None

    async def exists(self) -> bool:
        """Check if any matching rows exist."""
        return await self.count() > 0

    async def delete(self) -> int:
        """Delete all matching rows and return count."""
        sql, params = self._build_delete_sql()
        return await self._session._pool.execute_statement_py(sql, params)

    async def update(self, **values: Any) -> int:
        """Update all matching rows and return count.

        Example:
            >>> count = await session.query(User).filter(age__lt=18).update(status="minor")
        """
        return await self._session.bulk_update(
            self._model,
            values,
            *self._q_objects,
            **{f"{col}__{op}" if op != "eq" else col: val for col, op, val in self._filters}
        )

    async def values(self, *columns: str) -> list[dict[str, Any]]:
        """Return specific columns as dicts (like Django's values()).

        Example:
            >>> await query.values("id", "name")
            [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        """
        sql, params = self._build_select_sql(columns)
        result = await self._session._pool.execute(sql, params)
        return list(result.all())

    async def values_list(self, *columns: str, flat: bool = False) -> list[Any]:
        """Return specific columns as tuples (like Django's values_list()).

        Example:
            >>> await query.values_list("id", "name")
            [(1, "Alice"), (2, "Bob")]

            >>> await query.values_list("name", flat=True)
            ["Alice", "Bob"]
        """
        sql, params = self._build_select_sql(columns)
        result = await self._session._pool.execute(sql, params)

        if flat and len(columns) == 1:
            return list(result.column(columns[0]))
        return list(result.tuples())

    async def _execute(self) -> ExecuteResult[T]:
        """Build and execute the SELECT statement."""
        sql, params = self._build_select_sql()
        result = await self._session._pool.execute(sql, params)

        # Check if we have JOINs to process
        join_infos = self._build_join_info()
        if join_infos:
            return ExecuteResult(result, self._model, join_infos)

        return ExecuteResult(result, self._model)

    def _build_where_clause(self, param_offset: int = 0) -> tuple[str, list[Any]]:
        """Build WHERE clause from filters, Q objects, and soft delete filtering."""
        dialect = self._session._dialect
        params: list[Any] = []
        where_parts: list[str] = []

        # Add soft delete filter if model uses SoftDeleteMixin
        if getattr(self._model, "__soft_delete__", False):
            if not self._include_deleted:
                # Exclude deleted records by default
                where_parts.append("deleted_at IS NULL")
            elif self._only_deleted:
                # Only return deleted records
                where_parts.append("deleted_at IS NOT NULL")
            # If _include_deleted but not _only_deleted, no filter (show all)

        # Handle Q objects first
        for q in self._q_objects:
            q_sql, q_params = q.to_sql(dialect, param_offset + len(params))
            if q_sql:
                where_parts.append(q_sql)
                params.extend(q_params)

        # Handle simple filters
        for col, op, value in self._filters:
            filter_sql, filter_params = _build_filter_sql(col, op, value, dialect, param_offset + len(params))
            where_parts.append(filter_sql)
            params.extend(filter_params)

        if where_parts:
            return " WHERE " + " AND ".join(where_parts), params
        return "", []

    def _build_select_sql(self, columns: tuple[str, ...] | None = None) -> tuple[str, list[Any]]:
        """Build full SELECT SQL."""
        dialect = self._session._dialect
        table = self._model.__tablename__
        main_alias = "_t0"

        # Build JOIN info for joinedload options
        join_infos = self._build_join_info()

        # Columns
        if columns:
            # User-specified columns - use them as-is
            col_str = ", ".join(f"{main_alias}.{c}" if join_infos else c for c in columns)
        else:
            # All columns from main table
            main_cols = [f"{main_alias}.{c} AS {c}" for c in self._model.__columns__.keys()] if join_infos else list(self._model.__columns__.keys())

            # Add aliased columns from joined tables
            joined_cols = []
            for join_info in join_infos:
                for col in join_info.target_model.__columns__.keys():
                    alias_col = f"{join_info.alias}.{col} AS {join_info.alias}_{col}"
                    joined_cols.append(alias_col)

            col_str = ", ".join(main_cols + joined_cols)

        # DISTINCT
        distinct = "DISTINCT " if self._distinct else ""

        # Build FROM clause with JOINs
        if join_infos:
            sql = f"SELECT {distinct}{col_str} FROM {table} AS {main_alias}"
            for join_info in join_infos:
                target_table = join_info.target_model.__tablename__
                sql += f" {join_info.join_type} JOIN {target_table} AS {join_info.alias}"
                sql += f" ON {main_alias}.{join_info.local_col} = {join_info.alias}.{join_info.remote_col}"
        else:
            sql = f"SELECT {distinct}{col_str} FROM {table}"

        params: list[Any] = []

        # WHERE
        where_sql, where_params = self._build_where_clause()
        sql += where_sql
        params.extend(where_params)

        # GROUP BY
        if self._group_by:
            sql += " GROUP BY " + ", ".join(self._group_by)

        # HAVING
        if self._having:
            having_parts = []
            for col, op, value in self._having:
                filter_sql, filter_params = _build_filter_sql(col, op, value, dialect, len(params))
                having_parts.append(filter_sql)
                params.extend(filter_params)
            sql += " HAVING " + " AND ".join(having_parts)

        # ORDER BY
        if self._order:
            order_parts = [f"{col} {direction}" for col, direction in self._order]
            sql += " ORDER BY " + ", ".join(order_parts)

        # LIMIT / OFFSET
        if self._limit_val is not None:
            sql += f" LIMIT {self._limit_val}"
        if self._offset_val is not None:
            sql += f" OFFSET {self._offset_val}"

        return sql, params

    def _build_join_info(self) -> list[JoinInfo]:
        """Build JOIN info from joinedload options."""
        from ormkit.relationships import LoadOption

        join_infos: list[JoinInfo] = []

        # Resolve relationships first
        self._model._resolve_relationships()

        alias_counter = 1
        for opt in self._load_options:
            if not isinstance(opt, LoadOption):
                continue
            if opt.strategy != "joined":
                continue

            rel_name = opt.attr_name
            if rel_name not in self._model.__relationships__:
                continue

            rel_info = self._model.__relationships__[rel_name]

            # Resolve target model if needed
            if rel_info._target_model is None:
                rel_info.resolve(self._model, rel_name, self._model.__hints__.get(rel_name))

            target_model = rel_info._target_model
            if target_model is None:
                continue

            # Only use JOINs for many-to-one relationships (single object)
            # For one-to-many (uselist=True), selectinload is more appropriate
            if rel_info.uselist:
                continue

            # Many-to-one: FK is on main table, join to target
            local_col = rel_info._local_fk_column
            remote_col = rel_info._remote_pk_column or target_model.__primary_key__

            if not local_col or not remote_col:
                continue

            join_info = JoinInfo(
                rel_name=rel_name,
                target_model=target_model,
                join_type="LEFT",
                local_col=local_col,
                remote_col=remote_col,
                alias=f"_j{alias_counter}",
            )
            join_infos.append(join_info)
            alias_counter += 1

        return join_infos

    def _build_aggregate_sql(self, agg_expr: str, alias: str) -> tuple[str, list[Any]]:
        """Build aggregate SQL (COUNT, SUM, AVG, etc.)."""
        table = self._model.__tablename__
        sql = f"SELECT {agg_expr} as {alias} FROM {table}"
        params: list[Any] = []

        where_sql, where_params = self._build_where_clause()
        sql += where_sql
        params.extend(where_params)

        return sql, params

    def _build_delete_sql(self) -> tuple[str, list[Any]]:
        """Build DELETE SQL."""
        table = self._model.__tablename__
        sql = f"DELETE FROM {table}"
        params: list[Any] = []

        where_sql, where_params = self._build_where_clause()
        sql += where_sql
        params.extend(where_params)

        return sql, params

    async def _apply_load_options(self, instances: list[T]) -> None:
        """Apply eager loading options to loaded instances."""
        if not instances or not self._load_options:
            return

        from ormkit.relationships import LoadOption

        # Get relationships that were already loaded via JOIN
        join_infos = self._build_join_info()
        joined_rel_names = {j.rel_name for j in join_infos}

        # Resolve relationships if needed
        self._model._resolve_relationships()

        for opt in self._load_options:
            if not isinstance(opt, LoadOption):
                continue

            rel_name = opt.attr_name
            if rel_name not in self._model.__relationships__:
                continue

            # Skip if already loaded via JOIN
            if rel_name in joined_rel_names:
                continue

            rel_info = self._model.__relationships__[rel_name]

            if opt.strategy == "selectin":
                await self._load_selectin(instances, rel_name, rel_info)
            elif opt.strategy == "joined":
                # For one-to-many relationships, use selectinload strategy
                # (JOINs would create duplicate rows)
                await self._load_selectin(instances, rel_name, rel_info)
            elif opt.strategy == "noload":
                # Set empty values
                for instance in instances:
                    instance._set_relationship(rel_name, [] if rel_info.uselist else None)

    async def _load_selectin(
        self, instances: list[T], rel_name: str, rel_info: Any
    ) -> None:
        """Load a relationship using SELECT IN strategy."""
        # Resolve target model
        if rel_info._target_model is None:
            rel_info.resolve(self._model, rel_name, self._model.__hints__.get(rel_name))

        target_model = rel_info._target_model
        if target_model is None:
            return

        dialect = self._session._dialect

        # Check if this is a many-to-many relationship
        if rel_info.is_many_to_many:
            await self._load_selectin_m2m(instances, rel_name, rel_info, target_model)
            return

        if rel_info.uselist:
            # One-to-many: FK is on target model
            fk_col = rel_info._local_fk_column
            pk_col = rel_info._remote_pk_column or self._model.__primary_key__

            if not fk_col or not pk_col:
                return

            parent_ids = [getattr(inst, pk_col) for inst in instances if hasattr(inst, pk_col)]
            if not parent_ids:
                for instance in instances:
                    instance._set_relationship(rel_name, [])
                return

            table = target_model.__tablename__
            if dialect == "postgresql":
                placeholders = ", ".join(f"${i+1}" for i in range(len(parent_ids)))
            else:
                placeholders = ", ".join("?" for _ in parent_ids)

            sql = f"SELECT * FROM {table} WHERE {fk_col} IN ({placeholders})"
            result = await self._session._pool.execute(sql, parent_ids)

            related_by_parent: dict[Any, list[Any]] = {pid: [] for pid in parent_ids}
            for row in result.all():
                row_dict = dict(row)
                parent_id = row_dict.get(fk_col)
                if parent_id in related_by_parent:
                    related_by_parent[parent_id].append(target_model._from_row_fast(row_dict))

            for instance in instances:
                parent_id = getattr(instance, pk_col, None)
                instance._set_relationship(rel_name, related_by_parent.get(parent_id, []))

        else:
            # Many-to-one: FK is on this model
            fk_col = rel_info._local_fk_column
            remote_pk = rel_info._remote_pk_column or target_model.__primary_key__

            if not fk_col or not remote_pk:
                return

            fk_values = list({
                getattr(inst, fk_col) for inst in instances
                if hasattr(inst, fk_col) and getattr(inst, fk_col) is not None
            })

            if not fk_values:
                for instance in instances:
                    instance._set_relationship(rel_name, None)
                return

            table = target_model.__tablename__
            if dialect == "postgresql":
                placeholders = ", ".join(f"${i+1}" for i in range(len(fk_values)))
            else:
                placeholders = ", ".join("?" for _ in fk_values)

            sql = f"SELECT * FROM {table} WHERE {remote_pk} IN ({placeholders})"
            result = await self._session._pool.execute(sql, fk_values)

            related_by_pk: dict[Any, Any] = {}
            for row in result.all():
                row_dict = dict(row)
                pk_value = row_dict.get(remote_pk)
                related_by_pk[pk_value] = target_model._from_row_fast(row_dict)

            for instance in instances:
                fk_value = getattr(instance, fk_col, None)
                instance._set_relationship(rel_name, related_by_pk.get(fk_value))

    async def _load_selectin_m2m(
        self,
        instances: list[T],
        rel_name: str,
        rel_info: Any,
        target_model: type,
    ) -> None:
        """Load a many-to-many relationship via junction table.

        This performs two queries:
        1. SELECT from junction table to get associations
        2. SELECT from target table to get related objects
        """
        dialect = self._session._dialect
        junction_table = rel_info.secondary
        pk_col = self._model.__primary_key__

        if not pk_col:
            return

        # Get junction table column names
        junction_local = rel_info._junction_local_col
        junction_remote = rel_info._junction_remote_col
        target_pk = target_model.__primary_key__

        if not junction_local or not junction_remote or not target_pk:
            return

        # Get parent IDs
        parent_ids = [
            getattr(inst, pk_col)
            for inst in instances
            if hasattr(inst, pk_col) and getattr(inst, pk_col) is not None
        ]
        if not parent_ids:
            for instance in instances:
                instance._set_relationship(rel_name, [])
            return

        # Query junction table to get associations
        if dialect == "postgresql":
            placeholders = ", ".join(f"${i+1}" for i in range(len(parent_ids)))
        else:
            placeholders = ", ".join("?" for _ in parent_ids)

        junction_sql = (
            f"SELECT {junction_local}, {junction_remote} "
            f"FROM {junction_table} "
            f"WHERE {junction_local} IN ({placeholders})"
        )
        junction_result = await self._session._pool.execute(junction_sql, parent_ids)
        junction_rows = junction_result.all()

        # Build mapping: parent_id -> list of target_ids
        parent_to_targets: dict[Any, list[Any]] = {pid: [] for pid in parent_ids}
        target_ids_needed: set[Any] = set()

        for row in junction_rows:
            parent_id = row[junction_local]
            target_id = row[junction_remote]
            if parent_id in parent_to_targets:
                parent_to_targets[parent_id].append(target_id)
                target_ids_needed.add(target_id)

        if not target_ids_needed:
            for instance in instances:
                instance._set_relationship(rel_name, [])
            return

        # Query target table to get related objects
        target_table = target_model.__tablename__
        target_ids_list = list(target_ids_needed)

        if dialect == "postgresql":
            target_placeholders = ", ".join(
                f"${i+1}" for i in range(len(target_ids_list))
            )
        else:
            target_placeholders = ", ".join("?" for _ in target_ids_list)

        target_sql = (
            f"SELECT * FROM {target_table} "
            f"WHERE {target_pk} IN ({target_placeholders})"
        )
        target_result = await self._session._pool.execute(target_sql, target_ids_list)

        # Build mapping: target_id -> target instance
        targets_by_id: dict[Any, Any] = {}
        for row in target_result.all():
            row_dict = dict(row)
            tid = row_dict.get(target_pk)
            targets_by_id[tid] = target_model._from_row_fast(row_dict)

        # Assemble related objects for each instance
        for instance in instances:
            parent_id = getattr(instance, pk_col, None)
            target_ids = parent_to_targets.get(parent_id, [])
            related = [
                targets_by_id[tid]
                for tid in target_ids
                if tid in targets_by_id
            ]
            # Pass session so ManyToManyCollection can be created
            instance._set_relationship(rel_name, related, self._session)


class ExecuteResult[T: "Base"]:
    """Result from executing a query statement."""

    def __init__(
        self,
        result: QueryResult,
        model: type[T] | None = None,
        join_infos: list[JoinInfo] | None = None,
    ) -> None:
        self._result = result
        self._model = model
        self._join_infos = join_infos or []

    def scalars(self) -> ScalarResult[T]:
        """Get results as model instances."""
        return ScalarResult(self._result, self._model, self._join_infos)

    def all(self) -> list[dict[str, Any]]:
        """Get all results as dictionaries."""
        return list(self._result.all())

    def first(self) -> dict[str, Any] | None:
        """Get the first result as a dictionary."""
        return self._result.first()

    def one(self) -> dict[str, Any]:
        """Get exactly one result, or raise an error."""
        return self._result.one()

    def one_or_none(self) -> dict[str, Any] | None:
        """Get one result or None."""
        return self._result.one_or_none()

    @property
    def rowcount(self) -> int:
        """Number of rows returned."""
        return self._result.rowcount


class ScalarResult[T: "Base"]:
    """Result wrapper that converts rows to model instances."""

    def __init__(
        self,
        result: QueryResult,
        model: type[T] | None,
        join_infos: list[JoinInfo] | None = None,
    ) -> None:
        self._result = result
        self._model = model
        self._join_infos = join_infos or []

    def all(self) -> list[T]:
        """Get all results as model instances."""
        if self._model is None:
            raise ValueError("Cannot convert to model: no model specified")

        if self._join_infos:
            return self._hydrate_with_joins(self._result.all())

        # Rust's to_models() already returns a Python list, no need to wrap
        return self._result.to_models(self._model)

    def first(self) -> T | None:
        """Get the first result as a model instance."""
        if self._model is None:
            return None

        if self._join_infos:
            rows = self._result.all()
            if not rows:
                return None
            return self._hydrate_with_joins([rows[0]])[0]

        return self._result.to_model(self._model)

    def one(self) -> T:
        """Get exactly one result as a model instance."""
        if self._model is None:
            raise ValueError("Cannot convert to model: no model specified")
        if len(self._result) != 1:
            raise ValueError(f"Expected exactly 1 row, got {len(self._result)}")

        if self._join_infos:
            rows = self._result.all()
            return self._hydrate_with_joins(rows)[0]

        result = self._result.to_model(self._model)
        if result is None:
            raise ValueError("Expected exactly 1 row, got 0")
        return result

    def one_or_none(self) -> T | None:
        """Get one result or None."""
        if self._model is None:
            return None
        if len(self._result) > 1:
            raise ValueError(f"Expected at most 1 row, got {len(self._result)}")

        if self._join_infos:
            rows = self._result.all()
            if not rows:
                return None
            return self._hydrate_with_joins(rows)[0]

        return self._result.to_model(self._model)

    def _hydrate_with_joins(self, rows: list[dict[str, Any]]) -> list[T]:
        """Hydrate model instances with joined relationship data."""
        if self._model is None:
            return []

        instances: list[T] = []

        for row in rows:
            # Extract main model columns (not prefixed)
            main_data = {}
            for col in self._model.__columns__.keys():
                if col in row:
                    main_data[col] = row[col]

            # Create main instance
            instance = self._model._from_row_fast(main_data)

            # Hydrate each joined relationship
            for join_info in self._join_infos:
                alias = join_info.alias
                target_model = join_info.target_model

                # Extract joined columns (prefixed with alias_)
                related_data = {}
                has_data = False
                for col in target_model.__columns__.keys():
                    key = f"{alias}_{col}"
                    if key in row:
                        related_data[col] = row[key]
                        if row[key] is not None:
                            has_data = True

                # Create related instance if we have non-null data
                if has_data:
                    related_instance = target_model._from_row_fast(related_data)
                    instance._set_relationship(join_info.rel_name, related_instance)
                else:
                    instance._set_relationship(join_info.rel_name, None)

            instances.append(instance)

        return instances


# ========== Convenience Functions ==========

def create_session(pool: ConnectionPool, **kwargs: Any) -> AsyncSession:
    """Create a new session from a connection pool.

    Example:
        >>> session = create_session(engine)
    """
    return AsyncSession(pool, **kwargs)


@asynccontextmanager
async def session_context(pool: ConnectionPool, **kwargs: Any) -> AsyncIterator[AsyncSession]:
    """Create a session context that auto-commits on success.

    Example:
        >>> async with session_context(engine) as session:
        ...     await session.insert(User(name="Alice"))
        ...     # auto-commits on exit
    """
    session = AsyncSession(pool, **kwargs)
    try:
        yield session
        await session.commit()
    except Exception:
        await session.rollback()
        raise
