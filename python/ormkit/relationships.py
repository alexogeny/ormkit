"""Relationship definitions for ORM models."""

from __future__ import annotations

import typing
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ormkit.base import Base


# Global model registry - maps table names and class names to model classes
_model_registry: dict[str, type[Base]] = {}


def register_model(model_cls: type[Base]) -> None:
    """Register a model class for relationship resolution."""
    _model_registry[model_cls.__tablename__] = model_cls
    _model_registry[model_cls.__name__] = model_cls


def get_model(name: str) -> type[Base] | None:
    """Get a model class by table name or class name."""
    return _model_registry.get(name)


@dataclass
class RelationshipInfo:
    """Stores metadata about a relationship between models."""

    name: str | None = None
    back_populates: str | None = None
    foreign_keys: list[str] | None = None
    lazy: str = "select"  # select, joined, subquery, selectin, raise
    uselist: bool | None = None  # True for one-to-many, False for many-to-one
    secondary: str | None = None  # Junction table name for many-to-many relationships

    # Resolved at runtime
    _target_model: type[Base] | None = field(default=None, repr=False)
    _local_fk_column: str | None = field(default=None, repr=False)
    _remote_pk_column: str | None = field(default=None, repr=False)
    # M2M specific - column names in the junction table
    _junction_local_col: str | None = field(default=None, repr=False)
    _junction_remote_col: str | None = field(default=None, repr=False)

    @property
    def is_many_to_many(self) -> bool:
        """Check if this is a many-to-many relationship."""
        return self.secondary is not None

    def resolve(self, owner_model: type[Base], attr_name: str, type_hint: Any) -> None:
        """Resolve the relationship target model and columns."""
        self.name = attr_name

        # Extract target model from type hint (e.g., Mapped[list["Post"]] -> Post)
        target_name = self._extract_target_from_hint(type_hint)
        if target_name:
            self._target_model = get_model(target_name)

        # Determine if this is a collection or single relationship
        if self.uselist is None:
            # M2M relationships are always collections on both sides
            if self.secondary:
                self.uselist = True
            else:
                self.uselist = self._is_list_type(type_hint)

        # Find the foreign key column
        if self._target_model and self.foreign_keys is None:
            if self.secondary:
                self._resolve_m2m_columns(owner_model)
            else:
                self._resolve_foreign_key(owner_model)

    def _extract_target_from_hint(self, hint: Any) -> str | None:
        """Extract target model name from type hint."""
        # Handle Mapped[T] or Mapped[list[T]]
        typing.get_origin(hint)
        args = typing.get_args(hint)

        if not args:
            return None

        inner = args[0]

        # Handle list[T]
        inner_origin = typing.get_origin(inner)
        if inner_origin is list:
            inner_args = typing.get_args(inner)
            if inner_args:
                inner = inner_args[0]

        # Get the model name
        if isinstance(inner, str):
            return inner
        elif isinstance(inner, type):
            return inner.__name__
        elif hasattr(inner, "__forward_arg__"):
            return inner.__forward_arg__

        return None

    def _is_list_type(self, hint: Any) -> bool:
        """Check if the type hint indicates a collection."""
        args = typing.get_args(hint)
        if not args:
            return False

        inner = args[0]
        inner_origin = typing.get_origin(inner)
        return inner_origin is list

    def _resolve_foreign_key(self, owner_model: type[Base]) -> None:
        """Find the foreign key column linking the models."""
        if self._target_model is None:
            return

        target_table = self._target_model.__tablename__
        owner_table = owner_model.__tablename__

        if self.uselist:
            # One-to-many: FK is on the target model referencing owner
            # e.g., User.posts -> Post.author_id references users.id
            for col_name, col_info in self._target_model.__columns__.items():
                if col_info.foreign_key and col_info.foreign_key.table == owner_table:
                    self._local_fk_column = col_name
                    self._remote_pk_column = owner_model.__primary_key__
                    break
        else:
            # Many-to-one: FK is on owner model referencing target
            # e.g., Post.author -> Post.author_id references users.id
            for col_name, col_info in owner_model.__columns__.items():
                if col_info.foreign_key and col_info.foreign_key.table == target_table:
                    self._local_fk_column = col_name
                    self._remote_pk_column = col_info.foreign_key.column
                    break

    def _resolve_m2m_columns(self, owner_model: type[Base]) -> None:
        """Resolve column names for many-to-many relationship via junction table.

        Convention: Junction table columns are named {table}_id
        e.g., user_roles has user_id and role_id columns
        """
        if self._target_model is None or self.secondary is None:
            return

        owner_table = owner_model.__tablename__
        target_table = self._target_model.__tablename__

        # Primary key columns
        self._local_fk_column = owner_model.__primary_key__
        self._remote_pk_column = self._target_model.__primary_key__

        # Junction table column naming convention: {table}_id
        # e.g., users -> user_id, roles -> role_id
        # Strip trailing 's' for common pluralization
        owner_singular = owner_table.rstrip("s") if owner_table.endswith("s") else owner_table
        target_singular = target_table.rstrip("s") if target_table.endswith("s") else target_table

        self._junction_local_col = f"{owner_singular}_id"
        self._junction_remote_col = f"{target_singular}_id"


def relationship(
    *,
    back_populates: str | None = None,
    foreign_keys: list[str] | None = None,
    lazy: str = "select",
    uselist: bool | None = None,
    secondary: str | None = None,
) -> Any:
    """Define a relationship between models.

    Args:
        back_populates: Name of the relationship on the related model
        foreign_keys: List of foreign key column names (for ambiguous relationships)
        lazy: Loading strategy - "select", "joined", "subquery", "selectin", "raise"
        uselist: Whether to return a list (True) or single object (False)
        secondary: Table name for many-to-many relationships

    Returns:
        A RelationshipInfo descriptor

    Example:
        >>> class User(Base):
        ...     posts: Mapped[list["Post"]] = relationship(back_populates="author")
        ...
        >>> class Post(Base):
        ...     author: Mapped[User] = relationship(back_populates="posts")
    """
    return RelationshipInfo(
        back_populates=back_populates,
        foreign_keys=foreign_keys,
        lazy=lazy,
        uselist=uselist,
        secondary=secondary,
    )


def selectinload(attr: str | RelationshipInfo) -> LoadOption:
    """Eager load a relationship using a separate SELECT IN query.

    This is the recommended approach for loading collections efficiently.

    Example:
        >>> users = await session.query(User).options(selectinload("posts")).all()
        >>> # Or with the relationship object:
        >>> users = await session.query(User).options(selectinload(User.posts)).all()
    """
    return LoadOption("selectin", attr)


def joinedload(attr: str | RelationshipInfo) -> LoadOption:
    """Eager load a relationship using a JOIN.

    Use this for single-object relationships (many-to-one, one-to-one).

    Example:
        >>> posts = await session.query(Post).options(joinedload("author")).all()
    """
    return LoadOption("joined", attr)


def lazyload(attr: str | RelationshipInfo) -> LoadOption:
    """Explicitly set lazy loading for a relationship.

    Example:
        >>> posts = await session.query(Post).options(lazyload("author")).all()
    """
    return LoadOption("select", attr)


def noload(attr: str | RelationshipInfo) -> LoadOption:
    """Disable loading for a relationship.

    Example:
        >>> users = await session.query(User).options(noload("posts")).all()
    """
    return LoadOption("noload", attr)


@dataclass
class LoadOption:
    """Represents a relationship loading option."""

    strategy: str  # "selectin", "joined", "select", "noload", "raise"
    attribute: str | RelationshipInfo

    @property
    def attr_name(self) -> str:
        """Get the attribute name regardless of how it was specified."""
        if isinstance(self.attribute, str):
            return self.attribute
        elif isinstance(self.attribute, RelationshipInfo):
            return self.attribute.name or ""
        return str(self.attribute)

    def __repr__(self) -> str:
        return f"<LoadOption {self.strategy} {self.attr_name}>"


class ManyToManyCollection(list):
    """Proxy collection for many-to-many relationships.

    Provides async add/remove/clear operations that modify the junction table.

    Example:
        >>> await user.roles.add(admin_role)
        >>> await user.roles.add(editor_role, viewer_role)
        >>> await user.roles.remove(viewer_role)
        >>> await user.roles.clear()
    """

    def __init__(
        self,
        owner: Base,
        rel_info: RelationshipInfo,
        session: Any,
        initial: list | None = None,
    ) -> None:
        super().__init__(initial or [])
        self._owner = owner
        self._rel_info = rel_info
        self._session = session

    async def add(self, *items: Base) -> None:
        """Add items to the relationship (inserts into junction table).

        This is idempotent - adding an already-associated item is a no-op.
        """
        if not items:
            return

        junction_table = self._rel_info.secondary
        junction_local = self._rel_info._junction_local_col
        junction_remote = self._rel_info._junction_remote_col
        owner_pk_col = self._owner.__class__.__primary_key__
        target_pk_col = self._rel_info._target_model.__primary_key__

        if not all([junction_table, junction_local, junction_remote, owner_pk_col, target_pk_col]):
            raise RuntimeError("M2M relationship not properly configured")

        owner_id = getattr(self._owner, owner_pk_col)
        dialect = self._session._dialect

        for item in items:
            target_id = getattr(item, target_pk_col)

            # Use INSERT OR IGNORE / ON CONFLICT DO NOTHING for idempotency
            if dialect == "postgresql":
                sql = (
                    f"INSERT INTO {junction_table} ({junction_local}, {junction_remote}) "
                    f"VALUES ($1, $2) ON CONFLICT DO NOTHING"
                )
            else:
                sql = (
                    f"INSERT OR IGNORE INTO {junction_table} "
                    f"({junction_local}, {junction_remote}) VALUES (?, ?)"
                )

            await self._session._pool.execute_statement_py(sql, [owner_id, target_id])

            # Update local list if not already present
            if item not in self:
                self.append(item)

    async def remove(self, *items: Base) -> None:
        """Remove items from the relationship (deletes from junction table).

        This is safe to call even if the item isn't associated (no-op).
        """
        if not items:
            return

        junction_table = self._rel_info.secondary
        junction_local = self._rel_info._junction_local_col
        junction_remote = self._rel_info._junction_remote_col
        owner_pk_col = self._owner.__class__.__primary_key__
        target_pk_col = self._rel_info._target_model.__primary_key__

        if not all([junction_table, junction_local, junction_remote, owner_pk_col, target_pk_col]):
            raise RuntimeError("M2M relationship not properly configured")

        owner_id = getattr(self._owner, owner_pk_col)
        dialect = self._session._dialect

        for item in items:
            target_id = getattr(item, target_pk_col)

            if dialect == "postgresql":
                sql = (
                    f"DELETE FROM {junction_table} "
                    f"WHERE {junction_local} = $1 AND {junction_remote} = $2"
                )
            else:
                sql = (
                    f"DELETE FROM {junction_table} "
                    f"WHERE {junction_local} = ? AND {junction_remote} = ?"
                )

            await self._session._pool.execute_statement_py(sql, [owner_id, target_id])

            # Update local list
            try:
                list.remove(self, item)
            except ValueError:
                pass  # Item wasn't in the list

    async def clear(self) -> None:
        """Remove all items from the relationship."""
        junction_table = self._rel_info.secondary
        junction_local = self._rel_info._junction_local_col
        owner_pk_col = self._owner.__class__.__primary_key__

        if not all([junction_table, junction_local, owner_pk_col]):
            raise RuntimeError("M2M relationship not properly configured")

        owner_id = getattr(self._owner, owner_pk_col)
        dialect = self._session._dialect

        if dialect == "postgresql":
            sql = f"DELETE FROM {junction_table} WHERE {junction_local} = $1"
        else:
            sql = f"DELETE FROM {junction_table} WHERE {junction_local} = ?"

        await self._session._pool.execute_statement_py(sql, [owner_id])

        # Clear local list
        list.clear(self)
