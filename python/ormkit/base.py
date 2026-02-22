"""Declarative base for ORM models."""

from __future__ import annotations

import typing
from typing import Any, ClassVar, get_type_hints

if typing.TYPE_CHECKING:
    from ormkit.fields import ColumnInfo
    from ormkit.relationships import RelationshipInfo


class ModelMeta(type):
    """Metaclass for ORM models that processes field definitions."""

    def __new__(
        mcs,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
        **kwargs: Any,
    ) -> ModelMeta:
        cls = super().__new__(mcs, name, bases, namespace, **kwargs)

        # Skip processing for the Base class itself
        if name == "Base" and not bases:
            return cls

        # Get table name
        tablename = namespace.get("__tablename__")
        if tablename is None and any(isinstance(b, ModelMeta) for b in bases):
            # Generate table name from class name
            tablename = name.lower() + "s"

        cls.__tablename__ = tablename  # type: ignore[attr-defined]

        # Process column definitions
        columns: dict[str, ColumnInfo] = {}
        relationships: dict[str, RelationshipInfo] = {}

        # Get type hints including from parent classes
        # We need to provide the proper namespace for get_type_hints to resolve forward refs
        try:
            # Include the module's globals for proper resolution
            import sys
            module = sys.modules.get(cls.__module__, None)
            globalns = dict(getattr(module, "__dict__", {})) if module else {}
            # Add required typing constructs
            globalns["ClassVar"] = ClassVar
            globalns["Any"] = Any
            # Also include ormkit types
            from ormkit.fields import ColumnInfo as CI
            from ormkit.fields import Mapped
            from ormkit.relationships import RelationshipInfo as RI
            globalns["Mapped"] = Mapped
            globalns["ColumnInfo"] = CI
            globalns["RelationshipInfo"] = RI
            hints = get_type_hints(cls, globalns=globalns, localns={})
        except Exception:
            hints = {}

        # Import here to avoid circular imports
        from ormkit.fields import ColumnInfo, Mapped
        from ormkit.relationships import RelationshipInfo

        # Process annotations and collect column info from class namespace
        for attr_name, attr_value in namespace.items():
            if attr_name.startswith("_"):
                continue

            if isinstance(attr_value, ColumnInfo):
                attr_value.name = attr_name
                # Try to get type from annotation
                if attr_name in hints:
                    python_type = _extract_mapped_type(hints[attr_name])
                    attr_value.python_type = python_type
                    # Auto-detect JSON columns from dict/list type hints
                    if python_type is dict or python_type is list:
                        attr_value.is_json = True
                columns[attr_name] = attr_value
            elif isinstance(attr_value, RelationshipInfo):
                attr_value.name = attr_name
                relationships[attr_name] = attr_value
                # Remove from namespace so __getattr__ can handle it
                # We'll store it in __relationships__ instead
                delattr(cls, attr_name)

        # Check for ColumnInfo from parent classes/mixins FIRST (like SoftDeleteMixin)
        # This must happen BEFORE auto-generating from type annotations, because
        # get_type_hints() includes inherited annotations too
        for base in bases:
            for attr_name in dir(base):
                if attr_name.startswith("_") or attr_name in columns:
                    continue
                attr_value = getattr(base, attr_name, None)
                if isinstance(attr_value, ColumnInfo):
                    # Clone the ColumnInfo to avoid sharing between classes
                    col_copy = ColumnInfo(
                        name=attr_name,
                        python_type=attr_value.python_type,
                        primary_key=attr_value.primary_key,
                        nullable=attr_value.nullable,
                        unique=attr_value.unique,
                        index=attr_value.index,
                        default=attr_value.default,
                        server_default=attr_value.server_default,
                        max_length=attr_value.max_length,
                        foreign_key=attr_value.foreign_key,
                        autoincrement=attr_value.autoincrement,
                        is_json=attr_value.is_json,
                    )
                    columns[attr_name] = col_copy

        # Also process type annotations that are Mapped but have no mapped_column() value
        # These auto-generate a ColumnInfo based on the type
        # This runs AFTER parent class check so mixin columns are not overwritten
        for attr_name, hint in hints.items():
            if attr_name.startswith("_") or attr_name in columns or attr_name in relationships:
                continue
            # Check if this is a Mapped annotation
            hint_str = str(hint)
            if "Mapped[" in hint_str or "Mapped" in str(typing.get_origin(hint) or ""):
                # Extract the Python type from Mapped[T]
                python_type = _extract_mapped_type(hint)
                if python_type is not None:
                    # Check if the class attribute exists and isn't a ColumnInfo or RelationshipInfo
                    attr_val = namespace.get(attr_name)
                    if attr_val is None or not isinstance(attr_val, (ColumnInfo, RelationshipInfo)):
                        # Check if it's Optional (nullable)
                        is_nullable = "None" in hint_str or typing.get_origin(python_type) is typing.Union
                        # Auto-create ColumnInfo
                        col = ColumnInfo(
                            name=attr_name,
                            python_type=python_type if not is_nullable else (
                                typing.get_args(python_type)[0] if typing.get_args(python_type) else python_type
                            ),
                            nullable=is_nullable,
                            is_json=(python_type is dict or python_type is list),
                        )
                        columns[attr_name] = col

        cls.__columns__ = columns  # type: ignore[attr-defined]
        cls.__relationships__ = relationships  # type: ignore[attr-defined]
        cls.__primary_key__ = None  # type: ignore[attr-defined]
        cls.__hints__ = hints  # type: ignore[attr-defined]
        cls.__relationships_resolved__ = False  # type: ignore[attr-defined]

        # Find primary key
        for col_name, col_info in columns.items():
            if col_info.primary_key:
                cls.__primary_key__ = col_name  # type: ignore[attr-defined]
                break

        # Register model for relationship resolution
        from ormkit.relationships import register_model
        register_model(cls)  # type: ignore[arg-type]

        return cls

    def _resolve_relationships(cls) -> None:
        """Resolve all relationships after all models are defined."""
        unresolved = [
            (rel_name, rel_info)
            for rel_name, rel_info in cls.__relationships__.items()
            if rel_info._target_model is None
        ]
        if not unresolved:
            cls.__relationships_resolved__ = True  # type: ignore[attr-defined]
            return

        if getattr(cls, "__relationships_resolved__", False):
            return

        # Re-resolve hints now that all models may be defined
        try:
            import sys
            module = sys.modules.get(cls.__module__, None)
            globalns = dict(getattr(module, "__dict__", {})) if module else {}
            globalns["ClassVar"] = ClassVar
            globalns["Any"] = Any
            from ormkit.fields import ColumnInfo as CI
            from ormkit.fields import Mapped
            from ormkit.relationships import RelationshipInfo as RI
            from ormkit.relationships import _model_registry
            globalns["Mapped"] = Mapped
            globalns["ColumnInfo"] = CI
            globalns["RelationshipInfo"] = RI
            # Add all registered models to globalns
            globalns.update(_model_registry)
            hints = get_type_hints(cls, globalns=globalns, localns={})
        except Exception:
            hints = {}

        for rel_name, rel_info in unresolved:
            rel_info.resolve(cls, rel_name, hints.get(rel_name))  # type: ignore[arg-type]

        cls.__relationships_resolved__ = all(
            rel_info._target_model is not None for rel_info in cls.__relationships__.values()
        )  # type: ignore[attr-defined]


def _extract_mapped_type(hint: Any) -> type | None:
    """Extract the inner type from Mapped[T] annotation."""
    origin = typing.get_origin(hint)
    if origin is not None:
        args = typing.get_args(hint)
        if args:
            # Handle Optional (Union with None)
            if origin is typing.Union:
                non_none = [a for a in args if a is not type(None)]
                if len(non_none) == 1:
                    return non_none[0]
            return args[0]
    return hint if isinstance(hint, type) else None


class Base(metaclass=ModelMeta):
    """Base class for all ORM models.

    Example:
        >>> class User(Base):
        ...     __tablename__ = "users"
        ...     id: Mapped[int] = mapped_column(primary_key=True)
        ...     name: Mapped[str] = mapped_column(max_length=100)
    """

    __tablename__: ClassVar[str]
    __columns__: ClassVar[dict[str, ColumnInfo]]
    __relationships__: ClassVar[dict[str, RelationshipInfo]]
    __primary_key__: ClassVar[str | None]
    __hints__: ClassVar[dict[str, Any]]
    __relationships_resolved__: ClassVar[bool]

    # Instance attributes for relationship state
    _loaded_relationships: dict[str, Any]
    _session: Any  # Reference to session for lazy loading

    def __init__(self, **kwargs: Any) -> None:
        """Initialize a model instance with the given column values."""
        # Initialize relationship storage
        object.__setattr__(self, "_loaded_relationships", {})
        object.__setattr__(self, "_session", None)

        provided_keys = set(kwargs)
        for key, value in kwargs.items():
            if key in self.__columns__ or key in self.__relationships__:
                setattr(self, key, value)
            else:
                raise TypeError(f"Unknown column or relationship: {key}")

        # Set defaults only for columns that were not provided.
        for col_name, col_info in self.__columns__.items():
            if col_name in provided_keys:
                continue
            if col_info.default is not None:
                default = col_info.default() if callable(col_info.default) else col_info.default
                setattr(self, col_name, default)
            elif col_info.nullable:
                setattr(self, col_name, None)
            # For non-nullable columns without defaults (like autoincrement PKs),
            # we don't set anything - they'll be filled by the database

    def __repr__(self) -> str:
        pk = self.__primary_key__
        if pk and hasattr(self, pk):
            pk_val = getattr(self, pk)
            return f"<{self.__class__.__name__} {pk}={pk_val!r}>"
        return f"<{self.__class__.__name__}>"

    def __getattr__(self, name: str) -> Any:
        """Handle access to relationship attributes."""
        # Check if this is a relationship
        if name.startswith("_"):
            # Lazy-init _loaded_relationships if accessed before set
            if name == "_loaded_relationships":
                d: dict[str, Any] = {}
                object.__setattr__(self, "_loaded_relationships", d)
                return d
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

        relationships = type(self).__relationships__
        if name in relationships:
            # Check if already loaded - use try/except for speed
            try:
                loaded = object.__getattribute__(self, "_loaded_relationships")
            except AttributeError:
                loaded = {}
                object.__setattr__(self, "_loaded_relationships", loaded)

            if name in loaded:
                return loaded[name]

            rel_info = relationships[name]

            # Handle different lazy loading strategies
            if rel_info.lazy == "raise":
                raise ValueError(
                    f"Relationship '{name}' is not loaded and lazy='raise'. "
                    "Use eager loading with selectinload() or joinedload()."
                )
            elif rel_info.lazy == "noload":
                return [] if rel_info.uselist else None

            # For M2M relationships with a session attached, return a ManyToManyCollection
            # This allows add/remove/clear operations after insert
            if rel_info.is_many_to_many:
                try:
                    session = object.__getattribute__(self, "_session")
                except AttributeError:
                    session = None

                if session is not None:
                    from ormkit.relationships import ManyToManyCollection
                    collection = ManyToManyCollection(self, rel_info, session, [])
                    loaded[name] = collection
                    return collection
                else:
                    # No session - raise helpful error
                    raise AttributeError(
                        f"M2M relationship '{name}' is not loaded. "
                        "Use eager loading with selectinload() or ensure the instance "
                        "was created via session.insert()."
                    )

            # Async lazy loading is not possible in __getattr__, so raise
            # instead of silently returning empty results
            raise AttributeError(
                f"Relationship '{name}' is not loaded. "
                "Use eager loading with selectinload() or joinedload(), "
                "or set lazy='noload' to explicitly get empty defaults."
            )

        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    def _set_relationship(self, name: str, value: Any, session: Any = None) -> None:
        """Set a loaded relationship value.

        For M2M relationships, wraps the value in ManyToManyCollection if session is provided.
        """
        try:
            loaded = object.__getattribute__(self, "_loaded_relationships")
        except AttributeError:
            loaded = {}
            object.__setattr__(self, "_loaded_relationships", loaded)

        # Check if this is a M2M relationship that should be wrapped
        rel_info = type(self).__relationships__.get(name)
        if (
            rel_info
            and rel_info.is_many_to_many
            and isinstance(value, list)
            and session is not None
        ):
            from ormkit.relationships import ManyToManyCollection
            value = ManyToManyCollection(self, rel_info, session, value)

        loaded[name] = value

    def to_dict(self, include_relationships: bool = False) -> dict[str, Any]:
        """Convert model instance to a dictionary."""
        result = {}
        for col_name in self.__columns__:
            if hasattr(self, col_name):
                result[col_name] = getattr(self, col_name)

        if include_relationships:
            for rel_name in self.__relationships__:
                if rel_name in self._loaded_relationships:
                    rel_value = self._loaded_relationships[rel_name]
                    if isinstance(rel_value, list):
                        result[rel_name] = [item.to_dict() for item in rel_value]
                    elif rel_value is not None:
                        result[rel_name] = rel_value.to_dict()
                    else:
                        result[rel_name] = None

        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Base:
        """Create a model instance from a dictionary."""
        return cls(**{k: v for k, v in data.items() if k in cls.__columns__})

    @classmethod
    def _from_row_fast(cls, data: dict[str, Any]) -> Base:
        """Fast path for creating model from database row - skips validation.

        This is an internal method used by the ORM for bulk result conversion.
        It bypasses the normal __init__ validation for better performance.
        """
        import json as json_module

        instance = object.__new__(cls)
        object.__setattr__(instance, "_loaded_relationships", {})
        object.__setattr__(instance, "_session", None)

        # Set attributes directly without validation
        cols = cls.__columns__
        for key, value in data.items():
            if key in cols:
                col_info = cols[key]
                # Handle JSON deserialization for JSON columns stored as TEXT in SQLite
                if col_info.is_json and isinstance(value, str):
                    try:
                        value = json_module.loads(value)
                    except (json_module.JSONDecodeError, TypeError):
                        pass  # Keep as string if not valid JSON
                object.__setattr__(instance, key, value)

        return instance
