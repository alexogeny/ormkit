"""Mixins for common model patterns."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from ormkit.fields import Mapped


class SoftDeleteMixin:
    """Mixin that adds soft delete functionality to models.

    When applied to a model, adds a `deleted_at` column. Records with
    `deleted_at != NULL` are excluded from normal queries by default.

    The mixin provides:
    - `deleted_at` column for tracking deletion time
    - `is_deleted` property to check deletion status
    - Auto-filtering in queries to exclude soft-deleted records
    - `with_deleted()` to include soft-deleted records
    - `only_deleted()` to query only soft-deleted records

    Example:
        >>> from ormkit import Base, Mapped, mapped_column
        >>> from ormkit.mixins import SoftDeleteMixin
        >>>
        >>> class Article(Base, SoftDeleteMixin):
        ...     __tablename__ = "articles"
        ...     id: Mapped[int] = mapped_column(primary_key=True)
        ...     title: Mapped[str]
        >>>
        >>> # Normal queries exclude deleted
        >>> articles = await session.query(Article).all()
        >>>
        >>> # Include deleted records
        >>> all_articles = await session.query(Article).with_deleted().all()
        >>>
        >>> # Only deleted records
        >>> deleted = await session.query(Article).only_deleted().all()
        >>>
        >>> # Soft delete
        >>> await session.soft_delete(article)
        >>>
        >>> # Restore
        >>> await session.restore(article)
        >>>
        >>> # Permanent delete
        >>> await session.force_delete(article)
    """

    # Class variable to track if model uses soft delete
    __soft_delete__: ClassVar[bool] = True

    # The deleted_at column - will be picked up by ModelMeta
    # We use ClassVar annotation here but the actual column is defined
    # in __init_subclass__ to properly integrate with the ORM
    deleted_at: Mapped[datetime | None]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Set up soft delete column when class is created."""
        super().__init_subclass__(**kwargs)

        # Import here to avoid circular imports
        from ormkit.fields import ColumnInfo

        # Add deleted_at column if not already present
        if not hasattr(cls, "__annotations__"):
            cls.__annotations__ = {}

        # Add the type annotation for deleted_at
        cls.__annotations__["deleted_at"] = "Mapped[datetime | None]"

        # Create the column info for deleted_at
        # This will be processed by ModelMeta
        if not hasattr(cls, "deleted_at") or not isinstance(
            getattr(cls, "deleted_at", None), ColumnInfo
        ):
            cls.deleted_at = ColumnInfo(  # type: ignore[assignment]
                name="deleted_at",
                python_type=datetime,
                nullable=True,
                default=None,
                index=True,  # Index for efficient filtering
            )

    @property
    def is_deleted(self) -> bool:
        """Check if this instance is soft-deleted."""
        return getattr(self, "deleted_at", None) is not None

    def mark_deleted(self) -> None:
        """Mark this instance as deleted (sets deleted_at to now)."""
        from datetime import datetime

        self.deleted_at = datetime.now(UTC)  # type: ignore[assignment]

    def mark_restored(self) -> None:
        """Restore a soft-deleted instance (clears deleted_at)."""
        self.deleted_at = None  # type: ignore[assignment]
