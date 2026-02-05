"""Mixins for common model patterns."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from ormkit.fields import Mapped

# Import ColumnInfo at module level so we can use it for mixin columns
from ormkit.fields import ColumnInfo


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

    # The deleted_at column - defined directly as a ColumnInfo
    # so ModelMeta can pick it up when processing subclasses
    deleted_at: Mapped[datetime | None] = ColumnInfo(  # type: ignore[assignment]
        name="deleted_at",
        python_type=datetime,
        nullable=True,
        default=None,
        index=True,  # Index for efficient filtering
    )

    @property
    def is_deleted(self) -> bool:
        """Check if this instance is soft-deleted."""
        from ormkit.fields import ColumnInfo

        # Get the deleted_at value, handling the case where it's still a ColumnInfo
        # (class attribute) rather than an instance value
        try:
            val = object.__getattribute__(self, "deleted_at")
            # If it's a ColumnInfo, treat as not deleted (no instance value set)
            if isinstance(val, ColumnInfo):
                return False
            return val is not None
        except AttributeError:
            return False

    def mark_deleted(self) -> None:
        """Mark this instance as deleted (sets deleted_at to now)."""
        from datetime import datetime

        self.deleted_at = datetime.now(UTC)  # type: ignore[assignment]

    def mark_restored(self) -> None:
        """Restore a soft-deleted instance (clears deleted_at)."""
        self.deleted_at = None  # type: ignore[assignment]
