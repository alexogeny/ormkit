"""Tests for soft delete mixin."""

from __future__ import annotations

from datetime import datetime

import pytest

from ormkit import AsyncSession, Base, Mapped, mapped_column
from ormkit.mixins import SoftDeleteMixin


class Article(Base, SoftDeleteMixin):
    """Test model with soft delete."""

    __tablename__ = "articles"

    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column(max_length=200)


class RegularModel(Base):
    """Test model without soft delete."""

    __tablename__ = "regular_models"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(max_length=100)


class TestSoftDeleteMixinDefinition:
    """Test SoftDeleteMixin model definition."""

    def test_adds_deleted_at_column(self) -> None:
        """Mixin adds deleted_at column."""
        assert "deleted_at" in Article.__columns__
        col = Article.__columns__["deleted_at"]
        assert col.nullable is True

    def test_soft_delete_marker_set(self) -> None:
        """Model has __soft_delete__ = True."""
        assert Article.__soft_delete__ is True

    def test_regular_model_no_soft_delete(self) -> None:
        """Regular model without mixin has no marker."""
        assert not hasattr(RegularModel, "__soft_delete__") or RegularModel.__soft_delete__ is False

    def test_deleted_at_column_is_indexed(self) -> None:
        """deleted_at column should have an index for performance."""
        col = Article.__columns__["deleted_at"]
        assert col.index is True

    def test_is_deleted_property(self) -> None:
        """Instance has is_deleted property."""
        article = Article(title="Test")
        assert article.is_deleted is False

        article.deleted_at = datetime.utcnow()
        assert article.is_deleted is True


@pytest.fixture
async def articles_table(sqlite_pool) -> AsyncSession:
    """Create articles table for testing."""
    await sqlite_pool.execute(
        """
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            deleted_at TIMESTAMP
        )
        """,
        [],
    )
    return sqlite_pool


@pytest.fixture
async def regular_table(sqlite_pool) -> AsyncSession:
    """Create regular_models table for testing."""
    await sqlite_pool.execute(
        """
        CREATE TABLE IF NOT EXISTS regular_models (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL
        )
        """,
        [],
    )
    return sqlite_pool


class TestSoftDeleteQueries:
    """Test querying with soft delete."""

    async def test_query_excludes_deleted_by_default(self, articles_table) -> None:
        """Normal queries exclude soft-deleted records."""
        session = AsyncSession(articles_table)
        a1 = await session.insert(Article(title="Visible"))
        a2 = await session.insert(Article(title="Deleted"))
        await session.soft_delete(a2)

        articles = await session.query(Article).all()
        assert len(articles) == 1
        assert articles[0].title == "Visible"

    async def test_with_deleted_includes_all(self, articles_table) -> None:
        """with_deleted() includes soft-deleted records."""
        session = AsyncSession(articles_table)
        await session.insert(Article(title="Visible"))
        a2 = await session.insert(Article(title="Deleted"))
        await session.soft_delete(a2)

        articles = await session.query(Article).with_deleted().all()
        assert len(articles) == 2

    async def test_only_deleted_returns_deleted_only(self, articles_table) -> None:
        """only_deleted() returns only soft-deleted records."""
        session = AsyncSession(articles_table)
        await session.insert(Article(title="Visible"))
        a2 = await session.insert(Article(title="Deleted"))
        await session.soft_delete(a2)

        articles = await session.query(Article).only_deleted().all()
        assert len(articles) == 1
        assert articles[0].title == "Deleted"

    async def test_filter_works_with_soft_delete(self, articles_table) -> None:
        """Filters combined with soft delete exclusion."""
        session = AsyncSession(articles_table)
        await session.insert(Article(title="Python"))
        await session.insert(Article(title="Rust"))
        deleted = await session.insert(Article(title="Python Deleted"))
        await session.soft_delete(deleted)

        articles = await session.query(Article).filter(title__like="Python%").all()
        assert len(articles) == 1
        assert articles[0].title == "Python"

    async def test_get_excludes_deleted(self, articles_table) -> None:
        """session.get() excludes soft-deleted by default."""
        session = AsyncSession(articles_table)
        article = await session.insert(Article(title="ToDelete"))
        article_id = article.id
        await session.soft_delete(article)

        # get() should return None for deleted
        result = await session.get(Article, article_id)
        assert result is None

    async def test_get_with_include_deleted(self, articles_table) -> None:
        """session.get() can include deleted with flag."""
        session = AsyncSession(articles_table)
        article = await session.insert(Article(title="ToDelete"))
        article_id = article.id
        await session.soft_delete(article)

        # get with include_deleted
        result = await session.get(Article, article_id, include_deleted=True)
        assert result is not None
        assert result.is_deleted is True

    async def test_count_excludes_deleted(self, articles_table) -> None:
        """count() excludes soft-deleted records."""
        session = AsyncSession(articles_table)
        await session.insert(Article(title="Visible"))
        deleted = await session.insert(Article(title="Deleted"))
        await session.soft_delete(deleted)

        count = await session.query(Article).count()
        assert count == 1

    async def test_exists_excludes_deleted(self, articles_table) -> None:
        """exists() excludes soft-deleted records."""
        session = AsyncSession(articles_table)
        article = await session.insert(Article(title="OnlyOne"))
        await session.soft_delete(article)

        exists = await session.query(Article).exists()
        assert exists is False

    async def test_first_excludes_deleted(self, articles_table) -> None:
        """first() excludes soft-deleted records."""
        session = AsyncSession(articles_table)
        deleted = await session.insert(Article(title="Deleted"))
        await session.soft_delete(deleted)
        visible = await session.insert(Article(title="Visible"))

        result = await session.query(Article).first()
        assert result is not None
        assert result.id == visible.id

    async def test_order_by_with_soft_delete(self, articles_table) -> None:
        """order_by() works with soft delete filter."""
        session = AsyncSession(articles_table)
        await session.insert(Article(title="B"))
        await session.insert(Article(title="A"))
        deleted = await session.insert(Article(title="C"))
        await session.soft_delete(deleted)

        articles = await session.query(Article).order_by("title").all()
        assert len(articles) == 2
        assert articles[0].title == "A"
        assert articles[1].title == "B"


class TestSoftDeleteOperations:
    """Test soft delete/restore operations."""

    async def test_soft_delete_sets_deleted_at(self, articles_table) -> None:
        """soft_delete() sets deleted_at timestamp."""
        session = AsyncSession(articles_table)
        article = await session.insert(Article(title="Test"))
        assert article.deleted_at is None

        await session.soft_delete(article)

        assert article.deleted_at is not None
        assert article.is_deleted is True

    async def test_soft_delete_timestamp_is_current(self, articles_table) -> None:
        """soft_delete() sets deleted_at to current time."""
        from datetime import UTC

        session = AsyncSession(articles_table)
        article = await session.insert(Article(title="Test"))

        before = datetime.now(UTC)
        await session.soft_delete(article)
        after = datetime.now(UTC)

        assert article.deleted_at is not None
        # Make both timezone-aware for comparison
        deleted_at = article.deleted_at
        if deleted_at.tzinfo is None:
            deleted_at = deleted_at.replace(tzinfo=UTC)
        assert before <= deleted_at <= after

    async def test_restore_clears_deleted_at(self, articles_table) -> None:
        """restore() clears deleted_at timestamp."""
        session = AsyncSession(articles_table)
        article = await session.insert(Article(title="Test"))
        await session.soft_delete(article)
        assert article.is_deleted is True

        await session.restore(article)

        assert article.deleted_at is None
        assert article.is_deleted is False

        # Verify in DB
        loaded = await session.get(Article, article.id)
        assert loaded is not None
        assert loaded.deleted_at is None

    async def test_force_delete_removes_permanently(self, articles_table) -> None:
        """force_delete() permanently removes record."""
        session = AsyncSession(articles_table)
        article = await session.insert(Article(title="Test"))
        article_id = article.id

        await session.force_delete(article)

        # Should not exist at all
        result = await session.get(Article, article_id, include_deleted=True)
        assert result is None

    async def test_soft_delete_on_non_mixin_raises(self, regular_table) -> None:
        """soft_delete() on model without mixin raises TypeError."""
        session = AsyncSession(regular_table)
        instance = await session.insert(RegularModel(name="Test"))

        with pytest.raises(TypeError):
            await session.soft_delete(instance)

    async def test_restore_on_non_mixin_raises(self, regular_table) -> None:
        """restore() on model without mixin raises TypeError."""
        session = AsyncSession(regular_table)
        instance = await session.insert(RegularModel(name="Test"))

        with pytest.raises(TypeError):
            await session.restore(instance)

    async def test_soft_delete_idempotent(self, articles_table) -> None:
        """soft_delete() on already deleted record is idempotent."""
        session = AsyncSession(articles_table)
        article = await session.insert(Article(title="Test"))
        await session.soft_delete(article)
        first_deleted_at = article.deleted_at

        # Delete again
        await session.soft_delete(article)

        # Should still be deleted (timestamp may or may not change)
        assert article.is_deleted is True
        # We allow the timestamp to be updated or stay the same

    async def test_restore_non_deleted_is_noop(self, articles_table) -> None:
        """restore() on non-deleted record is a no-op."""
        session = AsyncSession(articles_table)
        article = await session.insert(Article(title="Test"))
        assert article.deleted_at is None

        # Restore should be safe to call
        await session.restore(article)

        assert article.deleted_at is None
        assert article.is_deleted is False


class TestSoftDeleteEdgeCases:
    """Test edge cases with soft delete."""

    async def test_multiple_soft_deletes(self, articles_table) -> None:
        """Multiple soft-deleted records are all excluded."""
        session = AsyncSession(articles_table)
        for i in range(5):
            article = await session.insert(Article(title=f"Article {i}"))
            if i % 2 == 0:
                await session.soft_delete(article)

        articles = await session.query(Article).all()
        assert len(articles) == 2  # Only odd indices

    async def test_soft_delete_with_pagination(self, articles_table) -> None:
        """Pagination works correctly with soft delete."""
        session = AsyncSession(articles_table)
        # Create 10 articles, delete 5
        for i in range(10):
            article = await session.insert(Article(title=f"Article {i}"))
            if i < 5:
                await session.soft_delete(article)

        # Get with limit/offset
        page = await session.query(Article).limit(2).offset(1).all()
        assert len(page) == 2

    async def test_soft_delete_bulk_operation(self, articles_table) -> None:
        """Bulk operations respect soft delete."""
        session = AsyncSession(articles_table)
        for i in range(5):
            article = await session.insert(Article(title=f"Article {i}"))
            await session.soft_delete(article)

        # Bulk delete should only affect non-deleted (none in this case)
        deleted_count = await session.query(Article).filter(title__like="Article%").delete()
        assert deleted_count == 0

        # With include deleted, should delete all
        deleted_count = (
            await session.query(Article).with_deleted().filter(title__like="Article%").delete()
        )
        assert deleted_count == 5


class TestSoftDeleteWithRelationships:
    """Test soft delete with relationships."""

    # These tests are stubs - will be implemented after relationships work

    async def test_soft_deleted_parent_excluded_in_join(self, articles_table) -> None:
        """Soft-deleted parent excluded when joining."""
        # e.g., User (soft-deleted) -> Posts
        # Loading posts with author should handle deleted authors
        pass

    async def test_cascade_soft_delete(self, articles_table) -> None:
        """Optional: cascade soft delete to children."""
        # Stretch goal
        pass
