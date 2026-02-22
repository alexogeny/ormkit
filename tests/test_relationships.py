"""Tests for foreign key relationships and eager loading."""

from __future__ import annotations

import pytest

from ormkit import (
    AsyncSession,
    Base,
    ForeignKey,
    Mapped,
    create_engine,
    joinedload,
    mapped_column,
    noload,
    relationship,
    selectinload,
)


# Use unique names to avoid conflicts with other test files
class RelUser(Base):
    __tablename__ = "rel_users"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(max_length=100)

    posts: Mapped[list["RelPost"]] = relationship(back_populates="author")


class RelPost(Base):
    __tablename__ = "rel_posts"

    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column(max_length=200)
    author_id: Mapped[int] = mapped_column(ForeignKey("rel_users.id"))

    author: Mapped[RelUser] = relationship(back_populates="posts")


@pytest.fixture
async def engine():
    """Create an in-memory SQLite database."""
    engine = await create_engine("sqlite::memory:")
    # Create tables
    await engine.execute("""
        CREATE TABLE rel_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL
        )
    """, [])
    await engine.execute("""
        CREATE TABLE rel_posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            author_id INTEGER NOT NULL,
            FOREIGN KEY (author_id) REFERENCES rel_users(id)
        )
    """, [])
    return engine


@pytest.fixture
async def session(engine):
    """Create a session with test data."""
    session = AsyncSession(engine)

    # Insert test data
    await engine.execute(
        "INSERT INTO rel_users (name) VALUES (?), (?), (?)",
        ["Alice", "Bob", "Charlie"]
    )
    await engine.execute(
        "INSERT INTO rel_posts (title, author_id) VALUES (?, ?), (?, ?), (?, ?), (?, ?)",
        ["Alice Post 1", 1, "Alice Post 2", 1, "Bob Post 1", 2, "Charlie Post 1", 3]
    )

    return session


class TestRelationshipDefinition:
    """Tests for relationship definition and resolution."""

    def test_relationship_info_stored(self):
        """Relationships should be stored on the model class."""
        assert "posts" in RelUser.__relationships__
        assert "author" in RelPost.__relationships__

    def test_relationship_uselist_inferred(self):
        """uselist should be inferred from type hint."""
        # After resolution
        RelUser._resolve_relationships()
        RelPost._resolve_relationships()

        assert RelUser.__relationships__["posts"].uselist is True
        assert RelPost.__relationships__["author"].uselist is False

    def test_foreign_key_resolution(self):
        """Foreign key columns should be resolved correctly."""
        RelUser._resolve_relationships()
        RelPost._resolve_relationships()

        # RelUser.posts should find RelPost.author_id
        user_rel = RelUser.__relationships__["posts"]
        assert user_rel._local_fk_column == "author_id"
        assert user_rel._remote_pk_column == "id"

        # RelPost.author should find RelPost.author_id -> rel_users.id
        post_rel = RelPost.__relationships__["author"]
        assert post_rel._local_fk_column == "author_id"
        assert post_rel._remote_pk_column == "id"


class TestSelectinLoad:
    """Tests for selectinload eager loading."""

    async def test_selectinload_one_to_many(self, session):
        """selectinload should load one-to-many relationships."""
        users = await session.query(RelUser).options(selectinload("posts")).all()

        assert len(users) == 3

        # Find Alice and check her posts
        alice = next(u for u in users if u.name == "Alice")
        assert len(alice.posts) == 2
        assert all(p.title.startswith("Alice") for p in alice.posts)

        # Bob should have 1 post
        bob = next(u for u in users if u.name == "Bob")
        assert len(bob.posts) == 1

        # Charlie should have 1 post
        charlie = next(u for u in users if u.name == "Charlie")
        assert len(charlie.posts) == 1

    async def test_selectinload_many_to_one(self, session):
        """selectinload should load many-to-one relationships."""
        posts = await session.query(RelPost).options(selectinload("author")).all()

        assert len(posts) == 4

        # Each post should have its author loaded
        for post in posts:
            assert post.author is not None
            if post.title.startswith("Alice"):
                assert post.author.name == "Alice"
            elif post.title.startswith("Bob"):
                assert post.author.name == "Bob"
            else:
                assert post.author.name == "Charlie"

    async def test_selectinload_with_filter(self, session):
        """selectinload should work with filtered queries."""
        users = await session.query(RelUser).filter(name="Alice").options(selectinload("posts")).all()

        assert len(users) == 1
        alice = users[0]
        assert len(alice.posts) == 2


class TestJoinedLoad:
    """Tests for joinedload eager loading."""

    async def test_joinedload_many_to_one(self, session):
        """joinedload should load many-to-one relationships."""
        posts = await session.query(RelPost).options(joinedload("author")).all()

        assert len(posts) == 4
        for post in posts:
            assert post.author is not None


class TestNoLoad:
    """Tests for noload option."""

    async def test_noload_returns_empty(self, session):
        """noload should set empty values without loading."""
        users = await session.query(RelUser).options(noload("posts")).all()

        assert len(users) == 3
        for user in users:
            assert user.posts == []


class TestRelationshipAccess:
    """Tests for accessing relationships on model instances."""

    async def test_unloaded_relationship_raises(self, session):
        """Accessing unloaded relationship should raise AttributeError."""
        users = await session.query(RelUser).all()

        # Without eager loading, accessing posts should raise
        for user in users:
            with pytest.raises(AttributeError, match="not loaded"):
                user.posts

    async def test_loaded_relationship_persists(self, session):
        """Loaded relationships should be accessible after loading."""
        users = await session.query(RelUser).options(selectinload("posts")).all()
        alice = next(u for u in users if u.name == "Alice")

        # Access posts multiple times - should return same data
        posts1 = alice.posts
        posts2 = alice.posts
        assert posts1 == posts2
        assert len(posts1) == 2


class TestToDict:
    """Tests for to_dict with relationships."""

    async def test_to_dict_without_relationships(self, session):
        """to_dict without include_relationships should exclude relationships."""
        users = await session.query(RelUser).options(selectinload("posts")).all()
        alice = next(u for u in users if u.name == "Alice")

        d = alice.to_dict()
        assert "posts" not in d
        assert d["name"] == "Alice"

    async def test_to_dict_with_relationships(self, session):
        """to_dict with include_relationships should include loaded relationships."""
        users = await session.query(RelUser).options(selectinload("posts")).all()
        alice = next(u for u in users if u.name == "Alice")

        d = alice.to_dict(include_relationships=True)
        assert "posts" in d
        assert len(d["posts"]) == 2
        assert all(isinstance(p, dict) for p in d["posts"])
