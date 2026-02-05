"""Tests for upsert (INSERT ... ON CONFLICT) operations."""

from __future__ import annotations

import pytest

from ormkit import AsyncSession, Base, Mapped, mapped_column
from ormkit.query import insert


class User(Base):
    """Test model for upsert operations."""

    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    email: Mapped[str] = mapped_column(max_length=255, unique=True)
    name: Mapped[str] = mapped_column(max_length=100)
    age: Mapped[int | None] = mapped_column(nullable=True)


class TeamMember(Base):
    """Test model with composite unique constraint."""

    __tablename__ = "team_members"

    id: Mapped[int] = mapped_column(primary_key=True)
    team_id: Mapped[int]
    user_id: Mapped[int]
    role: Mapped[str] = mapped_column(max_length=50)

    # Composite unique on (team_id, user_id)


@pytest.fixture
async def users_table(sqlite_pool) -> AsyncSession:
    """Create users table for testing."""
    await sqlite_pool.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            age INTEGER
        )
        """,
        [],
    )
    return sqlite_pool


@pytest.fixture
async def team_members_table(sqlite_pool) -> AsyncSession:
    """Create team_members table for testing."""
    await sqlite_pool.execute(
        """
        CREATE TABLE IF NOT EXISTS team_members (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            team_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            role TEXT NOT NULL,
            UNIQUE(team_id, user_id)
        )
        """,
        [],
    )
    return sqlite_pool


class TestUpsertStatement:
    """Test InsertStatement with ON CONFLICT."""

    def test_on_conflict_do_update_single_column(self) -> None:
        """Generate ON CONFLICT (col) DO UPDATE."""
        stmt = (
            insert(User)
            .values(email="a@b.com", name="A")
            .on_conflict_do_update("email", set_={"name": "Updated"})
        )
        sql, params = stmt.to_sql("postgresql")
        assert "ON CONFLICT (email) DO UPDATE SET" in sql
        assert "name" in sql

    def test_on_conflict_do_update_multiple_columns(self) -> None:
        """Generate ON CONFLICT (col1, col2) DO UPDATE."""
        stmt = (
            insert(TeamMember)
            .values(team_id=1, user_id=1, role="member")
            .on_conflict_do_update(["team_id", "user_id"], set_={"role": "admin"})
        )
        sql, _ = stmt.to_sql("postgresql")
        assert "ON CONFLICT (team_id, user_id)" in sql

    def test_on_conflict_do_nothing(self) -> None:
        """Generate ON CONFLICT DO NOTHING."""
        stmt = insert(User).values(email="a@b.com", name="A").on_conflict_do_nothing("email")
        sql, _ = stmt.to_sql("postgresql")
        assert "ON CONFLICT (email) DO NOTHING" in sql

    def test_on_conflict_do_update_all_non_pk_fields(self) -> None:
        """When set_ is None, update all non-PK fields."""
        stmt = (
            insert(User)
            .values(email="a@b.com", name="A", age=25)
            .on_conflict_do_update("email")  # No set_, update all
        )
        sql, _ = stmt.to_sql("postgresql")
        # Should update name and age but not id
        assert "name" in sql
        assert "age" in sql

    def test_sqlite_excluded_syntax(self) -> None:
        """SQLite uses 'excluded' reference."""
        stmt = (
            insert(User)
            .values(email="a@b.com", name="A")
            .on_conflict_do_update("email", set_={"name": "Updated"})
        )
        sql, _ = stmt.to_sql("sqlite")
        # SQLite uses excluded.column_name
        assert "excluded" in sql.lower()

    def test_postgresql_excluded_syntax(self) -> None:
        """PostgreSQL uses 'EXCLUDED' reference."""
        stmt = (
            insert(User)
            .values(email="a@b.com", name="A")
            .on_conflict_do_update("email", set_={"name": "Updated"})
        )
        sql, _ = stmt.to_sql("postgresql")
        # PostgreSQL uses EXCLUDED.column_name
        assert "EXCLUDED" in sql or "excluded" in sql


class TestSessionUpsert:
    """Test session.upsert() and session.upsert_all()."""

    async def test_upsert_inserts_new_record(self, users_table) -> None:
        """Upsert creates record if not exists."""
        session = AsyncSession(users_table)
        user = await session.upsert(
            User(email="new@example.com", name="New"),
            conflict_target="email",
        )
        assert user.id is not None

        # Verify in DB
        found = await session.get(User, user.id)
        assert found is not None
        assert found.email == "new@example.com"

    async def test_upsert_updates_existing_record(self, users_table) -> None:
        """Upsert updates record if exists."""
        session = AsyncSession(users_table)
        # Insert first
        user1 = await session.insert(User(email="test@example.com", name="Original"))

        # Upsert should update
        user2 = await session.upsert(
            User(email="test@example.com", name="Updated"),
            conflict_target="email",
            update_fields=["name"],
        )

        # Should be same record with updated name
        assert user2.id == user1.id
        assert user2.name == "Updated"

    async def test_upsert_returns_with_generated_id(self, users_table) -> None:
        """Upsert returns instance with generated ID."""
        session = AsyncSession(users_table)
        user = await session.upsert(
            User(email="a@b.com", name="A"),
            conflict_target="email",
        )
        assert user.id is not None

    async def test_upsert_preserves_unspecified_fields(self, users_table) -> None:
        """Upsert only updates specified fields."""
        session = AsyncSession(users_table)
        # Insert with age
        user1 = await session.insert(User(email="test@example.com", name="Original", age=25))

        # Upsert only updating name
        user2 = await session.upsert(
            User(email="test@example.com", name="Updated"),
            conflict_target="email",
            update_fields=["name"],
        )

        # Age should be preserved
        loaded = await session.get(User, user1.id)
        assert loaded is not None
        assert loaded.name == "Updated"
        assert loaded.age == 25

    async def test_upsert_all_batch(self, users_table) -> None:
        """Bulk upsert multiple records."""
        session = AsyncSession(users_table)
        users = [
            User(email="a@b.com", name="A"),
            User(email="b@b.com", name="B"),
            User(email="c@b.com", name="C"),
        ]
        results = await session.upsert_all(users, conflict_target="email")
        assert len(results) == 3
        assert all(u.id is not None for u in results)

    async def test_upsert_all_mixed_insert_update(self, users_table) -> None:
        """Bulk upsert with mix of new and existing records."""
        session = AsyncSession(users_table)
        # Pre-insert one
        await session.insert(User(email="existing@b.com", name="Old"))

        users = [
            User(email="existing@b.com", name="Updated"),  # Will update
            User(email="new@b.com", name="New"),  # Will insert
        ]
        await session.upsert_all(users, conflict_target="email", update_fields=["name"])

        # Check both worked
        existing = await session.query(User).filter(email="existing@b.com").first()
        assert existing is not None
        assert existing.name == "Updated"

        new = await session.query(User).filter(email="new@b.com").first()
        assert new is not None
        assert new.name == "New"

    async def test_upsert_with_composite_key(self, team_members_table) -> None:
        """Upsert with multiple conflict columns."""
        session = AsyncSession(team_members_table)

        # First insert
        member1 = await session.upsert(
            TeamMember(team_id=1, user_id=1, role="member"),
            conflict_target=["team_id", "user_id"],
            update_fields=["role"],
        )
        assert member1.id is not None

        # Upsert same team/user should update role
        member2 = await session.upsert(
            TeamMember(team_id=1, user_id=1, role="admin"),
            conflict_target=["team_id", "user_id"],
            update_fields=["role"],
        )

        # Should be same record with updated role
        assert member2.id == member1.id
        assert member2.role == "admin"

    async def test_upsert_do_nothing(self, users_table) -> None:
        """Upsert with do_nothing ignores conflicts."""
        session = AsyncSession(users_table)
        # Insert first
        user1 = await session.insert(User(email="test@example.com", name="Original"))

        # Upsert with do_nothing should not update
        user2 = await session.upsert(
            User(email="test@example.com", name="ShouldNotUpdate"),
            conflict_target="email",
            do_nothing=True,
        )

        # Original should be unchanged
        loaded = await session.get(User, user1.id)
        assert loaded is not None
        assert loaded.name == "Original"


class TestUpsertEdgeCases:
    """Test edge cases for upsert operations."""

    async def test_upsert_with_null_values(self, users_table) -> None:
        """Upsert handles NULL values correctly."""
        session = AsyncSession(users_table)
        user = await session.upsert(
            User(email="test@example.com", name="Test", age=None),
            conflict_target="email",
        )
        assert user.id is not None
        assert user.age is None

    async def test_upsert_updates_to_null(self, users_table) -> None:
        """Upsert can update field to NULL."""
        session = AsyncSession(users_table)
        # Insert with age
        user1 = await session.insert(User(email="test@example.com", name="Test", age=25))

        # Upsert setting age to None
        await session.upsert(
            User(email="test@example.com", name="Test", age=None),
            conflict_target="email",
            update_fields=["age"],
        )

        loaded = await session.get(User, user1.id)
        assert loaded is not None
        assert loaded.age is None

    async def test_upsert_all_empty_list(self, users_table) -> None:
        """Upsert with empty list returns empty list."""
        session = AsyncSession(users_table)
        results = await session.upsert_all([], conflict_target="email")
        assert results == []

    async def test_upsert_all_single_item(self, users_table) -> None:
        """Upsert_all with single item works."""
        session = AsyncSession(users_table)
        results = await session.upsert_all(
            [User(email="single@example.com", name="Single")],
            conflict_target="email",
        )
        assert len(results) == 1
        assert results[0].id is not None


class TestUpsertPostgreSQL:
    """PostgreSQL-specific upsert tests."""

    @pytest.mark.skipif(True, reason="Requires PostgreSQL")
    async def test_upsert_with_returning(self, postgres_pool) -> None:
        """PostgreSQL RETURNING clause with upsert."""
        session = AsyncSession(postgres_pool)
        user = await session.upsert(
            User(email="test@example.com", name="Test"),
            conflict_target="email",
        )
        # Should have all fields populated from RETURNING
        assert user.id is not None
        assert user.email == "test@example.com"
        assert user.name == "Test"

    @pytest.mark.skipif(True, reason="Requires PostgreSQL")
    async def test_upsert_excluded_column_reference(self, postgres_pool) -> None:
        """Access EXCLUDED values in update expression."""
        # This tests expressions like: name = EXCLUDED.name || ' (updated)'
        pass
