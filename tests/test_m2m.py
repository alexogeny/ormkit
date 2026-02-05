"""Tests for many-to-many relationships."""

from __future__ import annotations

import pytest

from ormkit import AsyncSession, Base, Mapped, mapped_column, relationship
from ormkit.relationships import selectinload


class User(Base):
    """Test model for M2M - user side."""

    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(max_length=100)
    roles: Mapped[list["Role"]] = relationship(secondary="user_roles", back_populates="users")


class Role(Base):
    """Test model for M2M - role side."""

    __tablename__ = "roles"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(max_length=50)
    users: Mapped[list["User"]] = relationship(secondary="user_roles", back_populates="roles")


@pytest.fixture
async def m2m_tables(sqlite_pool) -> AsyncSession:
    """Create tables for M2M testing."""
    # Users table
    await sqlite_pool.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL
        )
        """,
        [],
    )

    # Roles table
    await sqlite_pool.execute(
        """
        CREATE TABLE IF NOT EXISTS roles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL
        )
        """,
        [],
    )

    # Junction table
    await sqlite_pool.execute(
        """
        CREATE TABLE IF NOT EXISTS user_roles (
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            role_id INTEGER NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
            PRIMARY KEY (user_id, role_id)
        )
        """,
        [],
    )

    return sqlite_pool


@pytest.fixture
async def seeded_m2m_tables(m2m_tables) -> AsyncSession:
    """Create tables with seed data for M2M testing."""
    pool = m2m_tables

    # Insert users
    await pool.execute(
        "INSERT INTO users (name) VALUES ('Alice'), ('Bob'), ('Charlie')", []
    )

    # Insert roles
    await pool.execute(
        "INSERT INTO roles (name) VALUES ('Admin'), ('Editor'), ('Viewer')", []
    )

    # Insert user-role associations
    # Alice: Admin, Editor
    # Bob: Editor
    # Charlie: Viewer
    await pool.execute(
        """
        INSERT INTO user_roles (user_id, role_id) VALUES
            (1, 1), (1, 2),
            (2, 2),
            (3, 3)
        """,
        [],
    )

    return pool


class TestM2MRelationshipDefinition:
    """Test M2M relationship model definition."""

    def test_secondary_table_detected(self) -> None:
        """Relationship with secondary= is detected as M2M."""
        rel = User.__relationships__["roles"]
        assert rel.secondary == "user_roles"

    def test_is_many_to_many_property(self) -> None:
        """Relationship has is_many_to_many = True."""
        rel = User.__relationships__["roles"]
        assert rel.is_many_to_many is True

    def test_back_populates_both_sides(self) -> None:
        """Both sides reference each other."""
        user_rel = User.__relationships__["roles"]
        role_rel = Role.__relationships__["users"]
        assert user_rel.back_populates == "users"
        assert role_rel.back_populates == "roles"

    def test_uselist_true_for_both_sides(self) -> None:
        """Both sides are collections (list)."""
        assert User.__relationships__["roles"].uselist is True
        assert Role.__relationships__["users"].uselist is True


class TestM2MLoading:
    """Test loading M2M relationships."""

    async def test_selectinload_m2m(self, seeded_m2m_tables) -> None:
        """Load M2M via selectinload."""
        session = AsyncSession(seeded_m2m_tables)
        users = await session.query(User).options(selectinload("roles")).all()

        assert len(users) == 3

        # Alice should have 2 roles
        alice = next(u for u in users if u.name == "Alice")
        assert len(alice.roles) == 2
        role_names = {r.name for r in alice.roles}
        assert role_names == {"Admin", "Editor"}

        # Bob should have 1 role
        bob = next(u for u in users if u.name == "Bob")
        assert len(bob.roles) == 1
        assert bob.roles[0].name == "Editor"

        # Charlie should have 1 role
        charlie = next(u for u in users if u.name == "Charlie")
        assert len(charlie.roles) == 1
        assert charlie.roles[0].name == "Viewer"

    async def test_selectinload_m2m_reverse(self, seeded_m2m_tables) -> None:
        """Load M2M from reverse side."""
        session = AsyncSession(seeded_m2m_tables)
        roles = await session.query(Role).options(selectinload("users")).all()

        assert len(roles) == 3

        # Admin should have 1 user (Alice)
        admin = next(r for r in roles if r.name == "Admin")
        assert len(admin.users) == 1
        assert admin.users[0].name == "Alice"

        # Editor should have 2 users (Alice, Bob)
        editor = next(r for r in roles if r.name == "Editor")
        assert len(editor.users) == 2
        user_names = {u.name for u in editor.users}
        assert user_names == {"Alice", "Bob"}

    async def test_lazy_load_m2m_raises_by_default(self, seeded_m2m_tables) -> None:
        """Accessing M2M without eager loading raises."""
        session = AsyncSession(seeded_m2m_tables)
        user = await session.query(User).first()
        assert user is not None

        # Accessing roles without loading should raise
        with pytest.raises((AttributeError, RuntimeError)):
            _ = user.roles

    async def test_m2m_returns_empty_list_when_none(self, m2m_tables) -> None:
        """User with no roles returns empty list."""
        session = AsyncSession(m2m_tables)
        user = await session.insert(User(name="NoRoles"))

        loaded = (
            await session.query(User)
            .options(selectinload("roles"))
            .filter(id=user.id)
            .first()
        )
        assert loaded is not None
        assert loaded.roles == []

    async def test_m2m_with_filter(self, seeded_m2m_tables) -> None:
        """Filter works with M2M eager loading."""
        session = AsyncSession(seeded_m2m_tables)
        users = (
            await session.query(User)
            .options(selectinload("roles"))
            .filter(name__like="A%")
            .all()
        )

        assert len(users) == 1
        assert users[0].name == "Alice"
        assert len(users[0].roles) == 2


class TestM2MModification:
    """Test modifying M2M relationships."""

    async def test_add_to_m2m(self, m2m_tables) -> None:
        """Add item to M2M relationship."""
        session = AsyncSession(m2m_tables)
        user = await session.insert(User(name="TestUser"))
        role = await session.insert(Role(name="TestRole"))

        # Add role to user
        await user.roles.add(role)

        # Verify junction table has entry
        loaded = (
            await session.query(User)
            .options(selectinload("roles"))
            .filter(id=user.id)
            .first()
        )
        assert loaded is not None
        assert len(loaded.roles) == 1
        assert loaded.roles[0].id == role.id

    async def test_add_multiple_to_m2m(self, m2m_tables) -> None:
        """Add multiple items at once."""
        session = AsyncSession(m2m_tables)
        user = await session.insert(User(name="TestUser"))
        role1 = await session.insert(Role(name="Role1"))
        role2 = await session.insert(Role(name="Role2"))

        await user.roles.add(role1, role2)

        loaded = (
            await session.query(User)
            .options(selectinload("roles"))
            .filter(id=user.id)
            .first()
        )
        assert loaded is not None
        assert len(loaded.roles) == 2

    async def test_remove_from_m2m(self, m2m_tables) -> None:
        """Remove item from M2M relationship."""
        session = AsyncSession(m2m_tables)
        user = await session.insert(User(name="TestUser"))
        role = await session.insert(Role(name="TestRole"))

        # Add then remove
        await user.roles.add(role)
        await user.roles.remove(role)

        # Verify
        loaded = (
            await session.query(User)
            .options(selectinload("roles"))
            .filter(id=user.id)
            .first()
        )
        assert loaded is not None
        assert len(loaded.roles) == 0

    async def test_clear_m2m(self, m2m_tables) -> None:
        """Clear all items from M2M relationship."""
        session = AsyncSession(m2m_tables)
        user = await session.insert(User(name="TestUser"))

        # Add multiple roles
        for name in ["Admin", "Editor", "Viewer"]:
            role = await session.insert(Role(name=name))
            await user.roles.add(role)

        # Clear all
        await user.roles.clear()

        loaded = (
            await session.query(User)
            .options(selectinload("roles"))
            .filter(id=user.id)
            .first()
        )
        assert loaded is not None
        assert len(loaded.roles) == 0

    async def test_m2m_bidirectional_sync(self, m2m_tables) -> None:
        """Changes reflect on both sides of M2M."""
        session = AsyncSession(m2m_tables)
        user = await session.insert(User(name="TestUser"))
        role = await session.insert(Role(name="TestRole"))

        await user.roles.add(role)

        # Load role and check users
        loaded_role = (
            await session.query(Role)
            .options(selectinload("users"))
            .filter(id=role.id)
            .first()
        )
        assert loaded_role is not None
        assert len(loaded_role.users) == 1
        assert loaded_role.users[0].id == user.id

    async def test_add_duplicate_is_idempotent(self, m2m_tables) -> None:
        """Adding the same item twice is idempotent."""
        session = AsyncSession(m2m_tables)
        user = await session.insert(User(name="TestUser"))
        role = await session.insert(Role(name="TestRole"))

        await user.roles.add(role)
        await user.roles.add(role)  # Add again

        loaded = (
            await session.query(User)
            .options(selectinload("roles"))
            .filter(id=user.id)
            .first()
        )
        assert loaded is not None
        assert len(loaded.roles) == 1  # Still just one

    async def test_remove_nonexistent_is_noop(self, m2m_tables) -> None:
        """Removing item that isn't associated is a no-op."""
        session = AsyncSession(m2m_tables)
        user = await session.insert(User(name="TestUser"))
        role = await session.insert(Role(name="TestRole"))

        # Remove without adding first - should not raise
        await user.roles.remove(role)

        loaded = (
            await session.query(User)
            .options(selectinload("roles"))
            .filter(id=user.id)
            .first()
        )
        assert loaded is not None
        assert len(loaded.roles) == 0


class TestM2MEdgeCases:
    """Test edge cases for M2M relationships."""

    async def test_m2m_with_ordering(self, seeded_m2m_tables) -> None:
        """M2M with ordering on main query."""
        session = AsyncSession(seeded_m2m_tables)
        users = (
            await session.query(User)
            .options(selectinload("roles"))
            .order_by("name")
            .all()
        )

        names = [u.name for u in users]
        assert names == ["Alice", "Bob", "Charlie"]

    async def test_m2m_with_limit(self, seeded_m2m_tables) -> None:
        """M2M loading with LIMIT on main query."""
        session = AsyncSession(seeded_m2m_tables)
        users = (
            await session.query(User)
            .options(selectinload("roles"))
            .limit(2)
            .all()
        )

        assert len(users) == 2
        # Each user should have their roles loaded
        for user in users:
            assert isinstance(user.roles, list)

    async def test_m2m_self_referential(self) -> None:
        """Self-referential M2M (e.g., followers)."""
        # Stretch goal - test would define a model like:
        # class User:
        #     followers: Mapped[list["User"]] = relationship(secondary="user_followers")
        pass


class TestM2MWithExtraColumns:
    """Test M2M with extra columns in junction table.

    This is a stretch goal - may defer to explicit association object pattern.
    """

    async def test_m2m_junction_with_timestamp(self) -> None:
        """M2M junction table with created_at timestamp."""
        # e.g., user_roles with 'granted_at' timestamp
        pass

    async def test_m2m_junction_with_metadata(self) -> None:
        """M2M junction table with extra metadata."""
        # e.g., order_products with 'quantity' field
        pass
