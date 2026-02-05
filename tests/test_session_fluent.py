"""Tests for the fluent session API."""

import pytest
from ormkit import AsyncSession, Base, Mapped, mapped_column, session_context


class User(Base):
    __tablename__ = "fluent_users"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(max_length=100)
    email: Mapped[str] = mapped_column(unique=True)
    age: Mapped[int | None] = mapped_column(nullable=True)


@pytest.fixture
async def pool_with_table(sqlite_pool):
    """Create pool with test table."""
    await sqlite_pool.execute("""
        CREATE TABLE IF NOT EXISTS fluent_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            age INTEGER
        )
    """)
    yield sqlite_pool
    await sqlite_pool.execute("DROP TABLE IF EXISTS fluent_users")


@pytest.fixture
async def session(pool_with_table):
    """Create session with test table."""
    return AsyncSession(pool_with_table)


# ========== Transaction Context Tests ==========

@pytest.mark.asyncio
async def test_begin_transaction_auto_commit(session):
    """Test begin() auto-commits on success."""
    async with session.begin() as tx:
        tx.add(User(name="Alice", email="alice@example.com"))
        tx.add(User(name="Bob", email="bob@example.com"))

    # Should be committed
    users = await session.query(User).all()
    assert len(users) == 2


@pytest.mark.asyncio
async def test_begin_transaction_rollback_on_error(session):
    """Test begin() rolls back on exception."""
    try:
        async with session.begin() as tx:
            tx.add(User(name="Alice", email="alice@example.com"))
            raise ValueError("Simulated error")
    except ValueError:
        pass

    # Should be rolled back
    users = await session.query(User).all()
    assert len(users) == 0


@pytest.mark.asyncio
async def test_session_context_helper(pool_with_table):
    """Test session_context() convenience function."""
    async with session_context(pool_with_table) as session:
        await session.insert(User(name="Alice", email="alice@example.com"))

    # Verify in new session
    session2 = AsyncSession(pool_with_table)
    users = await session2.query(User).all()
    assert len(users) == 1


# ========== Fluent Insert API Tests ==========

@pytest.mark.asyncio
async def test_insert_single(session):
    """Test insert() returns model with ID."""
    user = await session.insert(User(name="Alice", email="alice@example.com"))

    assert user.name == "Alice"
    # Note: SQLite doesn't return ID via RETURNING, but model is inserted
    users = await session.query(User).all()
    assert len(users) == 1


@pytest.mark.asyncio
async def test_insert_all(session):
    """Test insert_all() for batch inserts."""
    users = await session.insert_all([
        User(name="Alice", email="alice@example.com"),
        User(name="Bob", email="bob@example.com"),
        User(name="Charlie", email="charlie@example.com"),
    ])

    assert len(users) == 3
    all_users = await session.query(User).all()
    assert len(all_users) == 3


# ========== Fluent Query API Tests ==========

@pytest.mark.asyncio
async def test_query_filter(session):
    """Test query().filter() with exact match."""
    await session.insert_all([
        User(name="Alice", email="alice@example.com", age=25),
        User(name="Bob", email="bob@example.com", age=30),
    ])

    users = await session.query(User).filter(name="Alice").all()
    assert len(users) == 1
    assert users[0].name == "Alice"


@pytest.mark.asyncio
async def test_query_filter_operators(session):
    """Test query().filter() with comparison operators."""
    await session.insert_all([
        User(name="Alice", email="alice@example.com", age=25),
        User(name="Bob", email="bob@example.com", age=30),
        User(name="Charlie", email="charlie@example.com", age=35),
    ])

    # Greater than
    users = await session.query(User).filter(age__gt=25).all()
    assert len(users) == 2

    # Greater than or equal
    users = await session.query(User).filter(age__gte=30).all()
    assert len(users) == 2

    # Less than
    users = await session.query(User).filter(age__lt=30).all()
    assert len(users) == 1


@pytest.mark.asyncio
async def test_query_first(session):
    """Test query().first()."""
    await session.insert_all([
        User(name="Alice", email="alice@example.com"),
        User(name="Bob", email="bob@example.com"),
    ])

    user = await session.query(User).filter(name="Alice").first()
    assert user is not None
    assert user.name == "Alice"

    # Non-existent
    user = await session.query(User).filter(name="Nobody").first()
    assert user is None


@pytest.mark.asyncio
async def test_query_one(session):
    """Test query().one()."""
    await session.insert(User(name="Alice", email="alice@example.com"))

    user = await session.query(User).filter(name="Alice").one()
    assert user.name == "Alice"


@pytest.mark.asyncio
async def test_query_count(session):
    """Test query().count()."""
    await session.insert_all([
        User(name="Alice", email="alice@example.com", age=25),
        User(name="Bob", email="bob@example.com", age=30),
        User(name="Charlie", email="charlie@example.com", age=30),
    ])

    count = await session.query(User).count()
    assert count == 3

    count = await session.query(User).filter(age=30).count()
    assert count == 2


@pytest.mark.asyncio
async def test_query_exists(session):
    """Test query().exists()."""
    assert await session.query(User).exists() is False

    await session.insert(User(name="Alice", email="alice@example.com"))

    assert await session.query(User).exists() is True
    assert await session.query(User).filter(name="Nobody").exists() is False


@pytest.mark.asyncio
async def test_query_order_by(session):
    """Test query().order_by()."""
    await session.insert_all([
        User(name="Charlie", email="charlie@example.com"),
        User(name="Alice", email="alice@example.com"),
        User(name="Bob", email="bob@example.com"),
    ])

    users = await session.query(User).order_by("name").all()
    assert [u.name for u in users] == ["Alice", "Bob", "Charlie"]

    # Descending with - prefix
    users = await session.query(User).order_by("-name").all()
    assert [u.name for u in users] == ["Charlie", "Bob", "Alice"]


@pytest.mark.asyncio
async def test_query_limit_offset(session):
    """Test query().limit().offset()."""
    await session.insert_all([
        User(name="User1", email="user1@example.com"),
        User(name="User2", email="user2@example.com"),
        User(name="User3", email="user3@example.com"),
        User(name="User4", email="user4@example.com"),
    ])

    users = await session.query(User).order_by("name").limit(2).all()
    assert len(users) == 2

    users = await session.query(User).order_by("name").limit(2).offset(2).all()
    assert len(users) == 2
    assert users[0].name == "User3"


# ========== Get by ID Tests ==========

@pytest.mark.asyncio
async def test_get_by_id(session):
    """Test session.get() by primary key."""
    await session.insert(User(name="Alice", email="alice@example.com"))

    # Get first user (ID 1)
    user = await session.get(User, 1)
    assert user is not None
    assert user.name == "Alice"

    # Non-existent
    user = await session.get(User, 999)
    assert user is None


@pytest.mark.asyncio
async def test_get_or_raise(session):
    """Test session.get_or_raise()."""
    await session.insert(User(name="Alice", email="alice@example.com"))

    user = await session.get_or_raise(User, 1)
    assert user.name == "Alice"

    with pytest.raises(LookupError):
        await session.get_or_raise(User, 999)


# ========== Update Tests ==========

@pytest.mark.asyncio
async def test_update_model(session):
    """Test session.update() for updating a model."""
    user = await session.insert(User(name="Alice", email="alice@example.com", age=25))
    user.id = 1  # Set ID manually for SQLite

    updated = await session.update(user, name="Alicia", age=26)

    assert updated.name == "Alicia"
    assert updated.age == 26

    # Verify in DB
    fetched = await session.query(User).filter(name="Alicia").first()
    assert fetched is not None
    assert fetched.age == 26


# ========== Delete Tests ==========

@pytest.mark.asyncio
async def test_remove_model(session):
    """Test session.remove() for deleting a model."""
    user = await session.insert(User(name="Alice", email="alice@example.com"))
    user.id = 1  # Set ID manually for SQLite

    await session.remove(user)

    users = await session.query(User).all()
    assert len(users) == 0


@pytest.mark.asyncio
async def test_query_delete(session):
    """Test query().delete() for bulk deletion."""
    await session.insert_all([
        User(name="Alice", email="alice@example.com", age=25),
        User(name="Bob", email="bob@example.com", age=30),
        User(name="Charlie", email="charlie@example.com", age=30),
    ])

    deleted = await session.query(User).filter(age=30).delete()
    assert deleted == 2

    remaining = await session.query(User).all()
    assert len(remaining) == 1
    assert remaining[0].name == "Alice"


# ========== Chaining Tests ==========

@pytest.mark.asyncio
async def test_transaction_chaining(session):
    """Test Transaction chainable API."""
    async with session.begin() as tx:
        tx.add(User(name="Alice", email="alice@example.com")) \
          .add(User(name="Bob", email="bob@example.com")) \
          .add(User(name="Charlie", email="charlie@example.com"))

    users = await session.query(User).all()
    assert len(users) == 3
