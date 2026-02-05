"""Tests for async session with SQLite."""

import pytest
from ormkit import AsyncSession, Base, Mapped, mapped_column, select, insert


class User(Base):
    __tablename__ = "test_users"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(max_length=100)
    email: Mapped[str] = mapped_column(unique=True)


@pytest.fixture
async def session_with_table(sqlite_pool):
    """Create session with test table."""
    # Create table
    await sqlite_pool.execute("""
        CREATE TABLE IF NOT EXISTS test_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL
        )
    """)

    async with AsyncSession(sqlite_pool) as session:
        yield session

    # Cleanup
    await sqlite_pool.execute("DROP TABLE IF EXISTS test_users")


@pytest.mark.asyncio
async def test_session_add_and_commit(session_with_table):
    """Test adding and committing a single model."""
    session = session_with_table

    user = User(name="Alice", email="alice@example.com")
    session.add(user)
    await session.commit()

    # Verify insertion
    result = await session.execute(select(User))
    users = result.scalars().all()
    assert len(users) == 1
    assert users[0].name == "Alice"
    assert users[0].email == "alice@example.com"


@pytest.mark.asyncio
async def test_session_add_all(session_with_table):
    """Test adding multiple models."""
    session = session_with_table

    users = [
        User(name="Alice", email="alice@example.com"),
        User(name="Bob", email="bob@example.com"),
        User(name="Charlie", email="charlie@example.com"),
    ]
    session.add_all(users)
    await session.commit()

    result = await session.execute(select(User))
    fetched = result.scalars().all()
    assert len(fetched) == 3


@pytest.mark.asyncio
async def test_session_select_filter_by(session_with_table):
    """Test select with filter_by."""
    session = session_with_table

    users = [
        User(name="Alice", email="alice@example.com"),
        User(name="Bob", email="bob@example.com"),
    ]
    session.add_all(users)
    await session.commit()

    stmt = select(User).filter_by(name="Alice")
    result = await session.execute(stmt)
    user = result.scalars().first()
    assert user is not None
    assert user.name == "Alice"


@pytest.mark.asyncio
async def test_session_select_one(session_with_table):
    """Test select one result."""
    session = session_with_table

    user = User(name="Alice", email="alice@example.com")
    session.add(user)
    await session.commit()

    stmt = select(User).filter_by(name="Alice")
    result = await session.execute(stmt)
    user = result.scalars().one()
    assert user.name == "Alice"


@pytest.mark.asyncio
async def test_session_select_one_or_none(session_with_table):
    """Test select one_or_none."""
    session = session_with_table

    # Empty result
    stmt = select(User).filter_by(name="Nonexistent")
    result = await session.execute(stmt)
    user = result.scalars().one_or_none()
    assert user is None

    # Add a user
    session.add(User(name="Alice", email="alice@example.com"))
    await session.commit()

    stmt = select(User).filter_by(name="Alice")
    result = await session.execute(stmt)
    user = result.scalars().one_or_none()
    assert user is not None
    assert user.name == "Alice"


@pytest.mark.asyncio
async def test_session_raw_execute(sqlite_pool):
    """Test raw SQL execution."""
    # Create and insert directly
    await sqlite_pool.execute("""
        CREATE TABLE IF NOT EXISTS raw_test (
            id INTEGER PRIMARY KEY,
            value TEXT
        )
    """)

    await sqlite_pool.execute(
        "INSERT INTO raw_test (value) VALUES (?)",
        ["hello"]
    )

    result = await sqlite_pool.execute("SELECT * FROM raw_test")
    rows = result.all()
    assert len(rows) == 1
    assert rows[0]["value"] == "hello"

    await sqlite_pool.execute("DROP TABLE raw_test")


@pytest.mark.asyncio
async def test_session_rollback(session_with_table):
    """Test rollback discards pending changes."""
    session = session_with_table

    user = User(name="Alice", email="alice@example.com")
    session.add(user)
    await session.rollback()

    result = await session.execute(select(User))
    users = result.scalars().all()
    assert len(users) == 0


@pytest.mark.asyncio
async def test_session_context_manager_rollback_on_error(sqlite_pool):
    """Test context manager rolls back on exception."""
    await sqlite_pool.execute("""
        CREATE TABLE IF NOT EXISTS ctx_test (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL
        )
    """)

    class ContextUser(Base):
        __tablename__ = "ctx_test"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column()
        email: Mapped[str] = mapped_column(unique=True)

    try:
        async with AsyncSession(sqlite_pool) as session:
            session.add(ContextUser(name="Alice", email="alice@example.com"))
            raise ValueError("Simulated error")
    except ValueError:
        pass

    # Verify nothing was committed
    result = await sqlite_pool.execute("SELECT * FROM ctx_test")
    rows = result.all()
    assert len(rows) == 0

    await sqlite_pool.execute("DROP TABLE ctx_test")
