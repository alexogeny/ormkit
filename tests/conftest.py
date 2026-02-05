"""Pytest configuration and fixtures."""

import os
import pytest
import pytest_asyncio


@pytest_asyncio.fixture
async def sqlite_pool():
    """Create an in-memory SQLite connection pool."""
    from ormkit import create_engine

    pool = await create_engine("sqlite::memory:")
    yield pool
    await pool.close()


@pytest_asyncio.fixture
async def postgres_pool():
    """Create a PostgreSQL connection pool.

    Set DATABASE_URL environment variable to use a real PostgreSQL database.
    Otherwise, this fixture is skipped.
    """
    from ormkit import create_engine

    url = os.environ.get("DATABASE_URL")
    if not url:
        pytest.skip("DATABASE_URL not set")

    pool = await create_engine(url)
    yield pool
    await pool.close()
