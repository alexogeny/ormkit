"""Pytest configuration for benchmarks."""

import asyncio
import os
import pytest


def pytest_configure(config):
    """Add benchmark markers."""
    config.addinivalue_line(
        "markers", "benchmark: mark test as a benchmark"
    )


@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for the session."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def pg_url():
    """Get PostgreSQL URL from environment or skip."""
    url = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/benchmark_test")
    return url


@pytest.fixture(scope="session")
def sqlite_url():
    """Get SQLite URL for benchmarks."""
    return "sqlite::memory:"
