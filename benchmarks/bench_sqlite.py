"""SQLite benchmarks comparing OrmKit vs aiosqlite."""

import asyncio
import time
from typing import Any

import pytest


# Number of rows for bulk operations
BULK_SIZES = [100, 1000, 10000]


async def setup_ormkit_table(pool):
    """Create benchmark table using ormkit."""
    await pool.execute("""
        DROP TABLE IF EXISTS bench_users
    """)
    await pool.execute("""
        CREATE TABLE bench_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            age INTEGER,
            score REAL
        )
    """)


async def setup_aiosqlite_table(db):
    """Create benchmark table using aiosqlite."""
    await db.execute("DROP TABLE IF EXISTS bench_users")
    await db.execute("""
        CREATE TABLE bench_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            age INTEGER,
            score REAL
        )
    """)
    await db.commit()


class TestForeignKeyInsert:
    """Benchmark ormkit insert operations."""

    @pytest.fixture
    async def pool(self, sqlite_url):
        """Create ormkit pool."""
        from ormkit import create_engine
        pool = await create_engine(sqlite_url)
        await setup_ormkit_table(pool)
        yield pool
        await pool.close()

    @pytest.mark.benchmark
    @pytest.mark.parametrize("count", [1, 10, 100])
    async def test_single_inserts(self, pool, count, benchmark):
        """Benchmark single row inserts."""
        async def run():
            for i in range(count):
                await pool.execute(
                    "INSERT INTO bench_users (name, email, age, score) VALUES (?, ?, ?, ?)",
                    [f"user{i}", f"user{i}@example.com", 25 + i % 50, 85.5 + i % 15]
                )

        benchmark(lambda: asyncio.get_event_loop().run_until_complete(run()))

    @pytest.mark.benchmark
    @pytest.mark.parametrize("count", BULK_SIZES)
    async def test_bulk_insert(self, pool, count, benchmark):
        """Benchmark bulk inserts using multi-value INSERT."""
        async def run():
            # Build multi-value INSERT
            placeholders = ", ".join(["(?, ?, ?, ?)"] * count)
            params = []
            for i in range(count):
                params.extend([f"user{i}", f"user{i}@example.com", 25 + i % 50, 85.5 + i % 15])

            await pool.execute(
                f"INSERT INTO bench_users (name, email, age, score) VALUES {placeholders}",
                params
            )

        benchmark(lambda: asyncio.get_event_loop().run_until_complete(run()))


class TestForeignKeySelect:
    """Benchmark ormkit select operations."""

    @pytest.fixture
    async def pool_with_data(self, sqlite_url):
        """Create ormkit pool with test data."""
        from ormkit import create_engine
        pool = await create_engine(sqlite_url)
        await setup_ormkit_table(pool)

        # Insert 1000 rows
        placeholders = ", ".join(["(?, ?, ?, ?)"] * 1000)
        params = []
        for i in range(1000):
            params.extend([f"user{i}", f"user{i}@example.com", 25 + i % 50, 85.5 + i % 15])
        await pool.execute(
            f"INSERT INTO bench_users (name, email, age, score) VALUES {placeholders}",
            params
        )

        yield pool
        await pool.close()

    @pytest.mark.benchmark
    async def test_select_all(self, pool_with_data, benchmark):
        """Benchmark selecting all rows."""
        async def run():
            result = await pool_with_data.execute("SELECT * FROM bench_users")
            return result.all()

        benchmark(lambda: asyncio.get_event_loop().run_until_complete(run()))

    @pytest.mark.benchmark
    async def test_select_by_id(self, pool_with_data, benchmark):
        """Benchmark selecting single row by ID."""
        async def run():
            result = await pool_with_data.execute(
                "SELECT * FROM bench_users WHERE id = ?",
                [500]
            )
            return result.first()

        benchmark(lambda: asyncio.get_event_loop().run_until_complete(run()))

    @pytest.mark.benchmark
    async def test_select_with_filter(self, pool_with_data, benchmark):
        """Benchmark selecting with WHERE clause."""
        async def run():
            result = await pool_with_data.execute(
                "SELECT * FROM bench_users WHERE age > ? AND score > ?",
                [30, 90.0]
            )
            return result.all()

        benchmark(lambda: asyncio.get_event_loop().run_until_complete(run()))


# Simple timing-based benchmark that doesn't require pytest-benchmark
async def run_simple_benchmarks():
    """Run simple timing benchmarks without pytest-benchmark."""
    from ormkit import create_engine

    print("\n" + "=" * 60)
    print("ForeignKey SQLite Benchmarks")
    print("=" * 60)

    pool = await create_engine("sqlite::memory:")
    await setup_ormkit_table(pool)

    # Benchmark 1: Single inserts
    print("\n--- Single Inserts (100 rows) ---")
    start = time.perf_counter()
    for i in range(100):
        await pool.execute(
            "INSERT INTO bench_users (name, email, age, score) VALUES (?, ?, ?, ?)",
            [f"user{i}", f"user{i}@example.com", 25 + i % 50, 85.5 + i % 15]
        )
    elapsed = time.perf_counter() - start
    print(f"Time: {elapsed*1000:.2f}ms ({100/elapsed:.0f} rows/sec)")

    # Clear table
    await pool.execute("DELETE FROM bench_users")

    # Benchmark 2: Bulk insert
    for count in [100, 1000, 10000]:
        print(f"\n--- Bulk Insert ({count} rows) ---")
        placeholders = ", ".join(["(?, ?, ?, ?)"] * count)
        params = []
        for i in range(count):
            params.extend([f"user{i}", f"user{i}@example.com", 25 + i % 50, 85.5 + i % 15])

        start = time.perf_counter()
        await pool.execute(
            f"INSERT INTO bench_users (name, email, age, score) VALUES {placeholders}",
            params
        )
        elapsed = time.perf_counter() - start
        print(f"Time: {elapsed*1000:.2f}ms ({count/elapsed:.0f} rows/sec)")

    # Benchmark 3: Select all
    print("\n--- Select All (10000 rows) ---")
    start = time.perf_counter()
    result = await pool.execute("SELECT * FROM bench_users")
    rows = result.all()
    elapsed = time.perf_counter() - start
    print(f"Time: {elapsed*1000:.2f}ms ({len(rows)} rows)")

    # Benchmark 4: Select by ID (100 iterations)
    print("\n--- Select by ID (100 iterations) ---")
    start = time.perf_counter()
    for i in range(100):
        result = await pool.execute("SELECT * FROM bench_users WHERE id = ?", [i + 1])
        result.first()
    elapsed = time.perf_counter() - start
    print(f"Time: {elapsed*1000:.2f}ms ({100/elapsed:.0f} queries/sec)")

    # Benchmark 5: Select with filter
    print("\n--- Select with Filter ---")
    start = time.perf_counter()
    result = await pool.execute(
        "SELECT * FROM bench_users WHERE age > ? AND score > ?",
        [30, 90.0]
    )
    rows = result.all()
    elapsed = time.perf_counter() - start
    print(f"Time: {elapsed*1000:.2f}ms ({len(rows)} rows)")

    await pool.close()
    print("\n" + "=" * 60)


if __name__ == "__main__":
    asyncio.run(run_simple_benchmarks())
