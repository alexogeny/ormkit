"""Benchmarks for soft delete operations."""

import asyncio
import time


async def setup_tables(pool) -> None:
    """Create benchmark tables."""
    # Soft delete model table
    await pool.execute("DROP TABLE IF EXISTS bench_articles", [])
    await pool.execute(
        """
        CREATE TABLE bench_articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            deleted_at TIMESTAMP
        )
        """,
        [],
    )

    # Regular model table (for comparison)
    await pool.execute("DROP TABLE IF EXISTS bench_posts", [])
    await pool.execute(
        """
        CREATE TABLE bench_posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL
        )
        """,
        [],
    )


async def seed_data(pool, count: int = 1000, delete_ratio: float = 0.5) -> None:
    """Seed tables with test data.

    Args:
        pool: Database connection pool
        count: Number of records to create
        delete_ratio: Fraction of records to soft-delete
    """
    # Seed soft delete table
    placeholders = ", ".join(["(?, ?)"] * count)
    params = []
    for i in range(count):
        title = f"Article {i}"
        # Soft delete based on ratio
        deleted_at = "'2024-01-01 00:00:00'" if i < int(count * delete_ratio) else "NULL"
        params.extend([title, deleted_at if deleted_at != "NULL" else None])

    # For SQLite, we need to handle NULL differently
    values = []
    for i in range(count):
        title = f"Article {i}"
        if i < int(count * delete_ratio):
            values.append(f"('{title}', '2024-01-01 00:00:00')")
        else:
            values.append(f"('{title}', NULL)")

    await pool.execute(
        f"INSERT INTO bench_articles (title, deleted_at) VALUES {', '.join(values)}", []
    )

    # Seed regular table
    values = [f"('Post {i}')" for i in range(count)]
    await pool.execute(
        f"INSERT INTO bench_posts (title) VALUES {', '.join(values)}", []
    )


async def bench_query_with_soft_delete(pool, iterations: int = 100) -> float:
    """Benchmark queries with automatic soft delete filtering.

    Returns time in milliseconds.
    """
    start = time.perf_counter()
    for _ in range(iterations):
        # This simulates the soft delete filter being auto-applied
        result = await pool.execute_query(
            "SELECT * FROM bench_articles WHERE deleted_at IS NULL", []
        )
        _ = result.all()
    elapsed = time.perf_counter() - start
    return elapsed * 1000


async def bench_query_without_soft_delete(pool, iterations: int = 100) -> float:
    """Benchmark queries without soft delete filtering.

    Returns time in milliseconds.
    """
    start = time.perf_counter()
    for _ in range(iterations):
        result = await pool.execute_query("SELECT * FROM bench_posts", [])
        _ = result.all()
    elapsed = time.perf_counter() - start
    return elapsed * 1000


async def bench_query_with_deleted(pool, iterations: int = 100) -> float:
    """Benchmark queries that include deleted records (with_deleted).

    Returns time in milliseconds.
    """
    start = time.perf_counter()
    for _ in range(iterations):
        result = await pool.execute_query("SELECT * FROM bench_articles", [])
        _ = result.all()
    elapsed = time.perf_counter() - start
    return elapsed * 1000


async def bench_query_only_deleted(pool, iterations: int = 100) -> float:
    """Benchmark queries that only return deleted records.

    Returns time in milliseconds.
    """
    start = time.perf_counter()
    for _ in range(iterations):
        result = await pool.execute_query(
            "SELECT * FROM bench_articles WHERE deleted_at IS NOT NULL", []
        )
        _ = result.all()
    elapsed = time.perf_counter() - start
    return elapsed * 1000


async def bench_soft_delete_operation(pool, count: int = 100) -> float:
    """Benchmark soft delete operations.

    Returns time in milliseconds.
    """
    # First, create fresh records to delete
    values = [f"('To Delete {i}', NULL)" for i in range(count)]
    await pool.execute(
        f"INSERT INTO bench_articles (title, deleted_at) VALUES {', '.join(values)}", []
    )

    # Get the IDs of records to soft delete
    result = await pool.execute_query(
        "SELECT id FROM bench_articles WHERE title LIKE 'To Delete%'", []
    )
    ids = [row["id"] for row in result.all()]

    start = time.perf_counter()
    for record_id in ids:
        await pool.execute(
            "UPDATE bench_articles SET deleted_at = datetime('now') WHERE id = ?",
            [record_id],
        )
    elapsed = time.perf_counter() - start
    return elapsed * 1000


async def bench_restore_operation(pool, count: int = 100) -> float:
    """Benchmark restore operations.

    Returns time in milliseconds.
    """
    # Get IDs of deleted records to restore
    result = await pool.execute_query(
        f"SELECT id FROM bench_articles WHERE deleted_at IS NOT NULL LIMIT {count}", []
    )
    ids = [row["id"] for row in result.all()]

    start = time.perf_counter()
    for record_id in ids:
        await pool.execute(
            "UPDATE bench_articles SET deleted_at = NULL WHERE id = ?", [record_id]
        )
    elapsed = time.perf_counter() - start
    return elapsed * 1000


async def run_benchmarks() -> None:
    """Run all soft delete benchmarks."""
    from ormkit import create_engine

    print("\n" + "=" * 60)
    print("Soft Delete Benchmarks")
    print("=" * 60)

    pool = await create_engine("sqlite::memory:")
    await setup_tables(pool)
    await seed_data(pool, count=1000, delete_ratio=0.5)

    # Query benchmarks
    print("\n--- Query WITH soft delete filter (100 iterations) ---")
    elapsed = await bench_query_with_soft_delete(pool, 100)
    print(f"Time: {elapsed:.2f}ms ({100 / (elapsed / 1000):.0f} queries/sec)")

    print("\n--- Query WITHOUT soft delete filter (100 iterations) ---")
    elapsed = await bench_query_without_soft_delete(pool, 100)
    print(f"Time: {elapsed:.2f}ms ({100 / (elapsed / 1000):.0f} queries/sec)")

    print("\n--- Query with_deleted (no filter, 100 iterations) ---")
    elapsed = await bench_query_with_deleted(pool, 100)
    print(f"Time: {elapsed:.2f}ms ({100 / (elapsed / 1000):.0f} queries/sec)")

    print("\n--- Query only_deleted (100 iterations) ---")
    elapsed = await bench_query_only_deleted(pool, 100)
    print(f"Time: {elapsed:.2f}ms ({100 / (elapsed / 1000):.0f} queries/sec)")

    # Operation benchmarks
    print("\n--- Soft delete operations (100 records) ---")
    elapsed = await bench_soft_delete_operation(pool, 100)
    print(f"Time: {elapsed:.2f}ms ({100 / (elapsed / 1000):.0f} ops/sec)")

    print("\n--- Restore operations (100 records) ---")
    elapsed = await bench_restore_operation(pool, 100)
    print(f"Time: {elapsed:.2f}ms ({100 / (elapsed / 1000):.0f} ops/sec)")

    # Overhead comparison
    print("\n--- Overhead Analysis ---")
    with_filter = await bench_query_with_soft_delete(pool, 100)
    without_filter = await bench_query_without_soft_delete(pool, 100)
    overhead = ((with_filter - without_filter) / without_filter) * 100
    print(f"Soft delete filter overhead: {overhead:.1f}%")

    await pool.close()
    print("\n" + "=" * 60)


if __name__ == "__main__":
    asyncio.run(run_benchmarks())
