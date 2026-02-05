"""Benchmarks for upsert operations."""

import asyncio
import time


async def setup_table(pool) -> None:
    """Create benchmark table."""
    await pool.execute("DROP TABLE IF EXISTS bench_users", [])
    await pool.execute(
        """
        CREATE TABLE bench_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            age INTEGER
        )
        """,
        [],
    )


async def bench_upsert_single_insert(pool, count: int = 1000) -> float:
    """Benchmark single upsert operations (new records).

    Returns time in milliseconds.
    """
    start = time.perf_counter()
    for i in range(count):
        await pool.execute(
            """
            INSERT INTO bench_users (email, name, age) VALUES (?, ?, ?)
            ON CONFLICT (email) DO UPDATE SET name = excluded.name, age = excluded.age
            """,
            [f"user{i}@test.com", f"User {i}", 25 + i % 50],
        )
    elapsed = time.perf_counter() - start
    return elapsed * 1000


async def bench_upsert_single_update(pool, count: int = 1000) -> float:
    """Benchmark single upsert operations (updating existing records).

    Returns time in milliseconds.
    """
    # First, seed the records
    for i in range(count):
        await pool.execute(
            "INSERT INTO bench_users (email, name, age) VALUES (?, ?, ?)",
            [f"update_user{i}@test.com", f"Original {i}", 20],
        )

    start = time.perf_counter()
    for i in range(count):
        await pool.execute(
            """
            INSERT INTO bench_users (email, name, age) VALUES (?, ?, ?)
            ON CONFLICT (email) DO UPDATE SET name = excluded.name, age = excluded.age
            """,
            [f"update_user{i}@test.com", f"Updated {i}", 30 + i % 50],
        )
    elapsed = time.perf_counter() - start
    return elapsed * 1000


async def bench_upsert_batch(pool, batch_size: int = 1000) -> float:
    """Benchmark bulk upsert.

    Returns time in milliseconds.
    """
    # Build multi-value INSERT with ON CONFLICT
    placeholders = ", ".join(["(?, ?, ?)"] * batch_size)
    params = []
    for i in range(batch_size):
        params.extend([f"batch_user{i}@test.com", f"User {i}", 25 + i % 50])

    start = time.perf_counter()
    await pool.execute(
        f"""
        INSERT INTO bench_users (email, name, age) VALUES {placeholders}
        ON CONFLICT (email) DO UPDATE SET name = excluded.name, age = excluded.age
        """,
        params,
    )
    elapsed = time.perf_counter() - start
    return elapsed * 1000


async def bench_get_then_insert(pool, count: int = 1000) -> float:
    """Benchmark manual check-then-insert pattern (for comparison).

    This is the naive approach that upsert replaces.
    Returns time in milliseconds.
    """
    start = time.perf_counter()
    for i in range(count):
        email = f"manual_user{i}@test.com"
        # Check if exists
        result = await pool.execute_query(
            "SELECT id FROM bench_users WHERE email = ?", [email]
        )
        existing = result.first()

        if existing:
            # Update
            await pool.execute(
                "UPDATE bench_users SET name = ?, age = ? WHERE email = ?",
                [f"Manual {i}", 25 + i % 50, email],
            )
        else:
            # Insert
            await pool.execute(
                "INSERT INTO bench_users (email, name, age) VALUES (?, ?, ?)",
                [email, f"Manual {i}", 25 + i % 50],
            )
    elapsed = time.perf_counter() - start
    return elapsed * 1000


async def bench_do_nothing(pool, count: int = 1000) -> float:
    """Benchmark upsert with DO NOTHING (ignore conflicts).

    Returns time in milliseconds.
    """
    # Seed half the records first
    for i in range(count // 2):
        await pool.execute(
            "INSERT INTO bench_users (email, name, age) VALUES (?, ?, ?)",
            [f"nothing_user{i}@test.com", f"Original {i}", 20],
        )

    start = time.perf_counter()
    for i in range(count):
        await pool.execute(
            """
            INSERT INTO bench_users (email, name, age) VALUES (?, ?, ?)
            ON CONFLICT (email) DO NOTHING
            """,
            [f"nothing_user{i}@test.com", f"New {i}", 30],
        )
    elapsed = time.perf_counter() - start
    return elapsed * 1000


async def run_benchmarks() -> None:
    """Run all upsert benchmarks."""
    from ormkit import create_engine

    print("\n" + "=" * 60)
    print("Upsert Benchmarks")
    print("=" * 60)

    pool = await create_engine("sqlite::memory:")

    # Single insert (new records)
    await setup_table(pool)
    print("\n--- Upsert Single Insert (1000 new records) ---")
    elapsed = await bench_upsert_single_insert(pool, 1000)
    print(f"Time: {elapsed:.2f}ms ({1000 / (elapsed / 1000):.0f} ops/sec)")

    # Single update (existing records)
    await setup_table(pool)
    print("\n--- Upsert Single Update (1000 existing records) ---")
    elapsed = await bench_upsert_single_update(pool, 1000)
    print(f"Time: {elapsed:.2f}ms ({1000 / (elapsed / 1000):.0f} ops/sec)")

    # Batch upsert
    await setup_table(pool)
    print("\n--- Batch Upsert (1000 records) ---")
    elapsed = await bench_upsert_batch(pool, 1000)
    print(f"Time: {elapsed:.2f}ms ({1000 / (elapsed / 1000):.0f} ops/sec)")

    # Manual check-then-insert
    await setup_table(pool)
    print("\n--- Manual Get-Then-Insert (1000 new records) ---")
    elapsed = await bench_get_then_insert(pool, 1000)
    print(f"Time: {elapsed:.2f}ms ({1000 / (elapsed / 1000):.0f} ops/sec)")

    # Do nothing
    await setup_table(pool)
    print("\n--- Upsert DO NOTHING (1000 records, 50% existing) ---")
    elapsed = await bench_do_nothing(pool, 1000)
    print(f"Time: {elapsed:.2f}ms ({1000 / (elapsed / 1000):.0f} ops/sec)")

    # Comparison
    print("\n--- Performance Comparison ---")
    await setup_table(pool)
    upsert_time = await bench_upsert_single_insert(pool, 500)

    await setup_table(pool)
    manual_time = await bench_get_then_insert(pool, 500)

    speedup = manual_time / upsert_time
    print(f"Upsert: {upsert_time:.2f}ms")
    print(f"Manual: {manual_time:.2f}ms")
    print(f"Upsert is {speedup:.1f}x faster than manual check-then-insert")

    await pool.close()
    print("\n" + "=" * 60)


if __name__ == "__main__":
    asyncio.run(run_benchmarks())
