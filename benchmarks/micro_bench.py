#!/usr/bin/env python
"""Microbenchmarks to isolate performance bottlenecks."""

import asyncio
import time
from typing import Any

def timeit(name: str, iterations: int = 1):
    """Decorator to time a function."""
    def decorator(fn):
        async def wrapper(*args, **kwargs):
            # Warmup
            for _ in range(min(3, iterations)):
                result = await fn(*args, **kwargs)

            start = time.perf_counter()
            for _ in range(iterations):
                result = await fn(*args, **kwargs)
            elapsed = time.perf_counter() - start

            per_iter = elapsed / iterations * 1000
            print(f"{name}: {per_iter:.3f}ms per iteration ({iterations} iterations)")
            return result
        return wrapper
    return decorator


async def main():
    from ormkit import create_engine, Base, Mapped, mapped_column

    print("=" * 70)
    print("MICROBENCHMARKS - Isolating Bottlenecks")
    print("=" * 70)

    # Define a simple model
    class BenchUser(Base):
        __tablename__ = "micro_bench_users"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(max_length=100)
        email: Mapped[str] = mapped_column()
        age: Mapped[int | None] = mapped_column(nullable=True)
        score: Mapped[float | None] = mapped_column(nullable=True)

    pool = await create_engine("sqlite::memory:")

    # Setup
    await pool.execute("DROP TABLE IF EXISTS micro_bench_users")
    await pool.execute("""
        CREATE TABLE micro_bench_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            age INTEGER,
            score REAL
        )
    """)

    # Insert 10k rows
    batch_size = 200
    total = 10000
    for offset in range(0, total, batch_size):
        batch = min(batch_size, total - offset)
        placeholders = ", ".join(["(?, ?, ?, ?)"] * batch)
        params = []
        for i in range(batch):
            idx = offset + i
            params.extend([f"user{idx}", f"user{idx}@example.com", 25 + idx % 50, 85.5 + idx % 15])
        await pool.execute(
            f"INSERT INTO micro_bench_users (name, email, age, score) VALUES {placeholders}",
            params
        )

    print(f"\nSetup complete: {total} rows in table\n")

    # Benchmark 1: Raw query execution (no Python conversion)
    print("--- Stage 1: Raw SQL Execution ---")
    iterations = 50
    start = time.perf_counter()
    for _ in range(iterations):
        result = await pool.execute("SELECT * FROM micro_bench_users")
        # Don't call .all() - just get the QueryResult
        _ = len(result)  # Access rowcount
    elapsed = time.perf_counter() - start
    print(f"Raw query (no conversion): {elapsed/iterations*1000:.3f}ms per query")

    # Benchmark 2: Query + .tuples() (fastest extraction)
    start = time.perf_counter()
    for _ in range(iterations):
        result = await pool.execute("SELECT * FROM micro_bench_users")
        tuples = result.tuples()
    elapsed = time.perf_counter() - start
    print(f"Query + tuples():          {elapsed/iterations*1000:.3f}ms per query")

    # Benchmark 3: Query + .all() (dict conversion)
    start = time.perf_counter()
    for _ in range(iterations):
        result = await pool.execute("SELECT * FROM micro_bench_users")
        dicts = result.all()
    elapsed = time.perf_counter() - start
    print(f"Query + all() (dicts):     {elapsed/iterations*1000:.3f}ms per query")

    # Benchmark 4: Query + to_models (Rust model creation)
    start = time.perf_counter()
    for _ in range(iterations):
        result = await pool.execute("SELECT * FROM micro_bench_users")
        models = list(result.to_models(BenchUser))
    elapsed = time.perf_counter() - start
    print(f"Query + to_models (Rust):  {elapsed/iterations*1000:.3f}ms per query")

    # Benchmark 5: Query + Python model creation
    start = time.perf_counter()
    for _ in range(iterations):
        result = await pool.execute("SELECT * FROM micro_bench_users")
        models = [BenchUser._from_row_fast(dict(row)) for row in result.all()]
    elapsed = time.perf_counter() - start
    print(f"Query + Python models:     {elapsed/iterations*1000:.3f}ms per query")

    print("\n--- Stage 2: Conversion Only (pre-fetched data) ---")

    # Pre-fetch data once
    result = await pool.execute("SELECT * FROM micro_bench_users")

    # Benchmark: tuples() alone
    iterations = 200
    start = time.perf_counter()
    for _ in range(iterations):
        _ = result.tuples()
    elapsed = time.perf_counter() - start
    print(f"tuples() conversion:       {elapsed/iterations*1000:.3f}ms ({len(result)} rows)")

    # Benchmark: all() alone
    start = time.perf_counter()
    for _ in range(iterations):
        _ = result.all()
    elapsed = time.perf_counter() - start
    print(f"all() (dicts) conversion:  {elapsed/iterations*1000:.3f}ms ({len(result)} rows)")

    # Benchmark: to_models() alone
    start = time.perf_counter()
    for _ in range(iterations):
        _ = list(result.to_models(BenchUser))
    elapsed = time.perf_counter() - start
    print(f"to_models() conversion:    {elapsed/iterations*1000:.3f}ms ({len(result)} rows)")

    # Benchmark: Python _from_row_fast
    dicts = result.all()
    start = time.perf_counter()
    for _ in range(iterations):
        _ = [BenchUser._from_row_fast(row) for row in dicts]
    elapsed = time.perf_counter() - start
    print(f"Python _from_row_fast:     {elapsed/iterations*1000:.3f}ms ({len(result)} rows)")

    # Benchmark: Pure Python model creation via __init__
    start = time.perf_counter()
    for _ in range(iterations):
        _ = [BenchUser(**row) for row in dicts]
    elapsed = time.perf_counter() - start
    print(f"Python __init__:           {elapsed/iterations*1000:.3f}ms ({len(result)} rows)")

    print("\n--- Stage 3: Single Row Operations ---")

    # Single row fetch
    iterations = 500
    start = time.perf_counter()
    for i in range(iterations):
        result = await pool.execute("SELECT * FROM micro_bench_users WHERE id = ?", [i % 10000 + 1])
        _ = result.first()
    elapsed = time.perf_counter() - start
    print(f"Single row query + first(): {elapsed/iterations*1000:.3f}ms per query")

    await pool.close()
    print("\n" + "=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
