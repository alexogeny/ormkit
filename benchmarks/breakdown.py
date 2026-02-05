#!/usr/bin/env python
"""Breakdown where time is spent in OrmKit queries."""

import asyncio
import time


async def main():
    print("=" * 70)
    print("TIME BREAKDOWN ANALYSIS")
    print("=" * 70)

    from ormkit import create_engine

    pool = await create_engine("sqlite::memory:")
    await pool.execute("DROP TABLE IF EXISTS bench")
    await pool.execute("""
        CREATE TABLE bench (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            age INTEGER,
            score REAL
        )
    """)

    # Insert test data
    total = 10000
    batch_size = 200
    for offset in range(0, total, batch_size):
        batch = min(batch_size, total - offset)
        placeholders = ", ".join(["(?, ?, ?, ?)"] * batch)
        params = []
        for i in range(batch):
            idx = offset + i
            params.extend([f"user{idx}", f"user{idx}@example.com", 25 + idx % 50, 85.5 + idx % 15])
        await pool.execute(
            f"INSERT INTO bench (name, email, age, score) VALUES {placeholders}",
            params
        )

    print(f"\n{total} rows in table\n")

    iterations = 100

    # Measure total time
    print("--- Full Query Cycle ---")
    start = time.perf_counter()
    for _ in range(iterations):
        result = await pool.execute("SELECT * FROM bench")
        tuples = result.tuples()
    total_time = (time.perf_counter() - start) / iterations * 1000
    print(f"Total (query + conversion): {total_time:.2f}ms")

    # Measure query only (just get the result object)
    start = time.perf_counter()
    for _ in range(iterations):
        result = await pool.execute("SELECT * FROM bench")
        _ = len(result)  # Just access rowcount
    query_time = (time.perf_counter() - start) / iterations * 1000
    print(f"Query only (no conversion): {query_time:.2f}ms")

    # The difference is conversion time
    conversion_time = total_time - query_time
    print(f"Conversion overhead:        {conversion_time:.2f}ms")

    print(f"\nBreakdown:")
    print(f"  - SQL execution + fetch:  {query_time:.2f}ms ({query_time/total_time*100:.1f}%)")
    print(f"  - Py conversion:          {conversion_time:.2f}ms ({conversion_time/total_time*100:.1f}%)")

    # Compare different conversion methods
    print("\n--- Conversion Methods (same result object) ---")
    result = await pool.execute("SELECT * FROM bench")

    start = time.perf_counter()
    for _ in range(iterations):
        _ = len(result)
    no_conv = (time.perf_counter() - start) / iterations * 1000
    print(f"No conversion (len only):   {no_conv:.3f}ms")

    start = time.perf_counter()
    for _ in range(iterations):
        _ = result.scalars()
    scalars = (time.perf_counter() - start) / iterations * 1000
    print(f"scalars() (first col):      {scalars:.3f}ms")

    start = time.perf_counter()
    for _ in range(iterations):
        _ = result.tuples()
    tuples = (time.perf_counter() - start) / iterations * 1000
    print(f"tuples():                   {tuples:.3f}ms")

    start = time.perf_counter()
    for _ in range(iterations):
        _ = result.all()
    dicts = (time.perf_counter() - start) / iterations * 1000
    print(f"all() (dicts):              {dicts:.3f}ms")

    await pool.close()
    print("\n" + "=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
