#!/usr/bin/env python
"""Compare raw query execution between OrmKit and aiosqlite."""

import asyncio
import time


async def main():
    print("=" * 70)
    print("RAW QUERY COMPARISON: OrmKit vs aiosqlite")
    print("=" * 70)

    # Setup identical databases
    from ormkit import create_engine
    import aiosqlite

    # ForeignKey setup
    fk_pool = await create_engine("sqlite::memory:")
    await fk_pool.execute("DROP TABLE IF EXISTS bench")
    await fk_pool.execute("""
        CREATE TABLE bench (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            age INTEGER,
            score REAL
        )
    """)

    # Insert test data
    batch_size = 200
    total = 10000
    for offset in range(0, total, batch_size):
        batch = min(batch_size, total - offset)
        placeholders = ", ".join(["(?, ?, ?, ?)"] * batch)
        params = []
        for i in range(batch):
            idx = offset + i
            params.extend([f"user{idx}", f"user{idx}@example.com", 25 + idx % 50, 85.5 + idx % 15])
        await fk_pool.execute(
            f"INSERT INTO bench (name, email, age, score) VALUES {placeholders}",
            params
        )

    # aiosqlite setup (same data)
    aio_db = await aiosqlite.connect(":memory:")
    await aio_db.execute("DROP TABLE IF EXISTS bench")
    await aio_db.execute("""
        CREATE TABLE bench (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            age INTEGER,
            score REAL
        )
    """)
    await aio_db.commit()

    # Insert same data
    data = [(f"user{i}", f"user{i}@example.com", 25 + i % 50, 85.5 + i % 15) for i in range(total)]
    await aio_db.executemany(
        "INSERT INTO bench (name, email, age, score) VALUES (?, ?, ?, ?)",
        data
    )
    await aio_db.commit()

    print(f"\nSetup complete: {total} rows in each database\n")

    iterations = 100

    # Test 1: SELECT * (all rows)
    print("--- SELECT * FROM bench (10000 rows) ---")

    # ForeignKey - raw tuples
    start = time.perf_counter()
    for _ in range(iterations):
        result = await fk_pool.execute("SELECT * FROM bench")
        rows = result.tuples()  # Fastest method
    fk_tuples = (time.perf_counter() - start) / iterations * 1000
    print(f"ForeignKey (tuples): {fk_tuples:.2f}ms  [{len(rows)} rows]")

    # ForeignKey - dicts
    start = time.perf_counter()
    for _ in range(iterations):
        result = await fk_pool.execute("SELECT * FROM bench")
        rows = result.all()  # Dict method
    fk_dicts = (time.perf_counter() - start) / iterations * 1000
    print(f"ForeignKey (dicts):  {fk_dicts:.2f}ms  [{len(rows)} rows]")

    # aiosqlite - raw tuples (fetchall)
    start = time.perf_counter()
    for _ in range(iterations):
        async with aio_db.execute("SELECT * FROM bench") as cursor:
            rows = await cursor.fetchall()
    aio_tuples = (time.perf_counter() - start) / iterations * 1000
    print(f"aiosqlite (tuples):  {aio_tuples:.2f}ms  [{len(rows)} rows]")

    # aiosqlite - row_factory dict
    aio_db.row_factory = aiosqlite.Row
    start = time.perf_counter()
    for _ in range(iterations):
        async with aio_db.execute("SELECT * FROM bench") as cursor:
            rows = await cursor.fetchall()
    aio_rows = (time.perf_counter() - start) / iterations * 1000
    print(f"aiosqlite (Row):     {aio_rows:.2f}ms  [{len(rows)} rows]")

    print(f"\nTuples: ForeignKey is {aio_tuples/fk_tuples:.2f}x {'faster' if fk_tuples < aio_tuples else 'slower'}")
    print(f"Dicts:  ForeignKey is {aio_rows/fk_dicts:.2f}x {'faster' if fk_dicts < aio_rows else 'slower'}")

    # Test 2: Single row queries
    print("\n--- Single row query (100 iterations) ---")

    iterations = 500
    start = time.perf_counter()
    for i in range(iterations):
        result = await fk_pool.execute("SELECT * FROM bench WHERE id = ?", [i % 10000 + 1])
        row = result.first()
    fk_single = (time.perf_counter() - start) / iterations * 1000
    print(f"ForeignKey: {fk_single:.3f}ms per query")

    aio_db.row_factory = None
    start = time.perf_counter()
    for i in range(iterations):
        async with aio_db.execute("SELECT * FROM bench WHERE id = ?", (i % 10000 + 1,)) as cursor:
            row = await cursor.fetchone()
    aio_single = (time.perf_counter() - start) / iterations * 1000
    print(f"aiosqlite:  {aio_single:.3f}ms per query")
    print(f"ForeignKey is {aio_single/fk_single:.2f}x {'faster' if fk_single < aio_single else 'slower'}")

    # Cleanup
    await fk_pool.close()
    await aio_db.close()

    print("\n" + "=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
