#!/usr/bin/env python
"""Isolated asyncpg benchmark."""

import asyncio
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))
from helpers import POSTGRES_URL, ROW_COUNT, output_results, timeit


async def main() -> None:
    import asyncpg

    results = []
    pool = await asyncpg.create_pool(POSTGRES_URL, min_size=1, max_size=1)
    assert pool is not None

    # Table should be pre-seeded via: python benchmarks/isolated/seed_postgres.py

    # SELECT * - raw
    async def select_raw() -> Any:
        async with pool.acquire() as conn:
            return await conn.fetch("SELECT * FROM bench_users")

    time_ms = await timeit(select_raw)
    results.append({"orm": "asyncpg (raw)", "operation": "SELECT *", "rows": ROW_COUNT, "time_ms": time_ms})

    # SELECT * - with hydration
    @dataclass
    class User:
        id: int
        name: str
        email: str
        age: int
        score: float

    async def select_hydrated() -> Any:
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM bench_users")
            return [User(**dict(r)) for r in rows]

    time_ms = await timeit(select_hydrated)
    results.append({"orm": "asyncpg + hydration", "operation": "SELECT *", "rows": ROW_COUNT, "time_ms": time_ms})

    # Single row by ID
    async def select_single() -> Any:
        async with pool.acquire() as conn:
            return await conn.fetchrow(
                "SELECT * FROM bench_users WHERE id = $1", ROW_COUNT // 2
            )

    time_ms = await timeit(select_single, iterations=200)
    results.append({"orm": "asyncpg", "operation": "SELECT by ID", "rows": 1, "time_ms": time_ms})

    # INSERT single row
    insert_counter = [ROW_COUNT + 1]

    async def insert_single() -> Any:
        i = insert_counter[0]
        insert_counter[0] += 1
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO bench_users (name, email, age, score) VALUES ($1, $2, $3, $4)",
                f"new_user{i}", f"new{i}@example.com", 30, 90.0
            )

    time_ms = await timeit(insert_single, iterations=200)
    results.append({"orm": "asyncpg", "operation": "INSERT single", "rows": 1, "time_ms": time_ms})

    # UPDATE single row
    async def update_single() -> Any:
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE bench_users SET score = score + 0.1 WHERE id = $1",
                ROW_COUNT // 2
            )

    time_ms = await timeit(update_single, iterations=200)
    results.append({"orm": "asyncpg", "operation": "UPDATE single", "rows": 1, "time_ms": time_ms})

    # DELETE single row
    delete_counter = [ROW_COUNT + 1]

    async def delete_single() -> Any:
        i = delete_counter[0]
        delete_counter[0] += 1
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM bench_users WHERE id = $1", i)

    time_ms = await timeit(delete_single, iterations=200)
    results.append({"orm": "asyncpg", "operation": "DELETE single", "rows": 1, "time_ms": time_ms})

    # Transaction: SELECT + UPDATE + SELECT (read-modify-write pattern)
    async def transaction_rmw() -> Any:
        async with pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    "SELECT score FROM bench_users WHERE id = $1", ROW_COUNT // 2
                )
                if row:
                    new_score = row["score"] + 1.0
                    await conn.execute(
                        "UPDATE bench_users SET score = $1 WHERE id = $2",
                        new_score, ROW_COUNT // 2
                    )
                return await conn.fetchrow(
                    "SELECT score FROM bench_users WHERE id = $1", ROW_COUNT // 2
                )

    time_ms = await timeit(transaction_rmw, iterations=100)
    results.append({"orm": "asyncpg", "operation": "Transaction (RMW)", "rows": 1, "time_ms": time_ms})

    # Bulk INSERT in transaction (100 rows)
    bulk_insert_counter = [100000]

    async def bulk_insert_tx() -> Any:
        async with pool.acquire() as conn:
            async with conn.transaction():
                for j in range(100):
                    i = bulk_insert_counter[0]
                    bulk_insert_counter[0] += 1
                    await conn.execute(
                        "INSERT INTO bench_users (name, email, age, score) VALUES ($1, $2, $3, $4)",
                        f"bulk{i}", f"bulk{i}@example.com", 25, 80.0
                    )

    time_ms = await timeit(bulk_insert_tx, iterations=20)
    results.append({"orm": "asyncpg", "operation": "Bulk INSERT (100)", "rows": 100, "time_ms": time_ms})

    await pool.close()
    output_results(results)


if __name__ == "__main__":
    asyncio.run(main())
