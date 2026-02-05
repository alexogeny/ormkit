#!/usr/bin/env python
"""Isolated OrmKit PostgreSQL benchmark."""

import asyncio
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))
from helpers import POSTGRES_URL, ROW_COUNT, output_results, timeit


async def main() -> None:
    from ormkit import create_engine

    results = []
    pool = await create_engine(POSTGRES_URL, min_connections=1, max_connections=1)

    # Table should be pre-seeded via: python benchmarks/isolated/seed_postgres.py

    # SELECT * - raw (no Python conversion)
    async def select_raw() -> Any:
        result = await pool.execute("SELECT * FROM bench_users")
        return result.rowcount  # Just return count, don't convert to Python

    time_ms = await timeit(select_raw)
    results.append({"orm": "OrmKit (raw)", "operation": "SELECT *", "rows": ROW_COUNT, "time_ms": time_ms})

    # SELECT * - tuples
    async def select_tuples() -> Any:
        result = await pool.execute("SELECT * FROM bench_users")
        return result.tuples()

    time_ms = await timeit(select_tuples)
    results.append({"orm": "OrmKit (tuples)", "operation": "SELECT *", "rows": ROW_COUNT, "time_ms": time_ms})

    # SELECT * - dicts
    async def select_dicts() -> Any:
        result = await pool.execute("SELECT * FROM bench_users")
        return result.all()

    time_ms = await timeit(select_dicts)
    results.append({"orm": "OrmKit (dicts)", "operation": "SELECT *", "rows": ROW_COUNT, "time_ms": time_ms})

    # Single row by ID
    async def select_single() -> Any:
        result = await pool.execute("SELECT * FROM bench_users WHERE id = $1", [ROW_COUNT // 2])
        return result.first()

    time_ms = await timeit(select_single, iterations=200)
    results.append({"orm": "OrmKit", "operation": "SELECT by ID", "rows": 1, "time_ms": time_ms})

    # INSERT single row
    insert_counter = [ROW_COUNT + 1]
    async def insert_single() -> Any:
        i = insert_counter[0]
        insert_counter[0] += 1
        await pool.execute(
            "INSERT INTO bench_users (name, email, age, score) VALUES ($1, $2, $3, $4)",
            [f"new_user{i}", f"new{i}@example.com", 30, 90.0]
        )

    time_ms = await timeit(insert_single, iterations=200)
    results.append({"orm": "OrmKit", "operation": "INSERT single", "rows": 1, "time_ms": time_ms})

    # UPDATE single row
    async def update_single() -> Any:
        await pool.execute(
            "UPDATE bench_users SET score = score + 0.1 WHERE id = $1",
            [ROW_COUNT // 2]
        )

    time_ms = await timeit(update_single, iterations=200)
    results.append({"orm": "OrmKit", "operation": "UPDATE single", "rows": 1, "time_ms": time_ms})

    # DELETE single row
    delete_counter = [ROW_COUNT + 1]

    async def delete_single() -> Any:
        i = delete_counter[0]
        delete_counter[0] += 1
        await pool.execute("DELETE FROM bench_users WHERE id = $1", [i])

    time_ms = await timeit(delete_single, iterations=200)
    results.append({"orm": "OrmKit", "operation": "DELETE single", "rows": 1, "time_ms": time_ms})

    # Transaction: SELECT + UPDATE + SELECT (read-modify-write pattern)
    # Pre-warm the specific queries used in transaction to ensure fair comparison
    await pool.execute("SELECT score FROM bench_users WHERE id = $1", [ROW_COUNT // 2])
    await pool.execute("UPDATE bench_users SET score = $1 WHERE id = $2", [1.0, ROW_COUNT // 2])

    async def transaction_rmw() -> Any:
        async with await pool.transaction() as tx:
            result = await tx.execute(
                "SELECT score FROM bench_users WHERE id = $1", [ROW_COUNT // 2]
            )
            row = result.first()
            if row:
                new_score = row["score"] + 1.0
                await tx.execute(
                    "UPDATE bench_users SET score = $1 WHERE id = $2",
                    [new_score, ROW_COUNT // 2]
                )
            result = await tx.execute(
                "SELECT score FROM bench_users WHERE id = $1", [ROW_COUNT // 2]
            )
            return result.first()

    time_ms = await timeit(transaction_rmw, iterations=100)
    results.append({"orm": "OrmKit", "operation": "Transaction (RMW)", "rows": 1, "time_ms": time_ms})

    # Bulk INSERT in transaction (100 rows) - using multi-value INSERT
    bulk_insert_counter = [100000]

    async def bulk_insert_tx() -> Any:
        async with await pool.transaction() as tx:
            start_i = bulk_insert_counter[0]
            bulk_insert_counter[0] += 100
            # Build multi-value INSERT
            values_parts = []
            params = []
            for j in range(100):
                idx = start_i + j
                base = j * 4
                values_parts.append(f"(${base+1}, ${base+2}, ${base+3}, ${base+4})")
                params.extend([f"bulk{idx}", f"bulk{idx}@example.com", 25, 80.0])
            sql = f"INSERT INTO bench_users (name, email, age, score) VALUES {','.join(values_parts)}"
            await tx.execute(sql, params)

    time_ms = await timeit(bulk_insert_tx, iterations=20)
    results.append({"orm": "OrmKit", "operation": "Bulk INSERT (100)", "rows": 100, "time_ms": time_ms})

    await pool.close()
    output_results(results)


if __name__ == "__main__":
    asyncio.run(main())
