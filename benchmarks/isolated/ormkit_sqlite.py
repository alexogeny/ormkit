#!/usr/bin/env python
"""Isolated OrmKit SQLite benchmark."""

import asyncio
import sys
from pathlib import Path
from typing import Any

# Add parent to path for helpers import
sys.path.insert(0, str(Path(__file__).parent))
from helpers import ROW_COUNT, output_results, timeit


async def main() -> None:
    from ormkit import create_engine

    results = []
    pool = await create_engine("sqlite://:memory:")

    # Setup
    await pool.execute("""
        CREATE TABLE IF NOT EXISTS bench_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            age INTEGER,
            score REAL
        )
    """)

    # Insert test data
    batch_size = 200
    for offset in range(0, ROW_COUNT, batch_size):
        batch = min(batch_size, ROW_COUNT - offset)
        placeholders = ", ".join(["(?, ?, ?, ?)"] * batch)
        params: list[Any] = []
        for i in range(batch):
            idx = offset + i
            params.extend([f"user{idx}", f"user{idx}@example.com", 25 + idx % 50, 85.5 + idx % 15])
        await pool.execute(
            f"INSERT INTO bench_users (name, email, age, score) VALUES {placeholders}",
            params
        )

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
        result = await pool.execute("SELECT * FROM bench_users WHERE id = ?", [ROW_COUNT // 2])
        return result.first()

    time_ms = await timeit(select_single, iterations=200)
    results.append({"orm": "OrmKit", "operation": "SELECT by ID", "rows": 1, "time_ms": time_ms})

    await pool.close()
    output_results(results)


if __name__ == "__main__":
    asyncio.run(main())
