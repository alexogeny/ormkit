#!/usr/bin/env python
"""Isolated aiosqlite benchmark."""

import asyncio
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))
from helpers import ROW_COUNT, output_results, timeit


async def main() -> None:
    import aiosqlite

    results = []
    db = await aiosqlite.connect(":memory:")

    # Setup
    await db.execute("""
        CREATE TABLE IF NOT EXISTS bench_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            age INTEGER,
            score REAL
        )
    """)

    # Insert test data
    data = [
        (f"user{i}", f"user{i}@example.com", 25 + i % 50, 85.5 + i % 15)
        for i in range(ROW_COUNT)
    ]
    await db.executemany(
        "INSERT INTO bench_users (name, email, age, score) VALUES (?, ?, ?, ?)",
        data
    )
    await db.commit()

    # SELECT * - tuples
    async def select_tuples() -> Any:
        async with db.execute("SELECT * FROM bench_users") as cursor:
            return await cursor.fetchall()

    time_ms = await timeit(select_tuples)
    results.append({"orm": "aiosqlite (tuples)", "operation": "SELECT *", "rows": ROW_COUNT, "time_ms": time_ms})

    # SELECT * - Row objects
    db.row_factory = aiosqlite.Row

    async def select_rows() -> Any:
        async with db.execute("SELECT * FROM bench_users") as cursor:
            return await cursor.fetchall()

    time_ms = await timeit(select_rows)
    results.append({"orm": "aiosqlite (Row)", "operation": "SELECT *", "rows": ROW_COUNT, "time_ms": time_ms})

    db.row_factory = None

    # Single row by ID
    async def select_single() -> Any:
        async with db.execute(
            "SELECT * FROM bench_users WHERE id = ?", (ROW_COUNT // 2,)
        ) as cursor:
            return await cursor.fetchone()

    time_ms = await timeit(select_single, iterations=200)
    results.append({"orm": "aiosqlite", "operation": "SELECT by ID", "rows": 1, "time_ms": time_ms})

    await db.close()
    output_results(results)


if __name__ == "__main__":
    asyncio.run(main())
