#!/usr/bin/env python
"""Isolated encode/databases benchmark."""

import asyncio
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))
from helpers import POSTGRES_URL, ROW_COUNT, output_results, timeit


async def main() -> None:
    from databases import Database

    results = []

    # databases uses same URL format
    database = Database(POSTGRES_URL)
    await database.connect()

    # Note: Table and data should already exist

    # SELECT *
    async def select_all() -> Any:
        return await database.fetch_all("SELECT * FROM bench_users")

    time_ms = await timeit(select_all)
    results.append({"orm": "databases", "operation": "SELECT *", "rows": ROW_COUNT, "time_ms": time_ms})

    # Single row by ID
    async def select_single() -> Any:
        return await database.fetch_one(
            "SELECT * FROM bench_users WHERE id = :id",
            {"id": ROW_COUNT // 2},
        )

    time_ms = await timeit(select_single, iterations=200)
    results.append({"orm": "databases", "operation": "SELECT by ID", "rows": 1, "time_ms": time_ms})

    await database.disconnect()
    output_results(results)


if __name__ == "__main__":
    asyncio.run(main())
