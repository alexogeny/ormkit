#!/usr/bin/env python
"""Isolated Piccolo ORM benchmark."""

import asyncio
import sys
import urllib.parse
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))
from helpers import POSTGRES_URL, ROW_COUNT, output_results, timeit


async def main() -> None:
    from piccolo.columns import Float, Integer, Serial, Varchar
    from piccolo.engine.postgres import PostgresEngine
    from piccolo.table import Table

    # Parse URL for Piccolo config
    parsed = urllib.parse.urlparse(POSTGRES_URL)

    engine = PostgresEngine(
        config={
            "host": parsed.hostname or "localhost",
            "port": parsed.port or 5432,
            "user": parsed.username or "postgres",
            "password": parsed.password or "",
            "database": parsed.path.lstrip("/") or "postgres",
        }
    )

    class PicUser(Table, tablename="bench_users", db=engine):
        id = Serial()
        name = Varchar(length=100)
        email = Varchar(length=255)
        age = Integer()
        score = Float()

    results = []

    await engine.start_connection_pool()

    # Note: Table and data should already exist

    # SELECT *
    async def select_all() -> Any:
        return await PicUser.select()

    time_ms = await timeit(select_all)
    results.append({"orm": "Piccolo", "operation": "SELECT *", "rows": ROW_COUNT, "time_ms": time_ms})

    # Single row by ID
    async def select_single() -> Any:
        return await PicUser.select().where(PicUser.id == ROW_COUNT // 2).first()

    time_ms = await timeit(select_single, iterations=200)
    results.append({"orm": "Piccolo", "operation": "SELECT by ID", "rows": 1, "time_ms": time_ms})

    await engine.close_connection_pool()
    output_results(results)


if __name__ == "__main__":
    asyncio.run(main())
