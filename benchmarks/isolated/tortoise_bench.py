#!/usr/bin/env python
"""Isolated Tortoise ORM benchmark."""

import asyncio
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))
from helpers import POSTGRES_URL, ROW_COUNT, output_results, timeit

# Import Tortoise at module level
from tortoise import Tortoise, fields
from tortoise.models import Model


# Define model at module level for proper registration
class TortUser(Model):
    id = fields.IntField(pk=True)
    name = fields.CharField(max_length=100)
    email = fields.CharField(max_length=255)
    age = fields.IntField()
    score = fields.FloatField()

    class Meta:
        table = "bench_users"


async def main() -> None:
    results = []

    # Convert URL format (Tortoise uses postgres:// not postgresql://)
    tort_url = POSTGRES_URL.replace("postgresql://", "postgres://")

    await Tortoise.init(
        db_url=tort_url,
        modules={"models": [__name__]},
    )

    # Note: Table and data should already exist

    # SELECT *
    async def select_all() -> Any:
        return await TortUser.all()

    time_ms = await timeit(select_all)
    results.append({"orm": "Tortoise ORM", "operation": "SELECT *", "rows": ROW_COUNT, "time_ms": time_ms})

    # Single row by ID
    async def select_single() -> Any:
        return await TortUser.get(id=ROW_COUNT // 2)

    time_ms = await timeit(select_single, iterations=200)
    results.append({"orm": "Tortoise ORM", "operation": "SELECT by ID", "rows": 1, "time_ms": time_ms})

    await Tortoise.close_connections()
    output_results(results)


if __name__ == "__main__":
    asyncio.run(main())
