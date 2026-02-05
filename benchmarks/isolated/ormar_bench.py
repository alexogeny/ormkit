#!/usr/bin/env python
"""Isolated Ormar ORM benchmark."""

import asyncio
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))
from helpers import POSTGRES_URL, ROW_COUNT, output_results, timeit


async def main() -> None:
    import databases
    import ormar
    import sqlalchemy

    # Ormar needs databases URL format
    db_url = POSTGRES_URL.replace("postgresql://", "postgresql+asyncpg://")
    database = databases.Database(db_url)
    metadata = sqlalchemy.MetaData()

    # ormar >= 0.20 uses OrmarConfig instead of ModelMeta
    base_ormar_config = ormar.OrmarConfig(
        database=database,
        metadata=metadata,
    )

    class OrmarUser(ormar.Model):
        ormar_config = base_ormar_config.copy(tablename="bench_users")

        id: int = ormar.Integer(primary_key=True)
        name: str = ormar.String(max_length=100)
        email: str = ormar.String(max_length=255)
        age: int = ormar.Integer()
        score: float = ormar.Float()

    results = []

    await database.connect()

    # Note: Table and data should already exist

    # SELECT *
    async def select_all() -> Any:
        return await OrmarUser.objects.all()

    time_ms = await timeit(select_all)
    results.append({"orm": "Ormar", "operation": "SELECT *", "rows": ROW_COUNT, "time_ms": time_ms})

    # Single row by ID
    async def select_single() -> Any:
        return await OrmarUser.objects.get(id=ROW_COUNT // 2)

    time_ms = await timeit(select_single, iterations=200)
    results.append({"orm": "Ormar", "operation": "SELECT by ID", "rows": 1, "time_ms": time_ms})

    # INSERT single row
    insert_counter = [ROW_COUNT + 1]

    async def insert_single() -> Any:
        i = insert_counter[0]
        insert_counter[0] += 1
        await OrmarUser.objects.create(
            name=f"new_user{i}",
            email=f"new{i}@example.com",
            age=30,
            score=90.0
        )

    time_ms = await timeit(insert_single, iterations=200)
    results.append({"orm": "Ormar", "operation": "INSERT single", "rows": 1, "time_ms": time_ms})

    # UPDATE single row
    async def update_single() -> Any:
        user = await OrmarUser.objects.get(id=ROW_COUNT // 2)
        user.score = user.score + 0.1
        await user.update()

    time_ms = await timeit(update_single, iterations=200)
    results.append({"orm": "Ormar", "operation": "UPDATE single", "rows": 1, "time_ms": time_ms})

    # DELETE single row
    delete_counter = [ROW_COUNT + 1]

    async def delete_single() -> Any:
        i = delete_counter[0]
        delete_counter[0] += 1
        try:
            user = await OrmarUser.objects.get(id=i)
            await user.delete()
        except ormar.NoMatch:
            pass

    time_ms = await timeit(delete_single, iterations=200)
    results.append({"orm": "Ormar", "operation": "DELETE single", "rows": 1, "time_ms": time_ms})

    await database.disconnect()
    output_results(results)


if __name__ == "__main__":
    asyncio.run(main())
