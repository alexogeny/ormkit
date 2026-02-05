#!/usr/bin/env python
"""Seed PostgreSQL with test data using generate_series (instant)."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from helpers import POSTGRES_URL, ROW_COUNT


async def main() -> None:
    import asyncpg

    conn = await asyncpg.connect(POSTGRES_URL)

    # Drop and recreate table
    await conn.execute("DROP TABLE IF EXISTS bench_users CASCADE")
    await conn.execute("""
        CREATE TABLE bench_users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(255) NOT NULL,
            age INTEGER NOT NULL,
            score REAL NOT NULL
        )
    """)

    # Seed using generate_series - instant for any row count
    await conn.execute(f"""
        INSERT INTO bench_users (name, email, age, score)
        SELECT
            'user' || i,
            'user' || i || '@example.com',
            25 + (i % 50),
            85.5 + (i % 15)
        FROM generate_series(0, {ROW_COUNT - 1}) AS i
    """)

    count = await conn.fetchval("SELECT COUNT(*) FROM bench_users")
    print(f"Seeded {count} rows")

    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
