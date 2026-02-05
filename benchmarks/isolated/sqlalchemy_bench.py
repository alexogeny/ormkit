#!/usr/bin/env python
"""Isolated SQLAlchemy 2.0 async benchmark."""

import asyncio
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))
from helpers import POSTGRES_URL, ROW_COUNT, output_results, timeit


async def main() -> None:
    from sqlalchemy import Float, Integer, String, select
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
    from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

    class Base(DeclarativeBase):
        pass

    class SAUser(Base):
        __tablename__ = "bench_users"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String(100))
        email: Mapped[str] = mapped_column(String(255))
        age: Mapped[int] = mapped_column(Integer)
        score: Mapped[float] = mapped_column(Float)

    results = []

    # SQLAlchemy needs asyncpg URL format
    sa_url = POSTGRES_URL.replace("postgresql://", "postgresql+asyncpg://")
    engine = create_async_engine(sa_url, pool_size=5)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    # Note: Table and data should already exist

    # SELECT *
    async def select_all() -> Any:
        async with session_factory() as session:
            result = await session.execute(select(SAUser))
            return result.scalars().all()

    time_ms = await timeit(select_all)
    results.append({"orm": "SQLAlchemy 2.0", "operation": "SELECT *", "rows": ROW_COUNT, "time_ms": time_ms})

    # Single row by ID
    async def select_single() -> Any:
        async with session_factory() as session:
            result = await session.execute(
                select(SAUser).where(SAUser.id == ROW_COUNT // 2)
            )
            return result.scalar_one_or_none()

    time_ms = await timeit(select_single, iterations=200)
    results.append({"orm": "SQLAlchemy 2.0", "operation": "SELECT by ID", "rows": 1, "time_ms": time_ms})

    await engine.dispose()
    output_results(results)


if __name__ == "__main__":
    asyncio.run(main())
