#!/usr/bin/env python
"""
Comprehensive PostgreSQL benchmarks comparing:
- ForeignKey (Rust-backed ORM)
- asyncpg (raw driver, gold standard)
- asyncpg + manual model hydration
- Tortoise ORM (pure Python async ORM)
- SQLAlchemy 2.0 (async, most popular Python ORM)
- Piccolo ORM (async Python ORM)
- databases + encode/databases (async query builder)

Requirements:
    pip install asyncpg tortoise-orm sqlalchemy[asyncio] piccolo databases

Usage:
    # Start PostgreSQL (e.g., via Docker):
    docker run -d --name fk-bench -e POSTGRES_PASSWORD=bench -p 5432:5432 postgres:15

    # Run benchmark:
    python benchmarks/compare_postgres.py

Environment variables:
    POSTGRES_URL: Connection string (default: postgresql://postgres:bench@localhost:5432/postgres)
"""

import asyncio
import os
import time
from dataclasses import dataclass
from typing import Any

# Configuration
POSTGRES_URL = os.getenv(
    "POSTGRES_URL", "postgresql://postgres:bench@localhost:5432/postgres"
)
ROW_COUNTS = [100, 1000, 10000]
ITERATIONS = 50
WARMUP = 3


@dataclass
class BenchResult:
    name: str
    operation: str
    rows: int
    time_ms: float
    rows_per_sec: float


def print_header(title: str) -> None:
    print("\n" + "=" * 70)
    print(f" {title}")
    print("=" * 70)


def print_results(results: list[BenchResult]) -> None:
    """Print benchmark results in a formatted table."""
    # Group by operation
    operations = {}
    for r in results:
        key = (r.operation, r.rows)
        if key not in operations:
            operations[key] = []
        operations[key].append(r)

    for (op, rows), entries in sorted(operations.items(), key=lambda x: (x[0][0], x[0][1])):
        print(f"\n{op} ({rows} rows):")
        print("-" * 50)

        # Sort by time
        entries = sorted(entries, key=lambda x: x.time_ms)
        fastest = entries[0].time_ms

        for e in entries:
            speedup = e.time_ms / fastest if fastest > 0 else 1
            bar = "█" * int(20 / speedup)
            if e == entries[0]:
                print(f"  {e.name:<25} {e.time_ms:>8.2f}ms  {bar} (fastest)")
            else:
                print(f"  {e.name:<25} {e.time_ms:>8.2f}ms  {bar} ({speedup:.1f}x slower)")


async def timeit(name: str, operation: str, rows: int, fn, iterations: int = ITERATIONS):
    """Time an async function and return benchmark result."""
    # Warmup
    for _ in range(WARMUP):
        await fn()

    # Benchmark
    start = time.perf_counter()
    for _ in range(iterations):
        await fn()
    elapsed = (time.perf_counter() - start) / iterations * 1000  # ms

    rows_per_sec = rows / (elapsed / 1000) if elapsed > 0 else 0

    return BenchResult(
        name=name,
        operation=operation,
        rows=rows,
        time_ms=elapsed,
        rows_per_sec=rows_per_sec,
    )


# ============================================================================
# OrmKit Setup
# ============================================================================


async def setup_ormkit():
    """Setup OrmKit models and connection."""
    from ormkit import (
        Base,
        ForeignKey as FK,
        Mapped,
        create_engine,
        create_session,
        joinedload,
        mapped_column,
        relationship,
        selectinload,
    )

    class BenchUser(Base):
        __tablename__ = "bench_users"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(max_length=100)
        email: Mapped[str] = mapped_column()
        age: Mapped[int] = mapped_column()
        score: Mapped[float] = mapped_column()
        posts: Mapped[list["BenchPost"]] = relationship(back_populates="author")

    class BenchPost(Base):
        __tablename__ = "bench_posts"
        id: Mapped[int] = mapped_column(primary_key=True)
        title: Mapped[str] = mapped_column(max_length=200)
        body: Mapped[str] = mapped_column()
        author_id: Mapped[int] = mapped_column(FK("bench_users.id"))
        author: Mapped[BenchUser] = relationship(back_populates="posts")

    pool = await create_engine(POSTGRES_URL, min_connections=1, max_connections=10)
    session = create_session(pool)

    return {
        "pool": pool,
        "session": session,
        "User": BenchUser,
        "Post": BenchPost,
        "joinedload": joinedload,
        "selectinload": selectinload,
    }


# ============================================================================
# asyncpg Setup
# ============================================================================


async def setup_asyncpg():
    """Setup raw asyncpg connection pool."""
    import asyncpg

    pool = await asyncpg.create_pool(POSTGRES_URL, min_size=1, max_size=10)
    return {"pool": pool}


# ============================================================================
# Tortoise ORM Setup
# ============================================================================


async def setup_tortoise():
    """Setup Tortoise ORM models and connection."""
    from tortoise import Tortoise, fields
    from tortoise.models import Model

    class TortUser(Model):
        id = fields.IntField(pk=True)
        name = fields.CharField(max_length=100)
        email = fields.CharField(max_length=255)
        age = fields.IntField()
        score = fields.FloatField()

        class Meta:
            table = "bench_users"

    class TortPost(Model):
        id = fields.IntField(pk=True)
        title = fields.CharField(max_length=200)
        body = fields.TextField()
        author = fields.ForeignKeyField(
            "models.TortUser", related_name="posts", db_column="author_id"
        )

        class Meta:
            table = "bench_posts"

    await Tortoise.init(
        db_url=POSTGRES_URL.replace("postgresql://", "postgres://"),
        modules={"models": [__name__]},
    )

    return {"User": TortUser, "Post": TortPost}


# ============================================================================
# SQLAlchemy 2.0 Async Setup
# ============================================================================


async def setup_sqlalchemy():
    """Setup SQLAlchemy 2.0 async models and connection."""
    from sqlalchemy import ForeignKey as SAFK
    from sqlalchemy import String, Integer, Float, Text
    from sqlalchemy.ext.asyncio import AsyncSession as SASession
    from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
    from sqlalchemy.orm import DeclarativeBase, Mapped as SAMapped, mapped_column as sa_mapped_column, relationship as sa_relationship

    class SABase(DeclarativeBase):
        pass

    class SAUser(SABase):
        __tablename__ = "bench_users"

        id: SAMapped[int] = sa_mapped_column(primary_key=True)
        name: SAMapped[str] = sa_mapped_column(String(100))
        email: SAMapped[str] = sa_mapped_column(String(255))
        age: SAMapped[int] = sa_mapped_column(Integer)
        score: SAMapped[float] = sa_mapped_column(Float)

        posts: SAMapped[list["SAPost"]] = sa_relationship(back_populates="author")

    class SAPost(SABase):
        __tablename__ = "bench_posts"

        id: SAMapped[int] = sa_mapped_column(primary_key=True)
        title: SAMapped[str] = sa_mapped_column(String(200))
        body: SAMapped[str] = sa_mapped_column(Text)
        author_id: SAMapped[int] = sa_mapped_column(SAFK("bench_users.id"))

        author: SAMapped[SAUser] = sa_relationship(back_populates="posts")

    # SQLAlchemy needs asyncpg URL format
    sa_url = POSTGRES_URL.replace("postgresql://", "postgresql+asyncpg://")
    engine = create_async_engine(sa_url, pool_size=10)
    session_factory = async_sessionmaker(engine, class_=SASession, expire_on_commit=False)

    return {
        "engine": engine,
        "session_factory": session_factory,
        "User": SAUser,
        "Post": SAPost,
    }


# ============================================================================
# Piccolo ORM Setup
# ============================================================================


async def setup_piccolo():
    """Setup Piccolo ORM models and connection."""
    try:
        from piccolo.engine.postgres import PostgresEngine
        from piccolo.table import Table
        from piccolo.columns import Varchar, Integer, Float, Text, ForeignKey as PiccoloFK
    except ImportError:
        return None

    # Parse URL for Piccolo config
    import urllib.parse
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
        name = Varchar(length=100)
        email = Varchar(length=255)
        age = Integer()
        score = Float()

    class PicPost(Table, tablename="bench_posts", db=engine):
        title = Varchar(length=200)
        body = Text()
        author_id = Integer()

    await engine.start_connection_pool()

    return {"engine": engine, "User": PicUser, "Post": PicPost}


# ============================================================================
# encode/databases Setup (lightweight async query builder)
# ============================================================================


async def setup_databases():
    """Setup encode/databases connection."""
    try:
        from databases import Database
    except ImportError:
        return None

    database = Database(POSTGRES_URL)
    await database.connect()

    return {"database": database}


# ============================================================================
# Database Setup
# ============================================================================


async def create_tables(pool):
    """Create benchmark tables."""
    import asyncpg

    if isinstance(pool, asyncpg.Pool):
        conn = pool
    else:
        # ForeignKey pool
        await pool.execute("DROP TABLE IF EXISTS bench_posts CASCADE")
        await pool.execute("DROP TABLE IF EXISTS bench_users CASCADE")
        await pool.execute("""
            CREATE TABLE bench_users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(255) NOT NULL,
                age INTEGER NOT NULL,
                score REAL NOT NULL
            )
        """)
        await pool.execute("""
            CREATE TABLE bench_posts (
                id SERIAL PRIMARY KEY,
                title VARCHAR(200) NOT NULL,
                body TEXT NOT NULL,
                author_id INTEGER REFERENCES bench_users(id)
            )
        """)
        return

    # asyncpg
    async with conn.acquire() as c:
        await c.execute("DROP TABLE IF EXISTS bench_posts CASCADE")
        await c.execute("DROP TABLE IF EXISTS bench_users CASCADE")
        await c.execute("""
            CREATE TABLE bench_users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(255) NOT NULL,
                age INTEGER NOT NULL,
                score REAL NOT NULL
            )
        """)
        await c.execute("""
            CREATE TABLE bench_posts (
                id SERIAL PRIMARY KEY,
                title VARCHAR(200) NOT NULL,
                body TEXT NOT NULL,
                author_id INTEGER REFERENCES bench_users(id)
            )
        """)


async def insert_test_data(asyncpg_pool, row_count: int):
    """Insert test data using asyncpg (fastest)."""
    # Generate data
    users = [
        (f"user_{i}", f"user_{i}@example.com", 20 + i % 50, 50.0 + i % 50)
        for i in range(row_count)
    ]

    async with asyncpg_pool.acquire() as conn:
        # Clear existing data
        await conn.execute("TRUNCATE bench_posts, bench_users RESTART IDENTITY CASCADE")

        # Insert users
        await conn.executemany(
            "INSERT INTO bench_users (name, email, age, score) VALUES ($1, $2, $3, $4)",
            users,
        )

        # Insert posts (3 per user)
        posts = [
            (f"Post {j} by user {i}", f"Body of post {j} by user {i}", i + 1)
            for i in range(row_count)
            for j in range(3)
        ]
        await conn.executemany(
            "INSERT INTO bench_posts (title, body, author_id) VALUES ($1, $2, $3)",
            posts,
        )


# ============================================================================
# Benchmark Functions
# ============================================================================


async def run_select_all_benchmarks(
    fk_ctx: dict,
    asyncpg_ctx: dict,
    tort_ctx: dict,
    sa_ctx: dict | None,
    piccolo_ctx: dict | None,
    databases_ctx: dict | None,
    row_count: int,
) -> list[BenchResult]:
    """Benchmark SELECT * queries."""
    results = []

    # ForeignKey - raw tuples
    async def fk_tuples():
        result = await fk_ctx["pool"].execute("SELECT * FROM bench_users")
        return result.tuples()

    results.append(await timeit("ForeignKey (tuples)", "SELECT *", row_count, fk_tuples))

    # ForeignKey - dicts
    async def fk_dicts():
        result = await fk_ctx["pool"].execute("SELECT * FROM bench_users")
        return result.all()

    results.append(await timeit("ForeignKey (dicts)", "SELECT *", row_count, fk_dicts))

    # ForeignKey - ORM models
    async def fk_models():
        return await fk_ctx["session"].query(fk_ctx["User"]).all()

    results.append(await timeit("ForeignKey (ORM)", "SELECT *", row_count, fk_models))

    # asyncpg - raw
    async def asyncpg_raw():
        async with asyncpg_ctx["pool"].acquire() as conn:
            return await conn.fetch("SELECT * FROM bench_users")

    results.append(await timeit("asyncpg (raw)", "SELECT *", row_count, asyncpg_raw))

    # asyncpg - with manual model hydration
    @dataclass
    class UserModel:
        id: int
        name: str
        email: str
        age: int
        score: float

    async def asyncpg_hydrated():
        async with asyncpg_ctx["pool"].acquire() as conn:
            rows = await conn.fetch("SELECT * FROM bench_users")
            return [UserModel(**dict(r)) for r in rows]

    results.append(
        await timeit("asyncpg + hydration", "SELECT *", row_count, asyncpg_hydrated)
    )

    # Tortoise ORM
    async def tortoise_all():
        return await tort_ctx["User"].all()

    results.append(await timeit("Tortoise ORM", "SELECT *", row_count, tortoise_all))

    # SQLAlchemy 2.0 (if available)
    if sa_ctx:
        from sqlalchemy import select as sa_select

        async def sqlalchemy_all():
            async with sa_ctx["session_factory"]() as session:
                result = await session.execute(sa_select(sa_ctx["User"]))
                return result.scalars().all()

        results.append(await timeit("SQLAlchemy 2.0", "SELECT *", row_count, sqlalchemy_all))

    # Piccolo ORM (if available)
    if piccolo_ctx:
        async def piccolo_all():
            return await piccolo_ctx["User"].select()

        results.append(await timeit("Piccolo ORM", "SELECT *", row_count, piccolo_all))

    # encode/databases (if available)
    if databases_ctx:
        async def databases_all():
            return await databases_ctx["database"].fetch_all("SELECT * FROM bench_users")

        results.append(await timeit("databases", "SELECT *", row_count, databases_all))

    return results


async def run_single_row_benchmarks(
    fk_ctx: dict, asyncpg_ctx: dict, tort_ctx: dict, row_count: int
) -> list[BenchResult]:
    """Benchmark single row queries by ID."""
    results = []
    iterations = 200

    # ForeignKey
    async def fk_single():
        for i in range(10):
            result = await fk_ctx["pool"].execute(
                "SELECT * FROM bench_users WHERE id = $1", [i % row_count + 1]
            )
            _ = result.first()

    results.append(
        await timeit("ForeignKey", "Single row ×10", 10, fk_single, iterations=iterations)
    )

    # ForeignKey ORM
    async def fk_orm_single():
        for i in range(10):
            _ = await fk_ctx["session"].get(fk_ctx["User"], i % row_count + 1)

    results.append(
        await timeit("ForeignKey (ORM)", "Single row ×10", 10, fk_orm_single, iterations=iterations)
    )

    # asyncpg
    async def asyncpg_single():
        async with asyncpg_ctx["pool"].acquire() as conn:
            for i in range(10):
                _ = await conn.fetchrow(
                    "SELECT * FROM bench_users WHERE id = $1", i % row_count + 1
                )

    results.append(
        await timeit("asyncpg", "Single row ×10", 10, asyncpg_single, iterations=iterations)
    )

    # Tortoise
    async def tort_single():
        for i in range(10):
            _ = await tort_ctx["User"].get(id=i % row_count + 1)

    results.append(
        await timeit("Tortoise ORM", "Single row ×10", 10, tort_single, iterations=iterations)
    )

    return results


async def run_relationship_benchmarks(
    fk_ctx: dict, asyncpg_ctx: dict, tort_ctx: dict, row_count: int
) -> list[BenchResult]:
    """Benchmark relationship loading."""
    results = []
    # Fewer iterations for relationship queries (more complex)
    iterations = 20

    # ForeignKey - selectinload (N+1 avoidance)
    async def fk_selectin():
        return await fk_ctx["session"].query(fk_ctx["Post"]).options(
            fk_ctx["selectinload"]("author")
        ).limit(100).all()

    results.append(
        await timeit("ForeignKey selectinload", "Posts+Author", 100, fk_selectin, iterations=iterations)
    )

    # ForeignKey - joinedload (single query)
    async def fk_joined():
        return await fk_ctx["session"].query(fk_ctx["Post"]).options(
            fk_ctx["joinedload"]("author")
        ).limit(100).all()

    results.append(
        await timeit("ForeignKey joinedload", "Posts+Author", 100, fk_joined, iterations=iterations)
    )

    # asyncpg - manual join
    @dataclass
    class PostWithAuthor:
        id: int
        title: str
        body: str
        author_id: int
        author_name: str

    async def asyncpg_join():
        async with asyncpg_ctx["pool"].acquire() as conn:
            rows = await conn.fetch("""
                SELECT p.id, p.title, p.body, p.author_id, u.name as author_name
                FROM bench_posts p
                JOIN bench_users u ON p.author_id = u.id
                LIMIT 100
            """)
            return [PostWithAuthor(**dict(r)) for r in rows]

    results.append(
        await timeit("asyncpg (manual JOIN)", "Posts+Author", 100, asyncpg_join, iterations=iterations)
    )

    # Tortoise - prefetch_related
    async def tort_prefetch():
        return await tort_ctx["Post"].all().prefetch_related("author").limit(100)

    results.append(
        await timeit("Tortoise prefetch", "Posts+Author", 100, tort_prefetch, iterations=iterations)
    )

    return results


async def run_insert_benchmarks(
    fk_ctx: dict, asyncpg_ctx: dict, tort_ctx: dict
) -> list[BenchResult]:
    """Benchmark INSERT operations."""
    results = []
    iterations = 10
    batch_size = 100

    # ForeignKey bulk insert
    async def fk_insert():
        users = [
            fk_ctx["User"](
                name=f"new_user_{i}",
                email=f"new_{i}@example.com",
                age=25,
                score=75.0,
            )
            for i in range(batch_size)
        ]
        await fk_ctx["session"].insert_all(users)

    results.append(
        await timeit("ForeignKey", f"INSERT {batch_size}", batch_size, fk_insert, iterations=iterations)
    )

    # asyncpg executemany
    async def asyncpg_insert():
        data = [
            (f"new_user_{i}", f"new_{i}@example.com", 25, 75.0)
            for i in range(batch_size)
        ]
        async with asyncpg_ctx["pool"].acquire() as conn:
            await conn.executemany(
                "INSERT INTO bench_users (name, email, age, score) VALUES ($1, $2, $3, $4)",
                data,
            )

    results.append(
        await timeit("asyncpg", f"INSERT {batch_size}", batch_size, asyncpg_insert, iterations=iterations)
    )

    # Tortoise bulk_create
    async def tort_insert():
        users = [
            tort_ctx["User"](
                name=f"new_user_{i}",
                email=f"new_{i}@example.com",
                age=25,
                score=75.0,
            )
            for i in range(batch_size)
        ]
        await tort_ctx["User"].bulk_create(users)

    results.append(
        await timeit("Tortoise ORM", f"INSERT {batch_size}", batch_size, tort_insert, iterations=iterations)
    )

    return results


# ============================================================================
# Main
# ============================================================================


async def main():
    print_header("PostgreSQL ORM Benchmark Suite")
    print(f"Connection: {POSTGRES_URL}")
    print(f"Iterations per test: {ITERATIONS}")

    all_results = []
    sa_ctx = None
    piccolo_ctx = None
    databases_ctx = None

    try:
        # Setup connections
        print("\nSetting up connections...")
        asyncpg_ctx = await setup_asyncpg()
        print("  ✓ asyncpg")

        await create_tables(asyncpg_ctx["pool"])
        print("  ✓ Tables created")

        fk_ctx = await setup_ormkit()
        print("  ✓ ForeignKey")

        tort_ctx = await setup_tortoise()
        print("  ✓ Tortoise ORM")

        # Optional ORMs
        try:
            sa_ctx = await setup_sqlalchemy()
            print("  ✓ SQLAlchemy 2.0")
        except ImportError:
            print("  ⊘ SQLAlchemy (not installed)")
        except Exception as e:
            print(f"  ⊘ SQLAlchemy (error: {e})")

        try:
            piccolo_ctx = await setup_piccolo()
            if piccolo_ctx:
                print("  ✓ Piccolo ORM")
            else:
                print("  ⊘ Piccolo (not installed)")
        except Exception as e:
            print(f"  ⊘ Piccolo (error: {e})")

        try:
            databases_ctx = await setup_databases()
            if databases_ctx:
                print("  ✓ databases")
            else:
                print("  ⊘ databases (not installed)")
        except Exception as e:
            print(f"  ⊘ databases (error: {e})")

        # Run benchmarks for each row count
        for row_count in ROW_COUNTS:
            print_header(f"Benchmarks with {row_count:,} rows")

            print("Inserting test data...")
            await insert_test_data(asyncpg_ctx["pool"], row_count)

            print("Running SELECT * benchmarks...")
            all_results.extend(
                await run_select_all_benchmarks(
                    fk_ctx, asyncpg_ctx, tort_ctx, sa_ctx, piccolo_ctx, databases_ctx, row_count
                )
            )

            print("Running single row benchmarks...")
            all_results.extend(
                await run_single_row_benchmarks(fk_ctx, asyncpg_ctx, tort_ctx, row_count)
            )

            if row_count >= 100:
                print("Running relationship benchmarks...")
                all_results.extend(
                    await run_relationship_benchmarks(fk_ctx, asyncpg_ctx, tort_ctx, row_count)
                )

        # Insert benchmarks (only once)
        print_header("INSERT Benchmarks")
        all_results.extend(await run_insert_benchmarks(fk_ctx, asyncpg_ctx, tort_ctx))

        # Print results
        print_header("RESULTS SUMMARY")
        print_results(all_results)

        # Print key insights
        print_header("KEY INSIGHTS")

        # Find fastest for SELECT
        select_10k = [r for r in all_results if r.operation == "SELECT *" and r.rows == 10000]
        if select_10k:
            select_10k.sort(key=lambda x: x.time_ms)
            print(f"\nSELECT * (10K rows) - Fastest: {select_10k[0].name}")
            asyncpg_time = next((r.time_ms for r in select_10k if "asyncpg (raw)" in r.name), None)
            fk_time = next((r.time_ms for r in select_10k if "ForeignKey (ORM)" in r.name), None)
            tort_time = next((r.time_ms for r in select_10k if "Tortoise" in r.name), None)
            sa_time = next((r.time_ms for r in select_10k if "SQLAlchemy" in r.name), None)

            if asyncpg_time and fk_time:
                print(f"  ForeignKey ORM vs asyncpg raw: {fk_time/asyncpg_time:.1f}x")
            if tort_time and fk_time:
                print(f"  ForeignKey ORM vs Tortoise ORM: {tort_time/fk_time:.1f}x faster")
            if sa_time and fk_time:
                print(f"  ForeignKey ORM vs SQLAlchemy: {sa_time/fk_time:.1f}x faster")

    except Exception as e:
        print(f"\nError: {e}")
        print("\nMake sure PostgreSQL is running and accessible.")
        print("You can start it with Docker:")
        print("  docker run -d --name fk-bench -e POSTGRES_PASSWORD=bench -p 5432:5432 postgres:15")
        raise

    finally:
        # Cleanup
        print("\nCleaning up...")
        try:
            await fk_ctx["pool"].close()
        except Exception:
            pass
        try:
            await asyncpg_ctx["pool"].close()
        except Exception:
            pass
        try:
            from tortoise import Tortoise
            await Tortoise.close_connections()
        except Exception:
            pass
        try:
            if sa_ctx:
                await sa_ctx["engine"].dispose()
        except Exception:
            pass
        try:
            if piccolo_ctx:
                await piccolo_ctx["engine"].close_connection_pool()
        except Exception:
            pass
        try:
            if databases_ctx:
                await databases_ctx["database"].disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(main())
