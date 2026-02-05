#!/usr/bin/env python
"""
Benchmark runner with rich output tables.

Usage:
    uv run --group bench fk-bench sqlite      # SQLite benchmarks
    uv run --group bench fk-bench postgres    # PostgreSQL benchmarks
    uv run --group bench fk-bench all         # All benchmarks

Each ORM runs in isolation to prevent cross-contamination.
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Coroutine

if TYPE_CHECKING:
    from collections.abc import Awaitable

# Configuration
POSTGRES_URL = os.getenv(
    "POSTGRES_URL", "postgresql://bench:bench@localhost:5499/bench"
)
ITERATIONS = 50
WARMUP = 3
ROW_COUNTS = [100, 1000, 10000]
BEST_OF = 3  # Run suite multiple times, take best result


@dataclass
class BenchResult:
    """Single benchmark result."""
    orm: str
    operation: str
    rows: int
    time_ms: float

    @property
    def rows_per_sec(self) -> float:
        return self.rows / (self.time_ms / 1000) if self.time_ms > 0 else 0


@dataclass
class BenchSuite:
    """Collection of benchmark results."""
    results: list[BenchResult] = field(default_factory=list)

    def add(self, result: BenchResult) -> None:
        self.results.append(result)

    def get_by_operation(self, operation: str, rows: int) -> list[BenchResult]:
        return [r for r in self.results if r.operation == operation and r.rows == rows]

    def merge_best(self, other: "BenchSuite") -> None:
        """Merge another suite, keeping only the best (fastest) result for each (orm, operation, rows)."""
        # Build a map of current best results
        best: dict[tuple[str, str, int], BenchResult] = {}
        for r in self.results:
            key = (r.orm, r.operation, r.rows)
            if key not in best or r.time_ms < best[key].time_ms:
                best[key] = r

        # Merge in other results, keeping best
        for r in other.results:
            key = (r.orm, r.operation, r.rows)
            if key not in best or r.time_ms < best[key].time_ms:
                best[key] = r

        # Replace results with best
        self.results = list(best.values())


def print_rich_table(suite: BenchSuite) -> None:
    """Print results using rich tables."""
    try:
        from rich.console import Console
        from rich.table import Table
        from rich.panel import Panel
        from rich import box
    except ImportError:
        print("rich not installed, falling back to plain output")
        print_plain_table(suite)
        return

    console = Console()

    # Group results by operation + rows
    operations: dict[tuple[str, int], list[BenchResult]] = {}
    for r in suite.results:
        key = (r.operation, r.rows)
        if key not in operations:
            operations[key] = []
        operations[key].append(r)

    # Print a table for each operation
    for (op, rows), results in sorted(operations.items()):
        results = sorted(results, key=lambda x: x.time_ms)
        fastest = results[0].time_ms if results else 1

        table = Table(
            title=f"{op} ({rows:,} rows)",
            box=box.ROUNDED,
            show_header=True,
            header_style="bold cyan",
        )
        table.add_column("ORM", style="bold")
        table.add_column("Time (ms)", justify="right")
        table.add_column("Rows/sec", justify="right")
        table.add_column("vs Fastest", justify="right")
        table.add_column("", justify="left")  # Bar

        for r in results:
            ratio = r.time_ms / fastest if fastest > 0 else 1
            bar_len = max(1, int(20 / ratio))
            bar = "█" * bar_len

            if r == results[0]:
                style = "green"
                vs = "fastest"
            elif ratio < 1.5:
                style = "yellow"
                vs = f"{ratio:.2f}x"
            else:
                style = "red"
                vs = f"{ratio:.2f}x"

            table.add_row(
                r.orm,
                f"{r.time_ms:.2f}",
                f"{r.rows_per_sec:,.0f}",
                vs,
                f"[{style}]{bar}[/{style}]",
            )

        console.print(table)
        console.print()


def print_plain_table(suite: BenchSuite) -> None:
    """Fallback plain text output."""
    operations: dict[tuple[str, int], list[BenchResult]] = {}
    for r in suite.results:
        key = (r.operation, r.rows)
        if key not in operations:
            operations[key] = []
        operations[key].append(r)

    for (op, rows), results in sorted(operations.items()):
        results = sorted(results, key=lambda x: x.time_ms)
        fastest = results[0].time_ms if results else 1

        print(f"\n{op} ({rows:,} rows)")
        print("-" * 60)
        print(f"{'ORM':<25} {'Time (ms)':>12} {'Rows/sec':>12} {'Ratio':>10}")
        print("-" * 60)

        for r in results:
            ratio = r.time_ms / fastest if fastest > 0 else 1
            print(f"{r.orm:<25} {r.time_ms:>12.2f} {r.rows_per_sec:>12,.0f} {ratio:>10.2f}x")


async def timeit(
    orm: str,
    operation: str,
    rows: int,
    fn: Callable[[], Awaitable[Any]],
    iterations: int = ITERATIONS,
    warmup: int = WARMUP,
) -> BenchResult:
    """Time an async function."""
    # Warmup
    for _ in range(warmup):
        await fn()

    # Force GC before timing
    gc.collect()

    # Benchmark
    start = time.perf_counter()
    for _ in range(iterations):
        await fn()
    elapsed = (time.perf_counter() - start) / iterations * 1000  # ms

    return BenchResult(orm=orm, operation=operation, rows=rows, time_ms=elapsed)


# ============================================================================
# Isolated benchmark runners - each runs in subprocess to avoid contamination
# ============================================================================

def run_isolated(bench_name: str, db_type: str, row_count: int) -> list[BenchResult]:
    """Run a benchmark in an isolated subprocess."""
    script = Path(__file__).parent / "isolated" / f"{bench_name}.py"
    if not script.exists():
        return []

    env = os.environ.copy()
    env["BENCH_DB_TYPE"] = db_type
    env["BENCH_ROW_COUNT"] = str(row_count)
    env["POSTGRES_URL"] = POSTGRES_URL

    try:
        result = subprocess.run(
            [sys.executable, str(script)],
            capture_output=True,
            text=True,
            timeout=120,
            env=env,
        )
        if result.returncode != 0:
            print(f"  ⚠ {bench_name} failed: {result.stderr[:200]}")
            return []

        # Parse JSON output
        import json
        data = json.loads(result.stdout)
        return [BenchResult(**r) for r in data]
    except subprocess.TimeoutExpired:
        print(f"  ⚠ {bench_name} timed out")
        return []
    except Exception as e:
        print(f"  ⚠ {bench_name} error: {e}")
        return []


# ============================================================================
# In-process benchmarks (for when isolation isn't critical)
# ============================================================================

async def bench_ormkit_sqlite(row_count: int) -> list[BenchResult]:
    """Benchmark OrmKit with SQLite."""
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
    for offset in range(0, row_count, batch_size):
        batch = min(batch_size, row_count - offset)
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

    results.append(await timeit("OrmKit (tuples)", "SELECT *", row_count, select_tuples))

    # SELECT * - dicts
    async def select_dicts() -> Any:
        result = await pool.execute("SELECT * FROM bench_users")
        return result.all()

    results.append(await timeit("OrmKit (dicts)", "SELECT *", row_count, select_dicts))

    # Single row by ID
    async def select_single() -> Any:
        result = await pool.execute("SELECT * FROM bench_users WHERE id = ?", [row_count // 2])
        return result.first()

    results.append(await timeit("OrmKit", "SELECT by ID", 1, select_single, iterations=200))

    await pool.close()
    return results


async def bench_aiosqlite(row_count: int) -> list[BenchResult]:
    """Benchmark aiosqlite."""
    try:
        import aiosqlite
    except ImportError:
        return []

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
        for i in range(row_count)
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

    results.append(await timeit("aiosqlite (tuples)", "SELECT *", row_count, select_tuples))

    # SELECT * - Row objects
    db.row_factory = aiosqlite.Row

    async def select_rows() -> Any:
        async with db.execute("SELECT * FROM bench_users") as cursor:
            return await cursor.fetchall()

    results.append(await timeit("aiosqlite (Row)", "SELECT *", row_count, select_rows))

    db.row_factory = None

    # Single row by ID
    async def select_single() -> Any:
        async with db.execute("SELECT * FROM bench_users WHERE id = ?", (row_count // 2,)) as cursor:
            return await cursor.fetchone()

    results.append(await timeit("aiosqlite", "SELECT by ID", 1, select_single, iterations=200))

    await db.close()
    return results


async def bench_ormkit_postgres(row_count: int) -> list[BenchResult]:
    """Benchmark OrmKit with PostgreSQL."""
    from ormkit import create_engine

    results = []
    pool = await create_engine(POSTGRES_URL, min_connections=1, max_connections=5)

    # Table already seeded by seed_postgres()

    # SELECT * - tuples
    async def select_tuples() -> Any:
        result = await pool.execute("SELECT * FROM bench_users")
        return result.tuples()

    results.append(await timeit("OrmKit (tuples)", "SELECT *", row_count, select_tuples))

    # SELECT * - dicts
    async def select_dicts() -> Any:
        result = await pool.execute("SELECT * FROM bench_users")
        return result.all()

    results.append(await timeit("OrmKit (dicts)", "SELECT *", row_count, select_dicts))

    # Single row by ID
    async def select_single() -> Any:
        result = await pool.execute("SELECT * FROM bench_users WHERE id = $1", [row_count // 2])
        return result.first()

    results.append(await timeit("OrmKit", "SELECT by ID", 1, select_single, iterations=200))

    await pool.close()
    return results


async def bench_asyncpg(row_count: int) -> list[BenchResult]:
    """Benchmark asyncpg."""
    try:
        import asyncpg
    except ImportError:
        return []

    results = []
    pool = await asyncpg.create_pool(POSTGRES_URL, min_size=1, max_size=5)
    assert pool is not None

    # Data should already exist from ormkit setup

    # SELECT * - raw
    async def select_raw() -> Any:
        async with pool.acquire() as conn:
            return await conn.fetch("SELECT * FROM bench_users")

    results.append(await timeit("asyncpg (raw)", "SELECT *", row_count, select_raw))

    # Single row by ID
    async def select_single() -> Any:
        async with pool.acquire() as conn:
            return await conn.fetchrow("SELECT * FROM bench_users WHERE id = $1", row_count // 2)

    results.append(await timeit("asyncpg", "SELECT by ID", 1, select_single, iterations=200))

    await pool.close()
    return results


async def bench_tortoise(row_count: int) -> list[BenchResult]:
    """Benchmark Tortoise ORM."""
    try:
        from tortoise import Tortoise, fields
        from tortoise.models import Model
    except ImportError:
        return []

    results = []

    # Define model
    class TortUser(Model):
        id = fields.IntField(pk=True)
        name = fields.CharField(max_length=100)
        email = fields.CharField(max_length=255)
        age = fields.IntField()
        score = fields.FloatField()

        class Meta:
            table = "bench_users"

    await Tortoise.init(
        db_url=POSTGRES_URL.replace("postgresql://", "postgres://"),
        modules={"models": ["__main__"]},
    )

    # SELECT *
    async def select_all() -> Any:
        return await TortUser.all()

    results.append(await timeit("Tortoise ORM", "SELECT *", row_count, select_all))

    # Single row by ID
    async def select_single() -> Any:
        return await TortUser.get(id=row_count // 2)

    results.append(await timeit("Tortoise ORM", "SELECT by ID", 1, select_single, iterations=200))

    await Tortoise.close_connections()
    return results


async def bench_sqlalchemy(row_count: int) -> list[BenchResult]:
    """Benchmark SQLAlchemy 2.0 async."""
    try:
        from sqlalchemy import String, Integer, Float, select
        from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
        from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
    except ImportError:
        return []

    results = []

    class Base(DeclarativeBase):
        pass

    class SAUser(Base):
        __tablename__ = "bench_users"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String(100))
        email: Mapped[str] = mapped_column(String(255))
        age: Mapped[int] = mapped_column(Integer)
        score: Mapped[float] = mapped_column(Float)

    sa_url = POSTGRES_URL.replace("postgresql://", "postgresql+asyncpg://")
    engine = create_async_engine(sa_url, pool_size=5)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    # SELECT *
    async def select_all() -> Any:
        async with session_factory() as session:
            result = await session.execute(select(SAUser))
            return result.scalars().all()

    results.append(await timeit("SQLAlchemy 2.0", "SELECT *", row_count, select_all))

    # Single row by ID
    async def select_single() -> Any:
        async with session_factory() as session:
            result = await session.execute(select(SAUser).where(SAUser.id == row_count // 2))
            return result.scalar_one_or_none()

    results.append(await timeit("SQLAlchemy 2.0", "SELECT by ID", 1, select_single, iterations=200))

    await engine.dispose()
    return results


# ============================================================================
# Main runner
# ============================================================================

async def run_sqlite_benchmarks_once(log: Callable[[str], None]) -> BenchSuite:
    """Run SQLite benchmarks once."""
    suite = BenchSuite()

    for row_count in ROW_COUNTS:
        log(f"[yellow]Running with {row_count:,} rows...[/yellow]")

        # OrmKit
        log("  • OrmKit...")
        for r in await bench_ormkit_sqlite(row_count):
            suite.add(r)

        # aiosqlite
        log("  • aiosqlite...")
        for r in await bench_aiosqlite(row_count):
            suite.add(r)

        gc.collect()

    return suite


async def run_sqlite_benchmarks() -> BenchSuite:
    """Run SQLite benchmarks, best of N runs."""
    try:
        from rich.console import Console
        console = Console()
    except ImportError:
        console = None  # type: ignore

    def log(msg: str) -> None:
        if console:
            console.print(msg)
        else:
            print(msg)

    log("\n[bold cyan]SQLite Benchmarks[/bold cyan]\n")
    log(f"[dim]Running best of {BEST_OF} for each benchmark[/dim]\n")

    best_suite = BenchSuite()

    for run_num in range(1, BEST_OF + 1):
        log(f"\n[bold magenta]━━━ Run {run_num}/{BEST_OF} ━━━[/bold magenta]\n")
        run_suite = await run_sqlite_benchmarks_once(log)
        best_suite.merge_best(run_suite)

    return best_suite


async def seed_postgres(row_count: int) -> None:
    """Seed PostgreSQL using generate_series (instant)."""
    import asyncpg

    conn = await asyncpg.connect(POSTGRES_URL)
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
    await conn.execute(f"""
        INSERT INTO bench_users (name, email, age, score)
        SELECT
            'user' || i,
            'user' || i || '@example.com',
            25 + (i % 50),
            85.5 + (i % 15)
        FROM generate_series(0, {row_count - 1}) AS i
    """)
    await conn.close()


async def run_postgres_benchmarks_once(log: Callable[[str], None]) -> BenchSuite:
    """Run PostgreSQL benchmarks once using isolated subprocesses."""
    suite = BenchSuite()

    # Benchmarks to run (script name without .py)
    postgres_benchmarks = [
        ("ormkit_postgres", "OrmKit"),
        ("asyncpg_bench", "asyncpg"),
        ("tortoise_bench", "Tortoise ORM"),
        ("sqlalchemy_bench", "SQLAlchemy 2.0"),
        ("databases_bench", "databases"),
        ("ormar_bench", "Ormar"),
    ]

    for row_count in ROW_COUNTS:
        log(f"[yellow]Seeding {row_count:,} rows...[/yellow]")
        await seed_postgres(row_count)

        log(f"[yellow]Running benchmarks with {row_count:,} rows...[/yellow]")

        for script_name, display_name in postgres_benchmarks:
            log(f"  • {display_name}...")
            results = run_isolated(script_name, "postgres", row_count)
            for r in results:
                suite.add(r)

        gc.collect()

    return suite


async def run_postgres_benchmarks() -> BenchSuite:
    """Run PostgreSQL benchmarks using isolated subprocesses, best of N runs."""
    try:
        from rich.console import Console
        console = Console()
    except ImportError:
        console = None  # type: ignore

    def log(msg: str) -> None:
        if console:
            console.print(msg)
        else:
            print(msg)

    log("\n[bold cyan]PostgreSQL Benchmarks[/bold cyan]\n")
    log(f"[dim]Running best of {BEST_OF} for each benchmark[/dim]\n")

    best_suite = BenchSuite()

    for run_num in range(1, BEST_OF + 1):
        log(f"\n[bold magenta]━━━ Run {run_num}/{BEST_OF} ━━━[/bold magenta]\n")
        run_suite = await run_postgres_benchmarks_once(log)
        best_suite.merge_best(run_suite)

    return best_suite


def main() -> None:
    """Main entry point."""
    global BEST_OF

    parser = argparse.ArgumentParser(description="OrmKit ORM Benchmarks")
    parser.add_argument(
        "target",
        choices=["sqlite", "postgres", "all"],
        default="all",
        nargs="?",
        help="Which benchmarks to run",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output results as JSON",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=BEST_OF,
        help=f"Number of runs to take best from (default: {BEST_OF})",
    )
    args = parser.parse_args()

    BEST_OF = args.runs

    try:
        from rich.console import Console
        from rich.panel import Panel
        console = Console()
        console.print(Panel.fit(
            "[bold]OrmKit ORM Benchmark Suite[/bold]\n"
            "Comparing against popular Python ORMs and drivers",
            border_style="cyan",
        ))
    except ImportError:
        print("=" * 60)
        print("OrmKit ORM Benchmark Suite")
        print("=" * 60)

    suite = BenchSuite()

    if args.target in ("sqlite", "all"):
        sqlite_suite = asyncio.run(run_sqlite_benchmarks())
        suite.results.extend(sqlite_suite.results)

    if args.target in ("postgres", "all"):
        postgres_suite = asyncio.run(run_postgres_benchmarks())
        suite.results.extend(postgres_suite.results)

    if args.json:
        import json
        print(json.dumps([
            {"orm": r.orm, "operation": r.operation, "rows": r.rows, "time_ms": r.time_ms}
            for r in suite.results
        ], indent=2))
    else:
        print_rich_table(suite)


if __name__ == "__main__":
    main()
