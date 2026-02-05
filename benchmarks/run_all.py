#!/usr/bin/env python
"""Run all benchmarks and generate comparison results."""

import asyncio
import json
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class BenchmarkResult:
    """Result from a single benchmark run."""
    name: str
    library: str
    operation: str
    rows: int
    time_ms: float
    rows_per_sec: float
    iterations: int = 1


@dataclass
class BenchmarkSuite:
    """Collection of benchmark results."""
    results: list[BenchmarkResult] = field(default_factory=list)

    def add(self, result: BenchmarkResult):
        self.results.append(result)

    def to_json(self) -> str:
        return json.dumps([r.__dict__ for r in self.results], indent=2)

    def to_markdown(self) -> str:
        """Generate markdown table of results."""
        lines = [
            "# OrmKit Benchmark Results",
            "",
            "## SQLite Benchmarks",
            "",
            "| Operation | Rows | OrmKit (ms) | OrmKit (rows/s) |",
            "|-----------|------|-----------------|---------------------|",
        ]

        for r in self.results:
            if r.library == "ormkit":
                lines.append(
                    f"| {r.operation} | {r.rows:,} | {r.time_ms:.2f} | {r.rows_per_sec:,.0f} |"
                )

        return "\n".join(lines)


async def benchmark_ormkit_sqlite() -> list[BenchmarkResult]:
    """Run OrmKit SQLite benchmarks."""
    from ormkit import create_engine

    results = []

    pool = await create_engine("sqlite::memory:")

    # Setup table
    await pool.execute("DROP TABLE IF EXISTS bench_users")
    await pool.execute("""
        CREATE TABLE bench_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            age INTEGER,
            score REAL
        )
    """)

    # Benchmark: Single inserts (100 rows)
    start = time.perf_counter()
    for i in range(100):
        await pool.execute(
            "INSERT INTO bench_users (name, email, age, score) VALUES (?, ?, ?, ?)",
            [f"user{i}", f"user{i}@example.com", 25 + i % 50, 85.5 + i % 15]
        )
    elapsed = time.perf_counter() - start
    results.append(BenchmarkResult(
        name="single_insert_100",
        library="ormkit",
        operation="Single Insert",
        rows=100,
        time_ms=elapsed * 1000,
        rows_per_sec=100 / elapsed,
        iterations=100,
    ))

    await pool.execute("DELETE FROM bench_users")

    # Benchmark: Bulk inserts
    # SQLite has a limit of ~999 variables, so we batch in chunks of 200 rows (800 params)
    for count in [100, 1000, 10000]:
        batch_size = 200  # 200 rows * 4 columns = 800 params
        start = time.perf_counter()

        remaining = count
        offset = 0
        while remaining > 0:
            batch = min(remaining, batch_size)
            placeholders = ", ".join(["(?, ?, ?, ?)"] * batch)
            params = []
            for i in range(batch):
                idx = offset + i
                params.extend([f"user{idx}", f"user{idx}@example.com", 25 + idx % 50, 85.5 + idx % 15])
            await pool.execute(
                f"INSERT INTO bench_users (name, email, age, score) VALUES {placeholders}",
                params
            )
            remaining -= batch
            offset += batch

        elapsed = time.perf_counter() - start
        results.append(BenchmarkResult(
            name=f"bulk_insert_{count}",
            library="ormkit",
            operation="Bulk Insert",
            rows=count,
            time_ms=elapsed * 1000,
            rows_per_sec=count / elapsed,
        ))

    # Benchmark: Select all (10000 rows in table)
    start = time.perf_counter()
    result = await pool.execute("SELECT * FROM bench_users")
    rows = result.all()
    elapsed = time.perf_counter() - start
    results.append(BenchmarkResult(
        name="select_all",
        library="ormkit",
        operation="Select All",
        rows=len(rows),
        time_ms=elapsed * 1000,
        rows_per_sec=len(rows) / elapsed,
    ))

    # Benchmark: Select by ID (100 iterations)
    start = time.perf_counter()
    for i in range(100):
        result = await pool.execute("SELECT * FROM bench_users WHERE id = ?", [i + 1])
        result.first()
    elapsed = time.perf_counter() - start
    results.append(BenchmarkResult(
        name="select_by_id",
        library="ormkit",
        operation="Select by ID",
        rows=1,
        time_ms=elapsed * 1000,
        rows_per_sec=100 / elapsed,
        iterations=100,
    ))

    # Benchmark: Select with filter
    start = time.perf_counter()
    result = await pool.execute(
        "SELECT * FROM bench_users WHERE age > ? AND score > ?",
        [30, 90.0]
    )
    rows = result.all()
    elapsed = time.perf_counter() - start
    results.append(BenchmarkResult(
        name="select_filter",
        library="ormkit",
        operation="Select with Filter",
        rows=len(rows),
        time_ms=elapsed * 1000,
        rows_per_sec=len(rows) / elapsed if elapsed > 0 else 0,
    ))

    await pool.close()
    return results


async def benchmark_aiosqlite() -> list[BenchmarkResult]:
    """Run aiosqlite benchmarks for comparison."""
    try:
        import aiosqlite
    except ImportError:
        print("aiosqlite not installed, skipping comparison benchmarks")
        return []

    results = []

    async with aiosqlite.connect(":memory:") as db:
        # Setup table
        await db.execute("DROP TABLE IF EXISTS bench_users")
        await db.execute("""
            CREATE TABLE bench_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                age INTEGER,
                score REAL
            )
        """)
        await db.commit()

        # Benchmark: Single inserts (100 rows)
        start = time.perf_counter()
        for i in range(100):
            await db.execute(
                "INSERT INTO bench_users (name, email, age, score) VALUES (?, ?, ?, ?)",
                (f"user{i}", f"user{i}@example.com", 25 + i % 50, 85.5 + i % 15)
            )
        await db.commit()
        elapsed = time.perf_counter() - start
        results.append(BenchmarkResult(
            name="single_insert_100",
            library="aiosqlite",
            operation="Single Insert",
            rows=100,
            time_ms=elapsed * 1000,
            rows_per_sec=100 / elapsed,
            iterations=100,
        ))

        await db.execute("DELETE FROM bench_users")
        await db.commit()

        # Benchmark: Bulk inserts
        for count in [100, 1000, 10000]:
            data = [
                (f"user{i}", f"user{i}@example.com", 25 + i % 50, 85.5 + i % 15)
                for i in range(count)
            ]

            start = time.perf_counter()
            await db.executemany(
                "INSERT INTO bench_users (name, email, age, score) VALUES (?, ?, ?, ?)",
                data
            )
            await db.commit()
            elapsed = time.perf_counter() - start
            results.append(BenchmarkResult(
                name=f"bulk_insert_{count}",
                library="aiosqlite",
                operation="Bulk Insert",
                rows=count,
                time_ms=elapsed * 1000,
                rows_per_sec=count / elapsed,
            ))

        # Benchmark: Select all
        start = time.perf_counter()
        async with db.execute("SELECT * FROM bench_users") as cursor:
            rows = await cursor.fetchall()
        elapsed = time.perf_counter() - start
        results.append(BenchmarkResult(
            name="select_all",
            library="aiosqlite",
            operation="Select All",
            rows=len(rows),
            time_ms=elapsed * 1000,
            rows_per_sec=len(rows) / elapsed,
        ))

        # Benchmark: Select by ID
        start = time.perf_counter()
        for i in range(100):
            async with db.execute("SELECT * FROM bench_users WHERE id = ?", (i + 1,)) as cursor:
                await cursor.fetchone()
        elapsed = time.perf_counter() - start
        results.append(BenchmarkResult(
            name="select_by_id",
            library="aiosqlite",
            operation="Select by ID",
            rows=1,
            time_ms=elapsed * 1000,
            rows_per_sec=100 / elapsed,
            iterations=100,
        ))

        # Benchmark: Select with filter
        start = time.perf_counter()
        async with db.execute(
            "SELECT * FROM bench_users WHERE age > ? AND score > ?",
            (30, 90.0)
        ) as cursor:
            rows = await cursor.fetchall()
        elapsed = time.perf_counter() - start
        results.append(BenchmarkResult(
            name="select_filter",
            library="aiosqlite",
            operation="Select with Filter",
            rows=len(rows),
            time_ms=elapsed * 1000,
            rows_per_sec=len(rows) / elapsed if elapsed > 0 else 0,
        ))

    return results


def print_comparison(fk_results: list[BenchmarkResult], other_results: list[BenchmarkResult]):
    """Print side-by-side comparison."""
    print("\n" + "=" * 80)
    print("BENCHMARK COMPARISON")
    print("=" * 80)

    # Group by operation name
    fk_by_name = {r.name: r for r in fk_results}
    other_by_name = {r.name: r for r in other_results}

    print(f"\n{'Operation':<25} {'Rows':>10} {'ForeignKey':>15} {'aiosqlite':>15} {'Speedup':>10}")
    print("-" * 80)

    for name in fk_by_name:
        fk = fk_by_name[name]
        if name in other_by_name:
            other = other_by_name[name]
            speedup = other.time_ms / fk.time_ms if fk.time_ms > 0 else 0
            print(f"{fk.operation:<25} {fk.rows:>10,} {fk.time_ms:>12.2f}ms {other.time_ms:>12.2f}ms {speedup:>9.2f}x")
        else:
            print(f"{fk.operation:<25} {fk.rows:>10,} {fk.time_ms:>12.2f}ms {'N/A':>15} {'N/A':>10}")


async def main():
    """Run all benchmarks."""
    print("Running ForeignKey benchmarks...")
    suite = BenchmarkSuite()

    # Run ormkit benchmarks
    fk_results = await benchmark_ormkit_sqlite()
    for r in fk_results:
        suite.add(r)
    print(f"  Completed {len(fk_results)} ormkit benchmarks")

    # Run comparison benchmarks
    other_results = await benchmark_aiosqlite()
    for r in other_results:
        suite.add(r)
    if other_results:
        print(f"  Completed {len(other_results)} aiosqlite benchmarks")

    # Print comparison
    if other_results:
        print_comparison(fk_results, other_results)

    # Save results
    output_dir = Path(__file__).parent
    results_file = output_dir / "results.json"
    with open(results_file, "w") as f:
        f.write(suite.to_json())
    print(f"\nResults saved to {results_file}")

    # Save markdown
    md_file = output_dir / "RESULTS.md"
    with open(md_file, "w") as f:
        f.write(suite.to_markdown())
    print(f"Markdown saved to {md_file}")


if __name__ == "__main__":
    asyncio.run(main())
