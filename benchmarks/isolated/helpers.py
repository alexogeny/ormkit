"""Shared helpers for isolated benchmarks."""

import asyncio
import gc
import json
import os
import time
from typing import Any, Callable, Coroutine

# Configuration from environment
ROW_COUNT = int(os.environ.get("BENCH_ROW_COUNT", "1000"))
POSTGRES_URL = os.environ.get("POSTGRES_URL", "postgresql://bench:bench@localhost:5499/bench")
ITERATIONS = 50
WARMUP = 15  # Increased for JIT warmup


async def timeit(fn: Callable[[], Coroutine[Any, Any, Any]], iterations: int = ITERATIONS) -> float:
    """Time an async function, return average time in ms."""
    # Warmup
    for _ in range(WARMUP):
        await fn()

    # Force GC before timing
    gc.collect()

    # Benchmark
    start = time.perf_counter()
    for _ in range(iterations):
        await fn()
    return (time.perf_counter() - start) / iterations * 1000


def output_results(results: list[dict[str, Any]]) -> None:
    """Output results as JSON to stdout."""
    print(json.dumps(results))


def make_result(orm: str, operation: str, rows: int, time_ms: float) -> dict[str, Any]:
    """Create a result dict."""
    return {
        "orm": orm,
        "operation": operation,
        "rows": rows,
        "time_ms": time_ms,
    }
