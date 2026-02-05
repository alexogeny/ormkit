# Benchmarks

Comprehensive benchmarks comparing OrmKit against popular Python database libraries.

## Methodology

- **Database**: PostgreSQL 16 running locally
- **Python**: 3.12
- **Iterations**: 50 per operation (after 15 warmup iterations)
- **Best of 3**: Each benchmark suite runs 3 times, keeping the fastest result
- **Connection pool**: Single connection to ensure fair comparison

All benchmarks are available in the `benchmarks/` directory of the repository.

## Single-Row Operations

These operations are critical for web applications where latency matters.

### SELECT by ID

| ORM | Time (ms) | Rows/sec | vs Fastest |
|-----|-----------|----------|------------|
| **OrmKit** | **0.04** | **22,265** | :material-trophy: fastest |
| asyncpg | 0.08 | 12,269 | 1.81x |
| databases | 0.11 | 9,357 | 2.38x |
| Tortoise ORM | 0.13 | 7,606 | 2.93x |
| SQLAlchemy 2.0 | 0.27 | 3,673 | 6.06x |
| Ormar | 0.37 | 2,685 | 8.29x |

### INSERT Single Row

| ORM | Time (ms) | Rows/sec | vs Fastest |
|-----|-----------|----------|------------|
| **OrmKit** | **0.04** | **22,999** | :material-trophy: fastest |
| asyncpg | 0.08 | 12,408 | 1.85x |
| Ormar | 0.26 | 3,816 | 6.03x |

### UPDATE Single Row

| ORM | Time (ms) | Rows/sec | vs Fastest |
|-----|-----------|----------|------------|
| **OrmKit** | **0.05** | **20,669** | :material-trophy: fastest |
| asyncpg | 0.09 | 11,495 | 1.80x |
| Ormar | 0.69 | 1,453 | 14.23x |

### DELETE Single Row

| ORM | Time (ms) | Rows/sec | vs Fastest |
|-----|-----------|----------|------------|
| **OrmKit** | **0.05** | **21,485** | :material-trophy: fastest |
| asyncpg | 0.08 | 12,101 | 1.78x |
| Ormar | 0.32 | 3,077 | 6.98x |

## Bulk Operations

### SELECT * (100 rows)

| ORM | Time (ms) | Rows/sec | vs Fastest |
|-----|-----------|----------|------------|
| **OrmKit (raw)** | **0.10** | **1,017,221** | :material-trophy: fastest |
| OrmKit (tuples) | 0.11 | 939,330 | 1.08x |
| OrmKit (dicts) | 0.11 | 936,743 | 1.09x |
| asyncpg (raw) | 1.63 | 61,432 | 16.56x |
| asyncpg + hydration | 3.93 | 25,464 | 39.95x |
| databases | 5.10 | 19,619 | 51.85x |
| SQLAlchemy 2.0 | 24.49 | 4,084 | 249.08x |
| Tortoise ORM | 26.26 | 3,808 | 267.10x |
| Ormar | 157.02 | 637 | 1597.29x |

### SELECT * (1,000 rows)

| ORM | Time (ms) | Rows/sec | vs Fastest |
|-----|-----------|----------|------------|
| **OrmKit (raw)** | **0.52** | **1,907,383** | :material-trophy: fastest |
| OrmKit (tuples) | 0.57 | 1,754,909 | 1.09x |
| OrmKit (dicts) | 0.62 | 1,622,139 | 1.18x |
| asyncpg (raw) | 2.04 | 490,531 | 3.89x |
| asyncpg + hydration | 4.88 | 204,818 | 9.31x |
| databases | 5.59 | 178,734 | 10.67x |
| SQLAlchemy 2.0 | 26.15 | 38,236 | 49.89x |
| Tortoise ORM | 29.62 | 33,766 | 56.49x |
| Ormar | 178.32 | 5,608 | 340.12x |

### SELECT * (10,000 rows)

| ORM | Time (ms) | Rows/sec | vs Fastest |
|-----|-----------|----------|------------|
| **OrmKit (raw)** | **4.45** | **2,248,862** | :material-trophy: fastest |
| OrmKit (tuples) | 5.35 | 1,867,817 | 1.20x |
| OrmKit (dicts) | 5.67 | 1,762,501 | 1.28x |
| asyncpg (raw) | 5.98 | 1,670,857 | 1.35x |
| databases | 12.46 | 802,479 | 2.80x |
| asyncpg + hydration | 14.79 | 676,131 | 3.33x |
| Tortoise ORM | 60.61 | 164,999 | 13.63x |
| SQLAlchemy 2.0 | 66.19 | 151,072 | 14.89x |
| Ormar | 373.66 | 26,763 | 84.03x |

## Transaction Operations

### Transaction (Read-Modify-Write)

A realistic transaction: SELECT → modify in Python → UPDATE → SELECT to verify.

| ORM | Time (ms) | Ops/sec | vs Fastest |
|-----|-----------|---------|------------|
| asyncpg | 0.21 | 4,793 | :material-trophy: fastest |
| **OrmKit** | **0.21** | **4,683** | 1.02x |

!!! note "Competitive with Raw Driver"
    OrmKit matches asyncpg's transaction performance despite being a full ORM.

### Bulk INSERT (100 rows in transaction)

| ORM | Time (ms) | Rows/sec | vs Fastest |
|-----|-----------|----------|------------|
| **OrmKit** | **0.47** | **212,091** | :material-trophy: fastest |
| asyncpg | 3.14 | 31,886 | 6.65x |

OrmKit's bulk insert is **6.7x faster** than asyncpg because it uses a single multi-value INSERT statement.

## Model Instantiation

Time to convert 10,000 database rows to Python model instances:

| Method | Time (ms) | vs Python __init__ |
|--------|-----------|-------------------|
| Raw tuples | 0.96 | - |
| Raw dicts | 1.10 | - |
| **Rust → Python models** | **2.00** | **4.9x faster** |
| Python `_from_row_fast` | 9.60 | baseline |
| Python `__init__` | 11.60 | 0.83x |

OrmKit's Rust-powered model creation is **4.9x faster** than equivalent Python code.

## Time Breakdown

For a 10,000 row SELECT operation:

```
┌─────────────────────────────────────────────┐
│ SQL execution + network    85.6%  (9.6ms)   │
├─────────────────────────────────────────────┤
│ Python conversion          14.4%  (1.6ms)   │
└─────────────────────────────────────────────┘
```

**Key insight**: SQL execution dominates. OrmKit's conversion layer is already highly optimized—most time is spent waiting for PostgreSQL.

## Running Benchmarks

```bash
# Clone the repository
git clone https://github.com/yourusername/ormkit.git
cd ormkit

# Start PostgreSQL (Docker)
docker run -d --name bench-postgres \
  -e POSTGRES_USER=bench \
  -e POSTGRES_PASSWORD=bench \
  -e POSTGRES_DB=bench \
  -p 5499:5432 \
  postgres:16

# Install dependencies
uv sync --group bench

# Build OrmKit
uv run maturin develop --release

# Run benchmarks
uv run python benchmarks/runner.py postgres

# Run with more iterations for stability
uv run python benchmarks/runner.py postgres --runs 5
```

## Benchmark Caveats

1. **Local database** - Results will differ with network latency
2. **Single connection** - Real apps use connection pools
3. **Synthetic data** - Real queries may be more complex
4. **Warm cache** - First query is slower due to statement parsing

For realistic performance expectations, run these benchmarks on your own infrastructure.
