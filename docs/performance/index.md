# Performance

OrmKit is designed to be the fastest Python ORM. This section covers benchmarks and optimization techniques.

## Key Performance Features

<div class="feature-grid" markdown>

<div class="feature-card" markdown>
### :material-language-rust: Rust Core
Query execution, connection pooling, and type conversion all happen in compiled Rust code—not interpreted Python.
</div>

<div class="feature-card" markdown>
### :material-sleep: Lazy Conversion
Data stays in Rust until you access it. A 10,000 row result doesn't convert anything until you iterate.
</div>

<div class="feature-card" markdown>
### :material-flash: Fast Model Creation
Model instantiation is 4.9x faster than pure Python `__init__` calls.
</div>

<div class="feature-card" markdown>
### :material-pipe: Pipelined Transactions
BEGIN, queries, and COMMIT are pipelined to minimize round trips.
</div>

</div>

## Quick Comparison

| Operation | OrmKit | asyncpg | SQLAlchemy |
|-----------|------------|---------|------------|
| SELECT by ID | **0.04ms** | 0.08ms | 0.27ms |
| INSERT single | **0.04ms** | 0.08ms | 0.15ms |
| Transaction (RMW) | **0.21ms** | 0.21ms | 0.45ms |
| SELECT * (10K rows) | **4.45ms** | 5.98ms | 66ms |

## Sections

- [**Benchmarks**](benchmarks.md) - Detailed benchmark methodology and results
- [**Optimization Guide**](optimization.md) - Tips for maximum performance

## Why is OrmKit Fast?

### 1. No Python Interpreter Overhead

Traditional Python ORMs execute everything in the Python interpreter:

```
asyncpg → Python dict → ORM model → Your code
         (slow)        (slow)
```

OrmKit moves the expensive parts to Rust:

```
Rust (sqlx) → Rust LazyRow → Rust model creation → Python object
              (instant)      (4.9x faster)          (minimal)
```

### 2. Deferred BEGIN

Most ORMs send BEGIN immediately when you start a transaction:

```
Traditional:  BEGIN → wait → query → wait → COMMIT → wait  (3 round trips)
OrmKit:   [query with pipelined BEGIN] → wait → COMMIT → wait  (2 round trips)
```

### 3. Statement Caching

Prepared statements are cached in Rust. The second time you run a query, it skips parsing:

```python
# First call: Parse + Execute (~0.15ms)
await session.get(User, 1)

# Second call: Execute only (~0.04ms)
await session.get(User, 2)
```

### 4. Simple Query Protocol for COMMIT

COMMIT uses PostgreSQL's simple query protocol instead of extended protocol—one message instead of four.
