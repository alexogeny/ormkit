---
title: OrmKit - Blazingly Fast Python ORM
description: A Python ORM powered by Rust with custom database drivers
---

<div class="hero" markdown>

# OrmKit

**A blazingly fast Python ORM powered by Rust**

The speed of raw SQL drivers with the convenience of a full-featured ORM.

<div class="button-group">
[Get Started](getting-started/quickstart.md){ .md-button .md-button--primary }
[View Benchmarks](performance/benchmarks.md){ .md-button }
</div>

</div>

---

<div class="feature-grid" markdown>

<div class="feature-card" markdown>

### :material-speedometer: Faster Than asyncpg

OrmKit beats asyncpg on single-row operations and transactions. Up to **16x faster** than other ORMs on SELECT queries.

</div>

<div class="feature-card" markdown>

### :material-language-rust: Rust-Powered Core

Query execution, connection pooling, and model instantiation all happen in Rust. **4.9x faster** model creation than pure Python.

</div>

<div class="feature-card" markdown>

### :material-battery-charging: Batteries Included

No need for asyncpg or aiosqlite. PostgreSQL and SQLite drivers are built-in with custom Rust implementations.

</div>

<div class="feature-card" markdown>

### :material-api: Familiar API

SQLAlchemy-style declarative models with Django-style query filters. If you know one, you'll feel right at home.

</div>

<div class="feature-card" markdown>

### :material-async: Async-First

Native async/await support throughout. No sync wrappers or compatibility layers.

</div>

<div class="feature-card" markdown>

### :material-shield-check: Type Safe

Full type hints with `Mapped[]` annotations. Works great with Pyright and mypy.

</div>

</div>

---

## Quick Example

```python
from ormkit import Base, Mapped, mapped_column, create_engine, AsyncSession

class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(max_length=100)
    email: Mapped[str] = mapped_column(unique=True)

async def main():
    engine = await create_engine("postgresql://localhost/mydb")
    session = AsyncSession(engine)

    # Insert - returns model with generated ID
    user = await session.insert(User(name="Alice", email="alice@example.com"))

    # Query with Django-style filters
    adults = await session.query(User).filter(age__gte=18).all()

    # Eager loading relationships
    users = await session.query(User).options(selectinload("posts")).all()
```

---

## Performance at a Glance

| Operation | OrmKit | asyncpg | vs Fastest |
|-----------|--------|---------|------------|
| SELECT by ID | **0.04ms** | 0.08ms | :material-trophy: **fastest** |
| INSERT single | **0.04ms** | 0.08ms | :material-trophy: **fastest** |
| UPDATE single | **0.05ms** | 0.09ms | :material-trophy: **fastest** |
| Transaction (RMW) | **0.21ms** | 0.21ms | :material-check: tied |
| Bulk INSERT (100) | **0.47ms** | 3.14ms | :material-trophy: **6.7x faster** |

<small>*Benchmarks run on PostgreSQL 16 with 1,000 rows. See [full benchmarks](performance/benchmarks.md) for methodology.*</small>

---

## Why OrmKit?

### The Problem

Python ORMs are slow. Even the fastest async drivers like asyncpg lose performance when you add an ORM layer on top:

```
asyncpg (raw)           ████████████████████████  0.08ms
asyncpg + SQLAlchemy    ████████████████████████████████████████████████  0.27ms
asyncpg + Tortoise      ████████████████████████████████████████████  0.13ms
```

### The Solution

OrmKit moves the expensive work to Rust:

- **Query execution** happens in Rust (custom wire protocol for PostgreSQL)
- **Connection pooling** happens in Rust (no Python asyncio overhead)
- **Model instantiation** happens in Rust (4.9x faster than Python `__init__`)
- **Type conversion** happens in Rust (lazy, only when accessed)

The result: **ORM convenience at driver-level speed**.

---

## Key Features

### Advanced Query Capabilities

- **Django-style filters**: `age__gte`, `name__contains`, `status__in`, `deleted_at__isnull`
- **Q objects** for complex queries: `Q(age__gt=18) | Q(vip=True)`
- **Aggregates**: `count()`, `sum()`, `avg()`, `min()`, `max()`
- **Streaming** for large datasets: `query.stream(batch_size=1000)`

### Relationship Support

- **One-to-many** and **many-to-one** relationships
- **Many-to-many** with junction tables
- **Eager loading**: `selectinload`, `joinedload`, `noload`

### Enterprise Features

- **Soft delete** with `SoftDeleteMixin`
- **Upsert** (INSERT ... ON CONFLICT)
- **JSON columns** with nested field queries
- **Migrations** (Alembic-compatible)

---

## Installation

=== "pip"

    ```bash
    pip install ormkit
    ```

=== "uv"

    ```bash
    uv add ormkit
    ```

=== "poetry"

    ```bash
    poetry add ormkit
    ```

---

## Supported Databases

| Database | Connection String |
|----------|-------------------|
| PostgreSQL 12+ | `postgresql://user:pass@host:port/dbname` |
| SQLite 3 | `sqlite:///path/to/db.sqlite` |
| SQLite (memory) | `sqlite::memory:` |

---

## Ready to Start?

<div class="button-group">
[Installation Guide](getting-started/installation.md){ .md-button .md-button--primary }
[Quick Start Tutorial](getting-started/quickstart.md){ .md-button }
[API Reference](api/index.md){ .md-button }
</div>
