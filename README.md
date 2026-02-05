# OrmKit

A blazingly fast Python ORM powered by Rust.

## Features

- **SQLAlchemy-like API**: Familiar declarative model syntax with `Mapped[]` type hints
- **Custom Rust Drivers**: PostgreSQL driver with custom wire protocol, SQLite via rusqlite - no external dependencies
- **Async-first**: Native async/await support throughout
- **PostgreSQL + SQLite**: Production and development databases covered
- **Multiple API styles**: From simple one-liners to full Unit of Work pattern
- **Relationships**: One-to-many, many-to-one, and many-to-many with eager loading
- **Django-style Queries**: Intuitive filter operators (`age__gt`, `name__like`, `tags__in`, etc.)
- **Lazy Row Conversion**: Data stays in Rust until accessed - minimizes Python/Rust boundary crossings
- **Advanced Features**: Soft delete, upsert, JSON columns, migrations, Q objects for complex queries

## Installation

```bash
pip install ormkit
```

## Quick Start

### Define Your Models

```python
from ormkit import (
    Base, Mapped, mapped_column, ForeignKey, relationship,
    create_engine, AsyncSession, selectinload, JSON
)

class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(max_length=100)
    email: Mapped[str] = mapped_column(unique=True)
    age: Mapped[int | None] = mapped_column(nullable=True)
    metadata: Mapped[dict] = mapped_column(JSON)  # JSONB on PostgreSQL

    # One-to-many relationship
    posts: Mapped[list["Post"]] = relationship(back_populates="author")


class Post(Base):
    __tablename__ = "posts"

    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column(max_length=200)
    author_id: Mapped[int] = mapped_column(ForeignKey("users.id"))

    # Many-to-one relationship
    author: Mapped[User] = relationship(back_populates="posts")
```

### Simple API (Recommended)

```python
async def main():
    engine = await create_engine("postgresql://localhost/mydb")
    # Or for SQLite:
    # engine = await create_engine("sqlite:///mydb.sqlite")

    session = AsyncSession(engine)

    # Insert - returns model with generated ID
    user = await session.insert(User(name="Alice", email="alice@example.com"))
    print(f"Created user with id={user.id}")

    # Query with Django-style filters
    adults = await session.query(User).filter(age__gte=18).all()
    user = await session.query(User).filter(email="alice@example.com").first()

    # Get by primary key
    user = await session.get(User, 1)

    # Update
    await session.update(user, name="Alicia", age=26)

    # Delete
    await session.remove(user)
```

### Relationships & Eager Loading

```python
# Load users with their posts in a single query
users = await session.query(User).options(selectinload("posts")).all()

for user in users:
    print(f"{user.name} has {len(user.posts)} posts")
    for post in user.posts:
        print(f"  - {post.title}")

# Load posts with their authors
posts = await session.query(Post).options(joinedload("author")).all()

for post in posts:
    print(f"{post.title} by {post.author.name}")
```

### Transaction Context (Auto-commit)

```python
from ormkit import session_context

async with session_context(engine) as session:
    await session.insert(User(name="Alice", email="alice@example.com"))
    await session.insert(User(name="Bob", email="bob@example.com"))
    # Commits automatically on exit, rolls back on exception
```

### Batch Operations

```python
async with session.begin() as tx:
    tx.add(User(name="Alice", email="alice@example.com"))
    tx.add(User(name="Bob", email="bob@example.com"))
    tx.add(User(name="Charlie", email="charlie@example.com"))
    # Commits automatically

# Or use insert_all for bulk inserts
users = await session.insert_all([
    User(name="User1", email="user1@example.com"),
    User(name="User2", email="user2@example.com"),
    User(name="User3", email="user3@example.com"),
])
```

### Query Builder (Django-style filters)

```python
# Comparison operators
users = await session.query(User).filter(age__gt=18).all()       # age > 18
users = await session.query(User).filter(age__gte=18).all()      # age >= 18
users = await session.query(User).filter(age__lt=65).all()       # age < 65
users = await session.query(User).filter(age__lte=65).all()      # age <= 65
users = await session.query(User).filter(age__ne=0).all()        # age != 0

# Pattern matching
users = await session.query(User).filter(name__like="A%").all()      # LIKE pattern
users = await session.query(User).filter(name__ilike="a%").all()     # Case-insensitive (PostgreSQL)
users = await session.query(User).filter(name__contains="ali").all() # Contains substring
users = await session.query(User).filter(name__startswith="A").all() # Starts with
users = await session.query(User).filter(name__endswith="e").all()   # Ends with

# IN and NOT IN
users = await session.query(User).filter(role__in=["admin", "mod"]).all()
users = await session.query(User).filter(status__notin=["banned", "deleted"]).all()

# NULL checks
users = await session.query(User).filter(deleted_at__isnull=True).all()

# Multiple filters (AND)
users = await session.query(User).filter(age__gte=18, age__lt=65).all()

# Complex queries with Q objects (OR, AND, NOT)
from ormkit import Q
users = await session.query(User).filter(
    Q(age__gt=18) | Q(vip=True)
).all()

# Chaining
users = await session.query(User) \
    .filter(age__gte=18) \
    .order_by("-created_at") \
    .limit(10) \
    .offset(20) \
    .all()

# Aggregates
count = await session.query(User).filter(age__gte=18).count()
total = await session.query(Order).filter(status="completed").sum("amount")
avg_age = await session.query(User).avg("age")
exists = await session.query(User).filter(email="admin@example.com").exists()

# Bulk operations
deleted = await session.query(User).filter(age__lt=18).delete()
updated = await session.query(User).filter(role="guest").update(role="member")
```

### JSON Column Queries

```python
# Query nested JSON fields
users = await session.query(User).filter(metadata__plan="premium").all()
users = await session.query(User).filter(metadata__settings__theme="dark").all()
```

### Upsert (INSERT ... ON CONFLICT)

```python
# Insert or update on conflict
user = await session.upsert(
    User(email="alice@example.com", name="Alice"),
    conflict_target="email",
    update_fields=["name"]
)

# Bulk upsert
users = await session.upsert_all(
    [User(email="a@example.com", name="A"), User(email="b@example.com", name="B")],
    conflict_target="email",
    update_fields=["name"]
)
```

### Soft Delete

```python
from ormkit import SoftDeleteMixin

class Article(SoftDeleteMixin, Base):
    __tablename__ = "articles"
    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str]

# Soft delete (sets deleted_at timestamp)
await session.soft_delete(article)

# Restore a soft-deleted record
await session.restore(article)

# Query excludes soft-deleted by default
articles = await session.query(Article).all()  # Only non-deleted

# Include soft-deleted records
all_articles = await session.query(Article).with_deleted().all()

# Query only soft-deleted records
deleted = await session.query(Article).only_deleted().all()

# Permanently delete
await session.force_delete(article)
```

### Streaming Large Result Sets

```python
# Process large datasets without loading all into memory
async for user in session.query(User).stream(batch_size=1000):
    process_user(user)
```

### Eager Loading Options

```python
from ormkit import selectinload, joinedload, noload

# selectinload - loads using SELECT IN query (best for collections)
users = await session.query(User).options(selectinload("posts")).all()

# joinedload - loads using JOIN (best for single objects)
posts = await session.query(Post).options(joinedload("author")).all()

# noload - explicitly disable loading
users = await session.query(User).options(noload("posts")).all()
# user.posts will be empty []

# Chain multiple options
users = await session.query(User).options(
    selectinload("posts"),
    selectinload("profile"),
).all()
```

### Traditional SQLAlchemy-style API

```python
from ormkit import select

async with AsyncSession(engine) as session:
    # Manual add/commit
    user = User(name="Alice", email="alice@example.com")
    session.add(user)
    await session.commit()

    # Query with select()
    stmt = select(User).where(User.age >= 18)
    result = await session.execute(stmt)
    users = result.scalars().all()
```

### Raw SQL Queries

```python
# Execute raw SQL
result = await session.execute_raw(
    "SELECT * FROM users WHERE age > ?",
    [18]
)
for row in result.all():
    print(row["name"], row["age"])

# Get results as tuples (faster for large result sets)
result = await engine.execute("SELECT id, name FROM users", [])
tuples = result.tuples()  # [(1, "Alice"), (2, "Bob"), ...]

# Get a single column
names = result.column("name")  # ["Alice", "Bob", ...]
```

## API Reference

### Model Definition

| Function | Description |
|----------|-------------|
| `mapped_column(primary_key=False, nullable=False, unique=False, index=False, default=None, max_length=None)` | Define a database column |
| `ForeignKey("table.column", ondelete=None, onupdate=None)` | Define a foreign key reference |
| `relationship(back_populates=None, lazy="select", uselist=None, secondary=None)` | Define a relationship |
| `JSON` | Marker for JSON/JSONB columns |

### Session Methods

| Method | Description |
|--------|-------------|
| `session.insert(instance)` | Insert and return with generated ID |
| `session.insert_all(instances)` | Bulk insert multiple instances |
| `session.get(Model, id)` | Get by primary key |
| `session.get_or_raise(Model, id)` | Get by primary key, raise if not found |
| `session.update(instance, **values)` | Update an instance |
| `session.remove(instance)` | Delete an instance |
| `session.query(Model)` | Create a query builder |
| `session.upsert(instance, conflict_target, update_fields)` | Insert or update on conflict |
| `session.upsert_all(instances, conflict_target, update_fields)` | Bulk upsert |
| `session.bulk_update(Model, values, **filters)` | Bulk update matching records |
| `session.soft_delete(instance)` | Soft delete (sets deleted_at) |
| `session.restore(instance)` | Restore soft-deleted record |
| `session.force_delete(instance)` | Permanently delete |
| `session.begin()` | Start a transaction context |
| `session.commit()` | Commit pending changes |
| `session.rollback()` | Rollback pending changes |

### Query Methods

| Method | Description |
|--------|-------------|
| `query.filter(**kwargs)` | Filter with Django-style operators |
| `query.filter_by(**kwargs)` | Filter with exact matches |
| `query.order_by(*columns)` | Order results (prefix with `-` for DESC) |
| `query.limit(n)` | Limit results |
| `query.offset(n)` | Offset results |
| `query.distinct()` | Return distinct results |
| `query.group_by(*columns)` | Group by columns |
| `query.having(**kwargs)` | Filter on aggregates |
| `query.options(*load_options)` | Add eager loading options |
| `query.all()` | Get all results |
| `query.first()` | Get first result |
| `query.one()` | Get exactly one result (raises if not 1) |
| `query.one_or_none()` | Get one or None (raises if > 1) |
| `query.count()` | Count matching rows |
| `query.sum(column)` | Sum of column values |
| `query.avg(column)` | Average of column values |
| `query.min(column)` | Minimum value |
| `query.max(column)` | Maximum value |
| `query.exists()` | Check if any rows match |
| `query.delete()` | Delete matching rows |
| `query.update(**values)` | Update matching rows |
| `query.values(*columns)` | Return dicts with specific columns |
| `query.values_list(*columns)` | Return tuples with specific columns |
| `query.stream(batch_size)` | Stream results in batches |
| `query.with_deleted()` | Include soft-deleted records |
| `query.only_deleted()` | Return only soft-deleted records |

### Filter Operators

| Operator | SQL | Example |
|----------|-----|---------|
| (none) | `=` | `filter(name="Alice")` |
| `__gt` | `>` | `filter(age__gt=18)` |
| `__gte` | `>=` | `filter(age__gte=18)` |
| `__lt` | `<` | `filter(age__lt=65)` |
| `__lte` | `<=` | `filter(age__lte=65)` |
| `__ne` | `!=` | `filter(status__ne="deleted")` |
| `__in` | `IN` | `filter(role__in=["admin", "mod"])` |
| `__notin` | `NOT IN` | `filter(status__notin=["banned"])` |
| `__like` | `LIKE` | `filter(name__like="A%")` |
| `__ilike` | `ILIKE` | `filter(name__ilike="a%")` (PostgreSQL) |
| `__contains` | `LIKE %x%` | `filter(name__contains="ali")` |
| `__icontains` | `ILIKE %x%` | `filter(name__icontains="ali")` |
| `__startswith` | `LIKE x%` | `filter(name__startswith="A")` |
| `__endswith` | `LIKE %x` | `filter(name__endswith="e")` |
| `__isnull` | `IS NULL` / `IS NOT NULL` | `filter(deleted_at__isnull=True)` |

## Benchmarks

### Single Row Queries (Where Latency Matters)

| Operation | OrmKit | aiosqlite | Notes |
|-----------|--------|-----------|-------|
| Single row by ID | 0.036ms | 0.036ms | **Identical performance** |

For typical web application queries, OrmKit matches raw aiosqlite performance.

### Bulk Operations (10,000 rows)

| Operation | OrmKit | aiosqlite | Relative |
|-----------|--------|-----------|----------|
| SELECT * (tuples) | 11ms | 5.7ms | 0.52x |
| SELECT * (dicts) | 11ms | 6.5ms | 0.59x |
| Bulk Insert | 2.3ms | 2.0ms | 0.87x |

### Model Instantiation (10,000 rows)

This is where Rust shines - converting raw data to ORM model instances:

| Method | Time | vs Pure Python |
|--------|------|----------------|
| Raw tuples | 0.96ms | - |
| Raw dicts | 1.1ms | - |
| **Rust → Python models** | **2.0ms** | **4.9x faster** |
| Python `_from_row_fast` | 9.6ms | baseline |
| Python `__init__` | 11.6ms | 0.83x |

OrmKit's Rust-powered model instantiation is **4.9x faster** than pure Python.

### Time Breakdown (10,000 row SELECT)

```
SQL execution + fetch:  85.6%  (9.6ms)  - Rust driver layer
Python conversion:      14.4%  (1.6ms)  - highly optimized
```

For maximum speed, use `result.tuples()` or `result.column()` methods which bypass model creation entirely.

## Development

```bash
# Install dependencies
uv venv && source .venv/bin/activate
uv pip install maturin pytest pytest-asyncio

# Build the Rust extension (debug)
maturin develop

# Build with optimizations (release)
maturin develop --release

# Run tests
pytest tests/ -v

# Run benchmarks
python benchmarks/run_all.py
```

## Architecture

OrmKit uses a layered architecture:

```
┌─────────────────────────────────────────────────────────────┐
│                    Python Layer                             │
│  - Declarative model definitions (like SQLAlchemy 2.0)      │
│  - Type hints with Mapped[] / mapped_column()               │
│  - Pythonic query builder API                               │
│  - Async session management                                 │
│  - Relationship loading (selectinload, joinedload)          │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Rust Core (PyO3)                         │
│  - Query execution                                          │
│  - Connection pool management                               │
│  - Lazy row conversion (data stays in Rust until accessed)  │
│  - Type-safe parameter binding                              │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   Custom Rust Drivers                       │
│  - PostgreSQL (custom wire protocol implementation)         │
│  - SQLite (rusqlite with tokio async wrapper)               │
└─────────────────────────────────────────────────────────────┘
```

## Supported Databases

- **PostgreSQL 12+** - `postgresql://user:pass@host:port/dbname`
- **SQLite 3** - `sqlite:///path/to/db.sqlite` or `sqlite::memory:`

## Python Type Support

| Python Type | PostgreSQL | SQLite |
|-------------|------------|--------|
| `int` | INTEGER/SERIAL | INTEGER |
| `str` | TEXT/VARCHAR | TEXT |
| `float` | DOUBLE PRECISION | REAL |
| `bool` | BOOLEAN | INTEGER (0/1) |
| `bytes` | BYTEA | BLOB |
| `datetime` | TIMESTAMP | TEXT |
| `date` | DATE | TEXT |
| `time` | TIME | TEXT |
| `dict` / `list` (JSON) | JSONB | TEXT |
| `Optional[T]` | T (nullable) | T (nullable) |

## License

MIT
