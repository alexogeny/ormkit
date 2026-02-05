# Optimization Guide

Tips for getting the best performance from OrmKit.

## Connection Pooling

### Configure Pool Size

```python
engine = await create_engine(
    "postgresql://localhost/mydb",
    min_connections=5,    # Minimum idle connections
    max_connections=20,   # Maximum total connections
)
```

**Guidelines:**
- `min_connections`: Set to your baseline concurrent request count
- `max_connections`: Set to peak concurrent requests (don't exceed PostgreSQL's `max_connections`)

### Reuse the Engine

Create one engine at application startup:

```python
# Good: Single engine for the app
engine = await create_engine("postgresql://localhost/mydb")

# Bad: Creating engine per request
async def handle_request():
    engine = await create_engine(...)  # Don't do this!
```

## Query Optimization

### Use Indexes

Add indexes to columns you filter on:

```python
class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    email: Mapped[str] = mapped_column(unique=True, index=True)  # Indexed
    status: Mapped[str] = mapped_column(index=True)              # Indexed
    created_at: Mapped[datetime] = mapped_column(index=True)     # Indexed
```

### Limit Results

Always limit when you don't need all rows:

```python
# Good: Only fetch what you need
recent = await session.query(Post).order_by("-created_at").limit(10).all()

# Bad: Fetch everything then slice in Python
all_posts = await session.query(Post).order_by("-created_at").all()
recent = all_posts[:10]  # Wasteful!
```

### Use count() and exists()

```python
# Good: Let the database count
total = await session.query(User).filter(status="active").count()

# Bad: Fetch all and count in Python
users = await session.query(User).filter(status="active").all()
total = len(users)  # Downloaded all data just to count!

# Good: Check existence efficiently
has_admin = await session.query(User).filter(role="admin").exists()

# Bad: Fetch to check
admins = await session.query(User).filter(role="admin").limit(1).all()
has_admin = len(admins) > 0
```

### Select Only Needed Columns

For large tables, select specific columns with raw SQL:

```python
# When you only need id and name from a table with 50 columns
result = await engine.execute(
    "SELECT id, name FROM users WHERE status = $1",
    ["active"]
)
```

## Result Format Selection

Choose the right result format for your use case:

### Use tuples() for Processing

```python
result = await engine.execute("SELECT id, name, email FROM users", [])

# 15% faster than dicts
for id, name, email in result.tuples():
    process(id, name, email)
```

### Use column() for Single Values

```python
result = await engine.execute("SELECT email FROM users WHERE status = $1", ["active"])
emails = result.column("email")  # ["alice@example.com", "bob@example.com"]
```

### Use first() for Single Row

```python
result = await engine.execute(
    "SELECT * FROM users WHERE id = $1", [user_id]
)
user = result.first()  # Single dict, not a list
```

## Eager Loading Strategy

### selectinload for Collections

```python
# Good: One query for users, one for all their posts
users = await session.query(User).options(selectinload("posts")).all()
```

### joinedload for Single Objects

```python
# Good: Single query with JOIN
posts = await session.query(Post).options(joinedload("author")).all()
```

### Avoid N+1 Queries

```python
# Bad: N+1 queries (1 for users + N for posts)
users = await session.query(User).all()
for user in users:
    posts = await session.query(Post).filter(author_id=user.id).all()

# Good: 2 queries total
users = await session.query(User).options(selectinload("posts")).all()
for user in users:
    for post in user.posts:
        print(post.title)
```

## Bulk Operations

### Use insert_all() for Multiple Rows

```python
# Good: Single INSERT with multiple VALUES
users = await session.insert_all([
    User(name="Alice", email="alice@example.com"),
    User(name="Bob", email="bob@example.com"),
    User(name="Charlie", email="charlie@example.com"),
])

# Bad: Multiple INSERT statements
for data in user_data:
    await session.insert(User(**data))
```

### Use Bulk DELETE

```python
# Good: Single DELETE statement
deleted = await session.query(User).filter(status="inactive").delete()

# Bad: Delete one by one
users = await session.query(User).filter(status="inactive").all()
for user in users:
    await session.remove(user)
```

## Transaction Best Practices

### Keep Transactions Short

```python
# Good: Quick transaction
async with session.begin() as tx:
    tx.add(User(name="Alice"))
    tx.add(AuditLog(action="user_created"))

# Bad: Long transaction with external I/O
async with session.begin() as tx:
    tx.add(User(name="Alice"))
    await send_welcome_email()  # Holds transaction open!
    tx.add(AuditLog(action="email_sent"))
```

### Batch in Transactions

```python
# Good: Batch operations in one transaction
async with session.begin() as tx:
    for user_data in users_to_create:
        tx.add(User(**user_data))
# Single COMMIT at the end

# Slower: Each insert is its own transaction
for user_data in users_to_create:
    await session.insert(User(**user_data))
# N separate COMMITs
```

## Caching

### Application-Level Caching

For frequently-accessed, rarely-changing data:

```python
from functools import lru_cache
import asyncio

# Simple in-memory cache
_user_cache: dict[int, User] = {}

async def get_user_cached(user_id: int) -> User | None:
    if user_id in _user_cache:
        return _user_cache[user_id]

    user = await session.get(User, user_id)
    if user:
        _user_cache[user_id] = user
    return user

def invalidate_user(user_id: int) -> None:
    _user_cache.pop(user_id, None)
```

### Query Result Caching

For expensive queries:

```python
import hashlib
import json

_query_cache: dict[str, tuple[float, list]] = {}
CACHE_TTL = 60  # seconds

async def cached_query(query_key: str, query_fn):
    import time
    now = time.time()

    if query_key in _query_cache:
        cached_time, result = _query_cache[query_key]
        if now - cached_time < CACHE_TTL:
            return result

    result = await query_fn()
    _query_cache[query_key] = (now, result)
    return result

# Usage
stats = await cached_query(
    "daily_stats",
    lambda: engine.execute("SELECT ... complex aggregation ...", [])
)
```

## Monitoring

### Log Slow Queries

```python
import time
import logging

logger = logging.getLogger("ormkit")

async def execute_with_logging(sql: str, params: list):
    start = time.perf_counter()
    result = await engine.execute(sql, params)
    elapsed = time.perf_counter() - start

    if elapsed > 0.1:  # Log queries > 100ms
        logger.warning(f"Slow query ({elapsed:.3f}s): {sql[:100]}")

    return result
```

## Summary

1. **Connection pool**: Size appropriately for your workload
2. **Indexes**: Add to frequently-filtered columns
3. **Limit results**: Don't fetch more than needed
4. **Eager loading**: Avoid N+1 with selectinload/joinedload
5. **Bulk operations**: Use insert_all() and bulk delete
6. **Short transactions**: Don't hold locks longer than necessary
7. **Right result format**: tuples() > dicts > models for speed
8. **Cache**: For frequently-accessed, stable data
