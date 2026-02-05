# Query API

The query builder provides a fluent interface for building database queries.

## Creating Queries

```python
query = session.query(User)
```

All query methods return a new query instance (immutable).

---

## Filtering

### query.filter

Filter with Django-style operators.

```python
def filter(self, *args, **kwargs) -> Query[T]
```

```python
# Exact match
query.filter(name="Alice")

# Comparison operators
query.filter(age__gt=18)      # age > 18
query.filter(age__gte=18)     # age >= 18
query.filter(age__lt=65)      # age < 65
query.filter(age__lte=65)     # age <= 65
query.filter(age__ne=0)       # age != 0

# IN / NOT IN
query.filter(role__in=["admin", "mod"])
query.filter(status__notin=["banned", "deleted"])

# Pattern matching
query.filter(name__like="A%")         # LIKE (case-sensitive)
query.filter(name__ilike="a%")        # ILIKE (case-insensitive, PostgreSQL)
query.filter(name__contains="ali")    # LIKE %ali%
query.filter(name__icontains="ali")   # ILIKE %ali%
query.filter(name__startswith="A")    # LIKE A%
query.filter(name__endswith="e")      # LIKE %e

# NULL checks
query.filter(deleted_at__isnull=True)   # IS NULL
query.filter(email__isnull=False)       # IS NOT NULL

# Multiple conditions (AND)
query.filter(age__gte=18, status="active")

# Q objects for OR
query.filter(Q(age__gt=65) | Q(role="retired"))
```

### query.filter_by

Filter with exact matches only.

```python
def filter_by(self, **kwargs) -> Query[T]
```

```python
query.filter_by(name="Alice", status="active")
```

### Operator Reference

| Suffix | SQL | Example |
|--------|-----|---------|
| (none) | `=` | `filter(status="active")` |
| `__gt` | `>` | `filter(age__gt=18)` |
| `__gte` | `>=` | `filter(age__gte=18)` |
| `__lt` | `<` | `filter(age__lt=65)` |
| `__lte` | `<=` | `filter(age__lte=65)` |
| `__ne` | `!=` | `filter(status__ne="deleted")` |
| `__in` | `IN` | `filter(role__in=["admin"])` |
| `__notin` | `NOT IN` | `filter(status__notin=["banned"])` |
| `__like` | `LIKE` | `filter(name__like="A%")` |
| `__ilike` | `ILIKE` | `filter(name__ilike="a%")` |
| `__contains` | `LIKE %x%` | `filter(name__contains="ali")` |
| `__icontains` | `ILIKE %x%` | `filter(name__icontains="ali")` |
| `__startswith` | `LIKE x%` | `filter(name__startswith="A")` |
| `__endswith` | `LIKE %x` | `filter(name__endswith="e")` |
| `__isnull` | `IS NULL` | `filter(deleted_at__isnull=True)` |

---

## Ordering

### query.order_by

Order results by columns.

```python
def order_by(self, *columns: str) -> Query[T]
```

```python
# Ascending
query.order_by("name")
query.order_by("created_at")

# Descending (prefix with -)
query.order_by("-created_at")
query.order_by("-age")

# Multiple columns
query.order_by("role", "-created_at")
```

---

## Pagination

### query.limit

Limit the number of results.

```python
def limit(self, n: int) -> Query[T]
```

```python
query.limit(10)
```

### query.offset

Skip a number of results.

```python
def offset(self, n: int) -> Query[T]
```

```python
query.offset(20).limit(10)  # Skip 20, get next 10
```

---

## Distinct and Grouping

### query.distinct

Return distinct results.

```python
def distinct(self) -> Query[T]
```

```python
query.distinct().values("role")
```

### query.group_by

Group results by columns.

```python
def group_by(self, *columns: str) -> Query[T]
```

```python
query.group_by("department").values("department", count="id")
```

### query.having

Filter on aggregate values (use after group_by).

```python
def having(self, **kwargs) -> Query[T]
```

```python
query.group_by("department").having(count__gt=5)
```

---

## Eager Loading

### query.options

Add eager loading options.

```python
def options(self, *opts: LoadOption) -> Query[T]
```

```python
from ormkit import selectinload, joinedload, noload

# Load collections
query.options(selectinload("posts"))

# Load single objects
query.options(joinedload("author"))

# Multiple relationships
query.options(
    selectinload("posts"),
    selectinload("comments"),
)

# Disable loading
query.options(noload("posts"))
```

### Load Options

| Option | Use Case | Strategy |
|--------|----------|----------|
| `selectinload(rel)` | Collections (one-to-many) | `SELECT ... WHERE id IN (...)` |
| `joinedload(rel)` | Single objects (many-to-one) | `JOIN` |
| `noload(rel)` | Explicitly skip | No query |

---

## Executing Queries

### query.all

Get all matching results.

```python
async def all(self) -> list[T]
```

```python
users = await session.query(User).filter(age__gte=18).all()
```

### query.first

Get the first result or None.

```python
async def first(self) -> T | None
```

```python
user = await session.query(User).filter(email="alice@example.com").first()
```

### query.one

Get exactly one result, raise if not exactly 1.

```python
async def one(self) -> T
```

```python
try:
    user = await session.query(User).filter(email="alice@example.com").one()
except NoResultFound:
    print("No user found")
except MultipleResultsFound:
    print("Multiple users found")
```

### query.one_or_none

Get one result or None, raise if more than 1.

```python
async def one_or_none(self) -> T | None
```

```python
user = await session.query(User).filter(email="alice@example.com").one_or_none()
```

---

## Aggregates

### query.count

Count matching rows.

```python
async def count(self) -> int
```

```python
total = await session.query(User).count()
adults = await session.query(User).filter(age__gte=18).count()
```

### query.sum

Sum of a column's values.

```python
async def sum(self, column: str) -> float | int | None
```

```python
total = await session.query(Order).sum("amount")
```

### query.avg

Average of a column's values.

```python
async def avg(self, column: str) -> float | None
```

```python
avg_age = await session.query(User).avg("age")
```

### query.min

Minimum value of a column.

```python
async def min(self, column: str) -> Any
```

```python
youngest = await session.query(User).min("age")
```

### query.max

Maximum value of a column.

```python
async def max(self, column: str) -> Any
```

```python
oldest = await session.query(User).max("age")
```

### query.exists

Check if any rows match.

```python
async def exists(self) -> bool
```

```python
has_admin = await session.query(User).filter(role="admin").exists()
```

---

## Projection

### query.values

Return dictionaries with specific columns only.

```python
async def values(self, *columns: str) -> list[dict[str, Any]]
```

```python
users = await session.query(User).values("id", "name", "email")
# [{"id": 1, "name": "Alice", "email": "alice@example.com"}, ...]
```

### query.values_list

Return tuples with specific columns (faster than dicts).

```python
async def values_list(self, *columns: str) -> list[tuple[Any, ...]]
```

```python
users = await session.query(User).values_list("id", "name")
# [(1, "Alice"), (2, "Bob"), ...]
```

---

## Streaming

### query.stream

Stream results in batches for large datasets.

```python
async def stream(self, batch_size: int = 1000) -> AsyncIterator[T]
```

```python
async for user in session.query(User).stream(batch_size=1000):
    process_user(user)
```

---

## Bulk Operations

### query.delete

Delete all matching rows.

```python
async def delete(self) -> int
```

```python
deleted = await session.query(User).filter(status="inactive").delete()
print(f"Deleted {deleted} users")
```

### query.update

Update all matching rows.

```python
async def update(self, **values: Any) -> int
```

```python
updated = await session.query(User).filter(role="guest").update(role="member")
print(f"Updated {updated} users")
```

---

## Soft Delete Filters

For models using `SoftDeleteMixin`:

### query.with_deleted

Include soft-deleted records in results.

```python
def with_deleted(self) -> Query[T]
```

```python
all_articles = await session.query(Article).with_deleted().all()
```

### query.only_deleted

Return only soft-deleted records.

```python
def only_deleted(self) -> Query[T]
```

```python
deleted_articles = await session.query(Article).only_deleted().all()
```

---

## Chaining Example

```python
users = await session.query(User) \
    .filter(age__gte=18) \
    .filter(status="active") \
    .order_by("-created_at") \
    .limit(10) \
    .offset(20) \
    .options(selectinload("posts")) \
    .all()
```
