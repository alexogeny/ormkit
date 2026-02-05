# Queries

OrmKit provides a fluent query builder with Django-style filter operators.

## Basic Queries

### Get by Primary Key

```python
# Returns None if not found
user = await session.get(User, 1)

# Raises exception if not found
user = await session.get_or_raise(User, 1)
```

### Get All Records

```python
users = await session.query(User).all()
```

### Get First Record

```python
user = await session.query(User).first()  # Returns None if no records
```

### Get Exactly One

```python
# Raises if not exactly 1 result
user = await session.query(User).filter(email="alice@example.com").one()

# Returns None if 0 results, raises if > 1
user = await session.query(User).filter(email="alice@example.com").one_or_none()
```

## Filtering

### Exact Match

```python
# Single condition
users = await session.query(User).filter(name="Alice").all()

# Multiple conditions (AND)
users = await session.query(User).filter(name="Alice", age=30).all()
```

### Comparison Operators

Use double-underscore suffixes for comparisons:

```python
# Greater than
adults = await session.query(User).filter(age__gt=18).all()

# Greater than or equal
adults = await session.query(User).filter(age__gte=18).all()

# Less than
young = await session.query(User).filter(age__lt=30).all()

# Less than or equal
young = await session.query(User).filter(age__lte=30).all()

# Not equal
active = await session.query(User).filter(status__ne="deleted").all()
```

### Pattern Matching

```python
# LIKE (case-sensitive)
users = await session.query(User).filter(name__like="A%").all()      # Starts with A
users = await session.query(User).filter(name__like="%son").all()    # Ends with son
users = await session.query(User).filter(name__like="%ali%").all()   # Contains ali

# ILIKE (case-insensitive, PostgreSQL only)
users = await session.query(User).filter(name__ilike="alice").all()

# Convenience operators
users = await session.query(User).filter(name__contains="ali").all()      # %ali%
users = await session.query(User).filter(name__icontains="ali").all()     # Case-insensitive
users = await session.query(User).filter(name__startswith="A").all()      # A%
users = await session.query(User).filter(name__endswith="son").all()      # %son
```

### IN and NOT IN

```python
# IN query
users = await session.query(User).filter(role__in=["admin", "moderator"]).all()

# NOT IN query
users = await session.query(User).filter(status__notin=["banned", "deleted"]).all()
```

### NULL Checks

```python
# IS NULL
users = await session.query(User).filter(deleted_at__isnull=True).all()

# IS NOT NULL
users = await session.query(User).filter(email__isnull=False).all()
```

### Chaining Filters

```python
# Chain multiple filter calls
users = await session.query(User) \
    .filter(age__gte=18) \
    .filter(status="active") \
    .filter(role="admin") \
    .all()
```

## Operator Reference

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
| `__ilike` | `ILIKE` | `filter(name__ilike="a%")` |
| `__contains` | `LIKE %x%` | `filter(name__contains="ali")` |
| `__icontains` | `ILIKE %x%` | `filter(name__icontains="ali")` |
| `__startswith` | `LIKE x%` | `filter(name__startswith="A")` |
| `__endswith` | `LIKE %x` | `filter(name__endswith="e")` |
| `__isnull` | `IS NULL` / `IS NOT NULL` | `filter(deleted_at__isnull=True)` |

## Q Objects for Complex Queries

Use `Q` objects for OR conditions and complex boolean logic:

```python
from ormkit import Q

# OR query
users = await session.query(User).filter(
    Q(age__gt=65) | Q(role="retired")
).all()

# AND with Q (explicit)
users = await session.query(User).filter(
    Q(age__gte=18) & Q(status="active")
).all()

# Complex combinations
users = await session.query(User).filter(
    (Q(age__gte=18) & Q(age__lt=65)) | Q(vip=True)
).all()

# NOT (negation)
users = await session.query(User).filter(
    ~Q(status="banned")
).all()
```

## JSON Field Queries

Query nested JSON fields using double-underscore notation:

```python
# Query top-level JSON key
users = await session.query(User).filter(metadata__plan="premium").all()

# Query nested JSON path
users = await session.query(User).filter(metadata__settings__theme="dark").all()

# Multiple levels
users = await session.query(User).filter(
    metadata__preferences__notifications__email=True
).all()
```

## Ordering

### Ascending Order

```python
users = await session.query(User).order_by("name").all()
users = await session.query(User).order_by("created_at").all()
```

### Descending Order

Prefix with `-` for descending:

```python
users = await session.query(User).order_by("-created_at").all()  # Newest first
users = await session.query(User).order_by("-age").all()          # Oldest first
```

### Multiple Columns

```python
users = await session.query(User).order_by("role", "-created_at").all()
```

## Pagination

### Limit and Offset

```python
# First 10 results
users = await session.query(User).limit(10).all()

# Skip first 20, get next 10
users = await session.query(User).offset(20).limit(10).all()
```

### Pagination Pattern

```python
async def get_users_page(page: int, per_page: int = 20) -> list[User]:
    return await session.query(User) \
        .order_by("-created_at") \
        .offset((page - 1) * per_page) \
        .limit(per_page) \
        .all()
```

## Distinct and Grouping

### Distinct Results

```python
# Get distinct values
roles = await session.query(User).distinct().values("role")
```

### Group By

```python
# Group by column
results = await session.query(User) \
    .group_by("department") \
    .values("department", count="id")
```

### Having (Filter on Aggregates)

```python
# Filter groups by aggregate values
results = await session.query(User) \
    .group_by("department") \
    .having(count__gt=5) \
    .values("department")
```

## Aggregates

### Count

```python
total = await session.query(User).count()
adults = await session.query(User).filter(age__gte=18).count()
```

### Sum, Avg, Min, Max

```python
# Sum of a column
total_balance = await session.query(Account).sum("balance")

# Average
avg_age = await session.query(User).avg("age")

# Minimum and maximum
youngest = await session.query(User).min("age")
oldest = await session.query(User).max("age")
```

### Exists

```python
has_admin = await session.query(User).filter(role="admin").exists()
if has_admin:
    print("At least one admin exists")
```

## Projection (Select Specific Columns)

### Values (Return Dicts)

```python
# Return only specific columns as dicts
users = await session.query(User).values("id", "name", "email")
# [{"id": 1, "name": "Alice", "email": "alice@example.com"}, ...]
```

### Values List (Return Tuples)

```python
# Return as tuples (faster)
users = await session.query(User).values_list("id", "name")
# [(1, "Alice"), (2, "Bob"), ...]
```

## Streaming Large Result Sets

For large datasets, use streaming to avoid loading everything into memory:

```python
# Stream results in batches
async for user in session.query(User).stream(batch_size=1000):
    process_user(user)

# With filters
async for order in session.query(Order).filter(status="pending").stream(batch_size=500):
    await process_order(order)
```

## Bulk Operations

### Bulk Delete

```python
# Delete all matching records
deleted = await session.query(User).filter(status="inactive").delete()
print(f"Deleted {deleted} users")
```

### Bulk Update

```python
# Update all matching records
updated = await session.query(User).filter(role="guest").update(role="member")
print(f"Updated {updated} users")

# Update multiple fields
updated = await session.query(User).filter(status="trial").update(
    status="expired",
    access_level=0
)
```

### Bulk Insert

```python
users = await session.insert_all([
    User(name="Alice", email="alice@example.com"),
    User(name="Bob", email="bob@example.com"),
    User(name="Charlie", email="charlie@example.com"),
])
```

## Soft Delete Queries

If using `SoftDeleteMixin`, queries automatically exclude soft-deleted records:

```python
# Only returns non-deleted records
articles = await session.query(Article).all()

# Include soft-deleted records
all_articles = await session.query(Article).with_deleted().all()

# Only soft-deleted records
deleted_articles = await session.query(Article).only_deleted().all()
```

## Complex Queries

### Combining Conditions

```python
# Find active adult admins
admins = await session.query(User) \
    .filter(age__gte=18) \
    .filter(status="active") \
    .filter(role="admin") \
    .order_by("name") \
    .all()
```

### Filter by Related Field

```python
# Get posts by a specific author
posts = await session.query(Post) \
    .filter(author_id=user.id) \
    .order_by("-created_at") \
    .all()
```

## Performance Tips

!!! tip "Use Indexes"
    Add indexes to columns you frequently filter on:
    ```python
    email: Mapped[str] = mapped_column(index=True)
    ```

!!! tip "Limit Results"
    Always use `.limit()` when you don't need all results:
    ```python
    recent = await session.query(Post).order_by("-created_at").limit(10).all()
    ```

!!! tip "Use count() and exists()"
    Let the database do the counting:
    ```python
    # Good
    total = await session.query(User).count()

    # Bad - downloads all data just to count
    users = await session.query(User).all()
    total = len(users)
    ```

!!! tip "Stream Large Results"
    For large datasets, use streaming instead of loading all at once:
    ```python
    async for user in session.query(User).stream(batch_size=1000):
        process(user)
    ```

## Next Steps

- [Load related data](relationships.md) with eager loading
- [Use transactions](transactions.md) for atomic operations
- [Write raw SQL](raw-sql.md) for complex queries
