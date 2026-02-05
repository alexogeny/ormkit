# API Reference

Complete reference for all OrmKit public APIs.

## Modules

| Module | Description |
|--------|-------------|
| [Engine](engine.md) | Database connection and raw query execution |
| [Session](session.md) | ORM session for model operations |
| [Query](query.md) | Query builder for filtering and fetching |
| [Models](models.md) | Model definition utilities |

## Quick Import Reference

```python
# Core
from ormkit import (
    create_engine,
    AsyncSession,
    session_context,
)

# Model definition
from ormkit import (
    Base,
    Mapped,
    mapped_column,
    OrmKit,
    relationship,
)

# Query building
from ormkit import (
    select,
    selectinload,
    joinedload,
    noload,
)
```

## Type Reference

### Column Types

| Python Type | PostgreSQL | SQLite |
|-------------|------------|--------|
| `int` | `INTEGER` / `SERIAL` | `INTEGER` |
| `str` | `TEXT` / `VARCHAR(n)` | `TEXT` |
| `float` | `DOUBLE PRECISION` | `REAL` |
| `bool` | `BOOLEAN` | `INTEGER` |
| `bytes` | `BYTEA` | `BLOB` |
| `datetime` | `TIMESTAMP` | `TEXT` |
| `date` | `DATE` | `TEXT` |
| `time` | `TIME` | `TEXT` |
| `T | None` | T (nullable) | T (nullable) |

### Filter Operators

| Suffix | SQL Operator | Example |
|--------|--------------|---------|
| (none) | `=` | `filter(name="Alice")` |
| `__gt` | `>` | `filter(age__gt=18)` |
| `__gte` | `>=` | `filter(age__gte=18)` |
| `__lt` | `<` | `filter(age__lt=65)` |
| `__lte` | `<=` | `filter(age__lte=65)` |
| `__ne` | `!=` / `<>` | `filter(status__ne="deleted")` |
| `__like` | `LIKE` | `filter(name__like="A%")` |
| `__ilike` | `ILIKE` | `filter(name__ilike="a%")` |
