# Models

Models define your database schema using Python classes with type hints.

## Basic Model

```python
from ormkit import Base, Mapped, mapped_column

class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(max_length=100)
    email: Mapped[str] = mapped_column(unique=True)
```

### Required Elements

1. **Inherit from `Base`** - All models must inherit from the `Base` class
2. **Set `__tablename__`** - The database table name
3. **Define columns** - Using `Mapped[]` type hints and `mapped_column()`

## Column Types

OrmKit infers SQL types from Python type hints:

| Python Type | PostgreSQL | SQLite |
|-------------|------------|--------|
| `int` | `INTEGER` / `SERIAL` | `INTEGER` |
| `str` | `TEXT` / `VARCHAR` | `TEXT` |
| `float` | `DOUBLE PRECISION` | `REAL` |
| `bool` | `BOOLEAN` | `INTEGER` |
| `bytes` | `BYTEA` | `BLOB` |
| `datetime` | `TIMESTAMP` | `TEXT` |
| `date` | `DATE` | `TEXT` |
| `time` | `TIME` | `TEXT` |

### Nullable Columns

Use `Optional` or union syntax for nullable columns:

```python
from typing import Optional

class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)

    # Both of these are nullable
    age: Mapped[int | None] = mapped_column(nullable=True)
    bio: Mapped[Optional[str]] = mapped_column(nullable=True)
```

## Column Options

`mapped_column()` accepts these parameters:

```python
mapped_column(
    primary_key=False,  # Is this the primary key?
    nullable=False,     # Allow NULL values?
    unique=False,       # Add UNIQUE constraint?
    index=False,        # Create an index?
    default=None,       # Default value or callable
    max_length=None,    # VARCHAR length limit
)
```

### Examples

```python
class User(Base):
    __tablename__ = "users"

    # Auto-incrementing primary key
    id: Mapped[int] = mapped_column(primary_key=True)

    # Required string with max length
    username: Mapped[str] = mapped_column(max_length=50, unique=True)

    # Optional with default
    role: Mapped[str] = mapped_column(default="user")

    # Indexed for fast lookups
    email: Mapped[str] = mapped_column(unique=True, index=True)

    # Nullable datetime with default
    created_at: Mapped[datetime] = mapped_column(default=datetime.now)
```

## Default Values

### Static Defaults

```python
class Post(Base):
    __tablename__ = "posts"

    id: Mapped[int] = mapped_column(primary_key=True)
    status: Mapped[str] = mapped_column(default="draft")
    views: Mapped[int] = mapped_column(default=0)
```

### Callable Defaults

For dynamic values, pass a callable:

```python
from datetime import datetime
import uuid

class Post(Base):
    __tablename__ = "posts"

    id: Mapped[int] = mapped_column(primary_key=True)

    # Called at insert time
    created_at: Mapped[datetime] = mapped_column(default=datetime.now)

    # Generate unique slug
    slug: Mapped[str] = mapped_column(default=lambda: str(uuid.uuid4())[:8])
```

!!! warning "Callable vs Value"
    Use `default=datetime.now` (callable), not `default=datetime.now()` (value).
    The latter would set all records to the same time!

## Foreign Keys

Reference other tables with `OrmKit`:

```python
from ormkit import ForeignKey

class Post(Base):
    __tablename__ = "posts"

    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column(max_length=200)

    # Reference users table
    author_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
```

### Cascade Options

```python
# Delete posts when user is deleted
author_id: Mapped[int] = mapped_column(
    ForeignKey("users.id", ondelete="CASCADE")
)

# Set to NULL when user is deleted
author_id: Mapped[int | None] = mapped_column(
    ForeignKey("users.id", ondelete="SET NULL"),
    nullable=True
)

# Prevent deletion if posts exist
author_id: Mapped[int] = mapped_column(
    ForeignKey("users.id", ondelete="RESTRICT")
)
```

## Table Options

### Custom Table Names

```python
class UserAccount(Base):
    __tablename__ = "user_accounts"  # Table name in database
```

## Creating Tables

Create all tables defined by your models:

```python
engine = await create_engine("postgresql://localhost/mydb")
await engine.create_all()
```

!!! note "Development Only"
    `create_all()` is great for development and testing. For production, use a migration tool like Alembic.

## Model Instances

### Creating Instances

```python
# Create without ID (will be set on insert)
user = User(name="Alice", email="alice@example.com")

# Access attributes
print(user.name)  # "Alice"
print(user.id)    # None (not yet inserted)

# Insert to get ID
user = await session.insert(user)
print(user.id)    # 1 (now has database ID)
```

### Updating Instances

```python
# Update through session (recommended)
await session.update(user, name="Alicia", age=30)

# The instance is updated too
print(user.name)  # "Alicia"
```

## Next Steps

- [Learn about relationships](relationships.md) between models
- [Master the query API](queries.md)
- [See the API reference](../api/models.md)
