# Quick Start

This guide will get you up and running with OrmKit in 5 minutes.

## 1. Define Your Models

Models in OrmKit use SQLAlchemy-style declarative syntax:

```python
from ormkit import Base, Mapped, mapped_column, ForeignKey, relationship

class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(max_length=100)
    email: Mapped[str] = mapped_column(unique=True)
    age: Mapped[int | None] = mapped_column(nullable=True)

    # Relationship to posts
    posts: Mapped[list["Post"]] = relationship(back_populates="author")


class Post(Base):
    __tablename__ = "posts"

    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column(max_length=200)
    content: Mapped[str]
    author_id: Mapped[int] = mapped_column(ForeignKey("users.id"))

    # Relationship to author
    author: Mapped[User] = relationship(back_populates="posts")
```

!!! tip "Type Hints Matter"
    The `Mapped[]` type hints aren't just for IDE support—OrmKit uses them to infer column types and generate SQL.

## 2. Create the Engine

Connect to your database:

=== "PostgreSQL"

    ```python
    from ormkit import create_engine

    engine = await create_engine("postgresql://user:pass@localhost/mydb")
    ```

=== "SQLite"

    ```python
    from ormkit import create_engine

    engine = await create_engine("sqlite:///myapp.db")
    ```

## 3. Create Tables

```python
# Create all tables defined in your models
await engine.create_all()
```

## 4. Insert Data

```python
from ormkit import AsyncSession

session = AsyncSession(engine)

# Insert returns the model with its generated ID
user = await session.insert(
    User(name="Alice", email="alice@example.com", age=30)
)
print(f"Created user with id={user.id}")

# Insert a post for this user
post = await session.insert(
    Post(title="Hello World", content="My first post!", author_id=user.id)
)
```

## 5. Query Data

### Get by Primary Key

```python
user = await session.get(User, 1)
if user:
    print(f"Found: {user.name}")
```

### Filter with Django-style Operators

```python
# Exact match
user = await session.query(User).filter(email="alice@example.com").first()

# Comparison operators
adults = await session.query(User).filter(age__gte=18).all()
young_adults = await session.query(User).filter(age__gte=18, age__lt=30).all()

# Pattern matching
a_users = await session.query(User).filter(name__like="A%").all()
a_users = await session.query(User).filter(name__startswith="A").all()

# IN queries
admins = await session.query(User).filter(role__in=["admin", "superuser"]).all()

# NULL checks
active = await session.query(User).filter(deleted_at__isnull=True).all()
```

### Ordering and Pagination

```python
# Order by name ascending
users = await session.query(User).order_by("name").all()

# Order by age descending
users = await session.query(User).order_by("-age").all()

# Pagination
users = await session.query(User).limit(10).offset(20).all()
```

## 6. Update Data

```python
# Update specific fields
await session.update(user, name="Alicia", age=31)

# The model instance is updated too
print(user.name)  # "Alicia"
```

## 7. Delete Data

```python
# Delete a single record
await session.remove(user)

# Bulk delete with filters
deleted_count = await session.query(User).filter(age__lt=18).delete()
```

## 8. Load Relationships

```python
from ormkit import selectinload

# Load users with their posts
users = await session.query(User).options(selectinload("posts")).all()

for user in users:
    print(f"{user.name} has {len(user.posts)} posts")
    for post in user.posts:
        print(f"  - {post.title}")
```

## 9. Aggregates

```python
# Count matching rows
total = await session.query(User).count()
adults = await session.query(User).filter(age__gte=18).count()

# Check existence
has_admin = await session.query(User).filter(role="admin").exists()

# Sum, average, min, max
total_balance = await session.query(Account).sum("balance")
avg_age = await session.query(User).avg("age")
youngest = await session.query(User).min("age")
oldest = await session.query(User).max("age")
```

## 10. Complex Queries with Q Objects

```python
from ormkit import Q

# OR queries
users = await session.query(User).filter(
    Q(age__gt=65) | Q(role="retired")
).all()

# Complex combinations
users = await session.query(User).filter(
    (Q(age__gte=18) & Q(age__lt=65)) | Q(vip=True)
).all()
```

## Complete Example

Here's everything together:

```python
import asyncio
from ormkit import (
    Base, Mapped, mapped_column, ForeignKey, relationship,
    create_engine, AsyncSession, selectinload
)

class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(max_length=100)
    email: Mapped[str] = mapped_column(unique=True)
    posts: Mapped[list["Post"]] = relationship(back_populates="author")

class Post(Base):
    __tablename__ = "posts"
    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column(max_length=200)
    author_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    author: Mapped[User] = relationship(back_populates="posts")

async def main():
    # Connect
    engine = await create_engine("sqlite::memory:")
    await engine.create_all()

    session = AsyncSession(engine)

    # Create user and posts
    user = await session.insert(User(name="Alice", email="alice@example.com"))
    await session.insert(Post(title="First Post", author_id=user.id))
    await session.insert(Post(title="Second Post", author_id=user.id))

    # Query with relationships
    users = await session.query(User).options(selectinload("posts")).all()
    for u in users:
        print(f"{u.name}: {len(u.posts)} posts")

asyncio.run(main())
```

## Next Steps

- [Build a complete app](first-app.md) with relationships and transactions
- [Learn about models](../guide/models.md) in depth
- [Master the query API](../guide/queries.md)
- [See the benchmarks](../performance/benchmarks.md)
