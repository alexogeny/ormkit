# User Guide

This guide covers OrmKit's features in depth.

## Topics

<div class="feature-grid" markdown>

<div class="feature-card" markdown>
### :material-database: Models
Define your database schema with Python classes.

[Learn Models →](models.md)
</div>

<div class="feature-card" markdown>
### :material-magnify: Queries
Filter, sort, paginate, and aggregate data.

[Learn Queries →](queries.md)
</div>

<div class="feature-card" markdown>
### :material-link: Relationships
Connect models with foreign keys and eager loading.

[Learn Relationships →](relationships.md)
</div>

<div class="feature-card" markdown>
### :material-swap-horizontal: Transactions
Atomic operations and error handling.

[Learn Transactions →](transactions.md)
</div>

<div class="feature-card" markdown>
### :material-code-tags: Raw SQL
Execute raw queries when you need full control.

[Learn Raw SQL →](raw-sql.md)
</div>

</div>

## Quick Reference

### Model Definition

```python
from ormkit import Base, Mapped, mapped_column

class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(max_length=100)
    email: Mapped[str] = mapped_column(unique=True)
    age: Mapped[int | None] = mapped_column(nullable=True, default=None)
```

### Basic CRUD

```python
# Create
user = await session.insert(User(name="Alice", email="alice@example.com"))

# Read
user = await session.get(User, 1)
users = await session.query(User).filter(age__gte=18).all()

# Update
await session.update(user, name="Alicia")

# Delete
await session.remove(user)
```

### Filter Operators

| Operator | SQL | Example |
|----------|-----|---------|
| (none) | `=` | `filter(name="Alice")` |
| `__gt` | `>` | `filter(age__gt=18)` |
| `__gte` | `>=` | `filter(age__gte=18)` |
| `__lt` | `<` | `filter(age__lt=65)` |
| `__lte` | `<=` | `filter(age__lte=65)` |
| `__ne` | `!=` | `filter(status__ne="deleted")` |
| `__like` | `LIKE` | `filter(name__like="A%")` |
| `__ilike` | `ILIKE` | `filter(name__ilike="a%")` |
