# Models API

Utilities for defining database models.

## Base

Base class for all models.

```python
from ormkit import Base

class User(Base):
    __tablename__ = "users"
    # ... columns
```

All models must:

1. Inherit from `Base`
2. Define `__tablename__`
3. Have at least one column with `primary_key=True`

---

## Mapped

Type hint wrapper for column types.

```python
from ormkit import Mapped

class User(Base):
    __tablename__ = "users"

    id: Mapped[int]           # Required int
    name: Mapped[str]         # Required str
    age: Mapped[int | None]   # Optional int
```

### Supported Types

| Python Type | PostgreSQL | SQLite |
|-------------|------------|--------|
| `int` | `INTEGER` / `SERIAL` | `INTEGER` |
| `str` | `TEXT` / `VARCHAR` | `TEXT` |
| `float` | `DOUBLE PRECISION` | `REAL` |
| `bool` | `BOOLEAN` | `INTEGER` (0/1) |
| `bytes` | `BYTEA` | `BLOB` |
| `datetime` | `TIMESTAMP` | `TEXT` |
| `date` | `DATE` | `TEXT` |
| `time` | `TIME` | `TEXT` |
| `dict` / `list` (with `JSON`) | `JSONB` | `TEXT` |

---

## mapped_column

Define a database column.

```python
def mapped_column(
    *args,  # Can include ForeignKey or JSON marker
    primary_key: bool = False,
    nullable: bool = False,
    unique: bool = False,
    index: bool = False,
    default: Any = None,
    max_length: int | None = None,
) -> Any
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `primary_key` | `bool` | `False` | Is this the primary key? |
| `nullable` | `bool` | `False` | Allow NULL values? |
| `unique` | `bool` | `False` | Add UNIQUE constraint? |
| `index` | `bool` | `False` | Create an index? |
| `default` | `Any` | `None` | Default value or callable |
| `max_length` | `int` | `None` | VARCHAR length limit |

### Examples

```python
from datetime import datetime
from ormkit import Base, Mapped, mapped_column, JSON

class User(Base):
    __tablename__ = "users"

    # Primary key (auto-increment)
    id: Mapped[int] = mapped_column(primary_key=True)

    # Required with max length
    username: Mapped[str] = mapped_column(max_length=50, unique=True)

    # Indexed for fast lookups
    email: Mapped[str] = mapped_column(unique=True, index=True)

    # Optional with static default
    role: Mapped[str] = mapped_column(default="user")

    # Optional nullable
    bio: Mapped[str | None] = mapped_column(nullable=True)

    # Callable default
    created_at: Mapped[datetime] = mapped_column(default=datetime.now)

    # JSON column (JSONB on PostgreSQL)
    metadata: Mapped[dict] = mapped_column(JSON)
```

---

## JSON

Marker for JSON/JSONB columns.

```python
from ormkit import JSON
```

```python
class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)

    # Store arbitrary JSON data
    # PostgreSQL: JSONB, SQLite: TEXT (with JSON serialization)
    metadata: Mapped[dict] = mapped_column(JSON)
    settings: Mapped[dict] = mapped_column(JSON, default=dict)
    tags: Mapped[list] = mapped_column(JSON, default=list)
```

### Querying JSON Fields

Query nested JSON fields using double-underscore notation:

```python
# Query top-level key
users = await session.query(User).filter(metadata__plan="premium").all()

# Query nested path
users = await session.query(User).filter(metadata__settings__theme="dark").all()
```

---

## ForeignKey

Define a foreign key reference.

```python
def ForeignKey(
    target: str,
    ondelete: str | None = None,
    onupdate: str | None = None,
) -> Any
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `target` | `str` | required | Target table and column (`"table.column"`) |
| `ondelete` | `str` | `None` | ON DELETE action |
| `onupdate` | `str` | `None` | ON UPDATE action |

### Cascade Options

| Value | Behavior |
|-------|----------|
| `"CASCADE"` | Delete/update child rows |
| `"SET NULL"` | Set foreign key to NULL |
| `"RESTRICT"` | Prevent if children exist |
| `"NO ACTION"` | Same as RESTRICT (default) |

### Examples

```python
from ormkit import Base, Mapped, mapped_column, ForeignKey

class Post(Base):
    __tablename__ = "posts"

    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str]

    # Basic foreign key
    author_id: Mapped[int] = mapped_column(ForeignKey("users.id"))

    # With cascade delete
    category_id: Mapped[int] = mapped_column(
        ForeignKey("categories.id", ondelete="CASCADE")
    )

    # Nullable with SET NULL
    reviewer_id: Mapped[int | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True
    )
```

---

## relationship

Define a relationship between models.

```python
def relationship(
    back_populates: str | None = None,
    lazy: str = "select",
    uselist: bool | None = None,
    secondary: str | None = None,
) -> Any
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `back_populates` | `str` | `None` | Name of reverse relationship |
| `lazy` | `str` | `"select"` | Loading strategy |
| `uselist` | `bool` | `None` | Return list or single object |
| `secondary` | `str` | `None` | Junction table for many-to-many |

### Examples

```python
from ormkit import Base, Mapped, mapped_column, ForeignKey, relationship

class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]

    # One-to-many: User has many posts
    posts: Mapped[list["Post"]] = relationship(back_populates="author")


class Post(Base):
    __tablename__ = "posts"

    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str]
    author_id: Mapped[int] = mapped_column(ForeignKey("users.id"))

    # Many-to-one: Post belongs to one author
    author: Mapped[User] = relationship(back_populates="posts")
```

### Many-to-Many Relationships

```python
# Junction table
class PostTag(Base):
    __tablename__ = "post_tags"

    post_id: Mapped[int] = mapped_column(ForeignKey("posts.id"), primary_key=True)
    tag_id: Mapped[int] = mapped_column(ForeignKey("tags.id"), primary_key=True)


class Post(Base):
    __tablename__ = "posts"

    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str]

    # Many-to-many via junction table
    tags: Mapped[list["Tag"]] = relationship(
        back_populates="posts",
        secondary="post_tags"
    )


class Tag(Base):
    __tablename__ = "tags"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]

    posts: Mapped[list["Post"]] = relationship(
        back_populates="tags",
        secondary="post_tags"
    )
```

### Type Hints for Relationships

| Type Hint | Relationship | Returns |
|-----------|--------------|---------|
| `Mapped[list["Model"]]` | One-to-many or Many-to-many | List of models |
| `Mapped["Model"]` | Many-to-one | Single model |

---

## SoftDeleteMixin

Add soft delete functionality to models.

```python
from ormkit import SoftDeleteMixin

class Article(SoftDeleteMixin, Base):
    __tablename__ = "articles"

    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str]
```

The mixin adds a `deleted_at` column and enables:

- `session.soft_delete(instance)` - Set deleted_at timestamp
- `session.restore(instance)` - Clear deleted_at
- `session.force_delete(instance)` - Permanently delete
- `query.with_deleted()` - Include soft-deleted records
- `query.only_deleted()` - Only soft-deleted records

---

## Loading Options

### selectinload

Load a relationship using `SELECT ... WHERE id IN (...)`.

```python
def selectinload(relationship: str) -> LoadOption
```

Best for collections (one-to-many).

```python
from ormkit import selectinload

users = await session.query(User) \
    .options(selectinload("posts")) \
    .all()
```

### joinedload

Load a relationship using `JOIN`.

```python
def joinedload(relationship: str) -> LoadOption
```

Best for single objects (many-to-one).

```python
from ormkit import joinedload

posts = await session.query(Post) \
    .options(joinedload("author")) \
    .all()
```

### noload

Explicitly skip loading a relationship.

```python
def noload(relationship: str) -> LoadOption
```

```python
from ormkit import noload

users = await session.query(User) \
    .options(noload("posts")) \
    .all()
# user.posts will be []
```

---

## Q Objects

Build complex query conditions with OR/AND/NOT.

```python
from ormkit import Q

# OR
query.filter(Q(age__gt=65) | Q(role="retired"))

# AND
query.filter(Q(age__gte=18) & Q(status="active"))

# NOT
query.filter(~Q(status="banned"))

# Complex combinations
query.filter(
    (Q(age__gte=18) & Q(age__lt=65)) | Q(vip=True)
)
```
