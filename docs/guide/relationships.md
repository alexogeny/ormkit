# Relationships

OrmKit supports one-to-many, many-to-one, and many-to-many relationships with eager loading.

## Defining Relationships

### One-to-Many

A user has many posts:

```python
from ormkit import Base, Mapped, mapped_column, ForeignKey, relationship

class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(max_length=100)

    # One user has many posts
    posts: Mapped[list["Post"]] = relationship(back_populates="author")


class Post(Base):
    __tablename__ = "posts"

    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column(max_length=200)
    author_id: Mapped[int] = mapped_column(ForeignKey("users.id"))

    # Many posts belong to one author
    author: Mapped[User] = relationship(back_populates="posts")
```

### Key Points

1. **Foreign key column** - `author_id` stores the reference
2. **`relationship()`** - Defines how to access related objects
3. **`back_populates`** - Links both sides of the relationship
4. **Type hints** - `list["Post"]` for one-to-many, `User` for many-to-one

### Many-to-Many

Posts and tags with a junction table:

```python
# Junction table
class PostTag(Base):
    __tablename__ = "post_tags"

    post_id: Mapped[int] = mapped_column(ForeignKey("posts.id"), primary_key=True)
    tag_id: Mapped[int] = mapped_column(ForeignKey("tags.id"), primary_key=True)


class Post(Base):
    __tablename__ = "posts"

    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column(max_length=200)

    # Many-to-many via junction table
    tags: Mapped[list["Tag"]] = relationship(
        back_populates="posts",
        secondary="post_tags"
    )


class Tag(Base):
    __tablename__ = "tags"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(max_length=50, unique=True)

    posts: Mapped[list["Post"]] = relationship(
        back_populates="tags",
        secondary="post_tags"
    )
```

#### Loading Many-to-Many

```python
# Load posts with their tags
posts = await session.query(Post) \
    .options(selectinload("tags")) \
    .all()

for post in posts:
    tag_names = ", ".join(tag.name for tag in post.tags)
    print(f"{post.title}: {tag_names}")

# Load tags with their posts
tags = await session.query(Tag) \
    .options(selectinload("posts")) \
    .all()
```

## Loading Relationships

By default, relationships are **not loaded** to avoid N+1 queries. Use eager loading to fetch related data.

### selectinload (Recommended for Collections)

Loads related objects with a separate `SELECT ... WHERE id IN (...)` query:

```python
from ormkit import selectinload

# Load users with their posts
users = await session.query(User) \
    .options(selectinload("posts")) \
    .all()

for user in users:
    print(f"{user.name} has {len(user.posts)} posts")
    for post in user.posts:
        print(f"  - {post.title}")
```

### joinedload (Best for Single Objects)

Loads related objects using a JOIN:

```python
from ormkit import joinedload

# Load posts with their authors
posts = await session.query(Post) \
    .options(joinedload("author")) \
    .all()

for post in posts:
    print(f"{post.title} by {post.author.name}")
```

### noload (Explicitly Disable)

Prevent loading a relationship:

```python
from ormkit import noload

users = await session.query(User) \
    .options(noload("posts")) \
    .all()

for user in users:
    print(user.posts)  # Empty list []
```

## Multiple Relationships

Load multiple relationships in one query:

```python
users = await session.query(User) \
    .options(
        selectinload("posts"),
        selectinload("comments"),
        selectinload("profile"),
    ) \
    .all()
```

## When to Use Each Strategy

| Strategy | Best For | SQL Generated |
|----------|----------|---------------|
| `selectinload` | Collections (one-to-many) | `SELECT * FROM posts WHERE author_id IN (1, 2, 3)` |
| `joinedload` | Single objects (many-to-one) | `SELECT * FROM posts JOIN users ON ...` |
| `noload` | Explicitly skip loading | No additional query |

!!! tip "Default to selectinload"
    `selectinload` works well for most cases. Use `joinedload` when you're loading many-to-one relationships and want a single query.

## Accessing Unloaded Relationships

If you access a relationship that wasn't eagerly loaded:

```python
user = await session.get(User, 1)  # No eager loading
print(user.posts)  # Returns empty list []
```

!!! warning "No Lazy Loading"
    OrmKit doesn't support lazy loading (automatic loading on access). This is intentional—lazy loading causes N+1 query problems. Always use explicit eager loading.

## Creating Related Objects

### Insert with Foreign Key

```python
# Create user first
user = await session.insert(User(name="Alice"))

# Create post with user's ID
post = await session.insert(
    Post(title="My Post", author_id=user.id)
)
```

### Transaction Pattern

```python
async with session.begin() as tx:
    user = User(name="Alice")
    tx.add(user)
    # user.id is available after add

    post = Post(title="My Post", author_id=user.id)
    tx.add(post)
```

## Cascade Deletes

Control what happens when a parent is deleted:

```python
class Post(Base):
    __tablename__ = "posts"

    id: Mapped[int] = mapped_column(primary_key=True)
    author_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE")
    )
```

### Cascade Options

| Option | Behavior |
|--------|----------|
| `CASCADE` | Delete child records when parent is deleted |
| `SET NULL` | Set foreign key to NULL (column must be nullable) |
| `RESTRICT` | Prevent deletion if children exist |
| `NO ACTION` | Same as RESTRICT (default) |

## Example: Blog with Comments

```python
class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]

    posts: Mapped[list["Post"]] = relationship(back_populates="author")
    comments: Mapped[list["Comment"]] = relationship(back_populates="author")


class Post(Base):
    __tablename__ = "posts"

    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str]
    author_id: Mapped[int] = mapped_column(ForeignKey("users.id"))

    author: Mapped[User] = relationship(back_populates="posts")
    comments: Mapped[list["Comment"]] = relationship(back_populates="post")


class Comment(Base):
    __tablename__ = "comments"

    id: Mapped[int] = mapped_column(primary_key=True)
    content: Mapped[str]
    author_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    post_id: Mapped[int] = mapped_column(ForeignKey("posts.id"))

    author: Mapped[User] = relationship(back_populates="comments")
    post: Mapped[Post] = relationship(back_populates="comments")


# Load post with author and comments (including comment authors)
post = await session.query(Post) \
    .filter(id=post_id) \
    .options(
        joinedload("author"),
        selectinload("comments"),
    ) \
    .first()

print(f"{post.title} by {post.author.name}")
print(f"{len(post.comments)} comments")
```

## Next Steps

- [Use transactions](transactions.md) for atomic operations
- [Write raw SQL](raw-sql.md) for complex joins
- [Optimize performance](../performance/optimization.md)
