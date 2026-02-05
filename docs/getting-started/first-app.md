# Building Your First App

Let's build a simple blog API to learn OrmKit's features in a realistic context.

## Project Structure

```
blog/
├── models.py      # Database models
├── database.py    # Database connection
├── main.py        # Application entry point
└── requirements.txt
```

## Step 1: Define Models

```python title="models.py"
from datetime import datetime
from ormkit import Base, Mapped, mapped_column, ForeignKey, relationship

class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    username: Mapped[str] = mapped_column(max_length=50, unique=True)
    email: Mapped[str] = mapped_column(max_length=255, unique=True)
    created_at: Mapped[datetime] = mapped_column(default=datetime.now)

    # Relationships
    posts: Mapped[list["Post"]] = relationship(back_populates="author")
    comments: Mapped[list["Comment"]] = relationship(back_populates="author")


class Post(Base):
    __tablename__ = "posts"

    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column(max_length=200)
    content: Mapped[str]
    published: Mapped[bool] = mapped_column(default=False)
    created_at: Mapped[datetime] = mapped_column(default=datetime.now)
    author_id: Mapped[int] = mapped_column(ForeignKey("users.id"))

    # Relationships
    author: Mapped[User] = relationship(back_populates="posts")
    comments: Mapped[list["Comment"]] = relationship(back_populates="post")


class Comment(Base):
    __tablename__ = "comments"

    id: Mapped[int] = mapped_column(primary_key=True)
    content: Mapped[str]
    created_at: Mapped[datetime] = mapped_column(default=datetime.now)
    author_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    post_id: Mapped[int] = mapped_column(ForeignKey("posts.id"))

    # Relationships
    author: Mapped[User] = relationship(back_populates="comments")
    post: Mapped[Post] = relationship(back_populates="comments")
```

## Step 2: Database Setup

```python title="database.py"
from ormkit import create_engine, AsyncSession

DATABASE_URL = "postgresql://localhost/blog"
# Or for development: DATABASE_URL = "sqlite:///blog.db"

engine = None
session = None

async def init_db():
    global engine, session
    engine = await create_engine(DATABASE_URL)
    session = AsyncSession(engine)
    await engine.create_all()

async def close_db():
    if engine:
        await engine.close()

def get_session() -> AsyncSession:
    return session
```

## Step 3: CRUD Operations

```python title="main.py"
import asyncio
from models import User, Post, Comment
from database import init_db, close_db, get_session
from ormkit import selectinload

# ============ User Operations ============

async def create_user(username: str, email: str) -> User:
    session = get_session()
    return await session.insert(
        User(username=username, email=email)
    )

async def get_user_by_username(username: str) -> User | None:
    session = get_session()
    return await session.query(User).filter(username=username).first()

async def get_user_with_posts(user_id: int) -> User | None:
    session = get_session()
    return await session.query(User) \
        .filter(id=user_id) \
        .options(selectinload("posts")) \
        .first()

# ============ Post Operations ============

async def create_post(author_id: int, title: str, content: str) -> Post:
    session = get_session()
    return await session.insert(
        Post(author_id=author_id, title=title, content=content)
    )

async def publish_post(post_id: int) -> Post | None:
    session = get_session()
    post = await session.get(Post, post_id)
    if post:
        await session.update(post, published=True)
    return post

async def get_published_posts(limit: int = 10, offset: int = 0) -> list[Post]:
    session = get_session()
    return await session.query(Post) \
        .filter(published=True) \
        .order_by("-created_at") \
        .limit(limit) \
        .offset(offset) \
        .options(selectinload("author")) \
        .all()

async def get_post_with_comments(post_id: int) -> Post | None:
    session = get_session()
    return await session.query(Post) \
        .filter(id=post_id) \
        .options(
            selectinload("author"),
            selectinload("comments"),
        ) \
        .first()

async def search_posts(query: str) -> list[Post]:
    session = get_session()
    return await session.query(Post) \
        .filter(title__like=f"%{query}%") \
        .filter(published=True) \
        .order_by("-created_at") \
        .all()

# ============ Comment Operations ============

async def add_comment(post_id: int, author_id: int, content: str) -> Comment:
    session = get_session()
    return await session.insert(
        Comment(post_id=post_id, author_id=author_id, content=content)
    )

async def delete_comment(comment_id: int, author_id: int) -> bool:
    """Delete a comment if the user owns it."""
    session = get_session()
    deleted = await session.query(Comment) \
        .filter(id=comment_id, author_id=author_id) \
        .delete()
    return deleted > 0

# ============ Main ============

async def main():
    await init_db()

    try:
        # Create users
        alice = await create_user("alice", "alice@example.com")
        bob = await create_user("bob", "bob@example.com")
        print(f"Created users: {alice.username}, {bob.username}")

        # Create posts
        post1 = await create_post(alice.id, "Hello World", "My first blog post!")
        post2 = await create_post(alice.id, "OrmKit Tutorial", "Learn the fastest Python ORM...")
        print(f"Created {post1.title} and {post2.title}")

        # Publish posts
        await publish_post(post1.id)
        await publish_post(post2.id)

        # Add comments
        await add_comment(post1.id, bob.id, "Great post!")
        await add_comment(post1.id, alice.id, "Thanks!")

        # Query published posts with authors
        posts = await get_published_posts()
        print(f"\nPublished posts ({len(posts)}):")
        for post in posts:
            print(f"  - {post.title} by {post.author.username}")

        # Get post with comments
        post = await get_post_with_comments(post1.id)
        if post:
            print(f"\n{post.title} has {len(post.comments)} comments")

        # Search
        results = await search_posts("Tutorial")
        print(f"\nSearch results: {len(results)} posts")

    finally:
        await close_db()

if __name__ == "__main__":
    asyncio.run(main())
```

## Step 4: Using Transactions

For operations that need to be atomic:

```python
from ormkit import session_context

async def create_post_with_initial_comment(
    author_id: int,
    title: str,
    content: str,
    initial_comment: str
) -> Post:
    """Create a post with an initial comment atomically."""
    session = get_session()

    async with session.begin() as tx:
        # Both operations happen in a single transaction
        post = Post(author_id=author_id, title=title, content=content)
        tx.add(post)

        comment = Comment(
            post_id=post.id,
            author_id=author_id,
            content=initial_comment
        )
        tx.add(comment)

    return post
```

## Step 5: Bulk Operations

```python
async def import_users(user_data: list[dict]) -> list[User]:
    """Bulk import users efficiently."""
    session = get_session()

    users = [
        User(username=u["username"], email=u["email"])
        for u in user_data
    ]

    return await session.insert_all(users)

async def deactivate_old_posts(days: int = 365) -> int:
    """Archive posts older than N days."""
    from datetime import datetime, timedelta

    session = get_session()
    cutoff = datetime.now() - timedelta(days=days)

    return await session.query(Post) \
        .filter(created_at__lt=cutoff, published=True) \
        .delete()
```

## Next Steps

You now have a working blog application! Continue learning:

- [Deep dive into models](../guide/models.md) - Constraints, defaults, and more
- [Master queries](../guide/queries.md) - Complex filters, aggregates
- [Understand relationships](../guide/relationships.md) - Many-to-many, lazy loading
- [Optimize performance](../performance/optimization.md) - Connection pooling, caching
