# Session API

The session provides ORM operations for models.

## AsyncSession

```python
class AsyncSession:
    def __init__(self, engine: Engine) -> None
```

### Example

```python
from ormkit import AsyncSession, create_engine

engine = await create_engine("postgresql://localhost/mydb")
session = AsyncSession(engine)
```

---

## CRUD Operations

### session.insert

Insert a model and return it with the generated ID.

```python
async def insert(self, instance: T) -> T
```

```python
user = await session.insert(
    User(name="Alice", email="alice@example.com")
)
print(user.id)  # Generated ID is now set
```

### session.insert_all

Insert multiple models efficiently.

```python
async def insert_all(self, instances: list[T]) -> list[T]
```

```python
users = await session.insert_all([
    User(name="Alice", email="alice@example.com"),
    User(name="Bob", email="bob@example.com"),
])
```

### session.get

Get a model by primary key.

```python
async def get(self, model: type[T], id: Any) -> T | None
```

```python
user = await session.get(User, 1)
if user:
    print(user.name)
```

### session.get_or_raise

Get a model by primary key, raising if not found.

```python
async def get_or_raise(self, model: type[T], id: Any) -> T
```

```python
try:
    user = await session.get_or_raise(User, 999)
except NoResultFound:
    print("User not found")
```

### session.update

Update a model's attributes.

```python
async def update(self, instance: T, **values: Any) -> None
```

```python
await session.update(user, name="Alicia", age=31)
print(user.name)  # "Alicia" - instance is updated too
```

### session.remove

Delete a model.

```python
async def remove(self, instance: T) -> None
```

```python
await session.remove(user)
```

---

## Upsert Operations

### session.upsert

Insert a model, or update it if a conflict occurs.

```python
async def upsert(
    self,
    instance: T,
    conflict_target: str | list[str],
    update_fields: list[str] | None = None,
) -> T
```

```python
# Insert or update based on email conflict
user = await session.upsert(
    User(email="alice@example.com", name="Alice Updated"),
    conflict_target="email",
    update_fields=["name"]
)
```

### session.upsert_all

Bulk upsert multiple models.

```python
async def upsert_all(
    self,
    instances: list[T],
    conflict_target: str | list[str],
    update_fields: list[str] | None = None,
) -> list[T]
```

```python
users = await session.upsert_all(
    [User(email="a@example.com", name="A"), User(email="b@example.com", name="B")],
    conflict_target="email",
    update_fields=["name"]
)
```

---

## Bulk Operations

### session.bulk_update

Update multiple records matching filters.

```python
async def bulk_update(
    self,
    model: type[T],
    values: dict[str, Any],
    **filters: Any,
) -> int
```

```python
# Update all users with role="guest" to role="member"
count = await session.bulk_update(User, {"role": "member"}, role="guest")
print(f"Updated {count} users")
```

---

## Soft Delete Operations

These methods work with models that inherit from `SoftDeleteMixin`.

### session.soft_delete

Soft delete a model (sets `deleted_at` timestamp).

```python
async def soft_delete(self, instance: T) -> None
```

```python
await session.soft_delete(article)
# article.deleted_at is now set
```

### session.restore

Restore a soft-deleted model.

```python
async def restore(self, instance: T) -> None
```

```python
await session.restore(article)
# article.deleted_at is now None
```

### session.force_delete

Permanently delete a model (bypasses soft delete).

```python
async def force_delete(self, instance: T) -> None
```

```python
await session.force_delete(article)
# Record is permanently removed from database
```

---

## Query Operations

### session.query

Create a query builder for a model.

```python
def query(self, model: type[T]) -> Query[T]
```

```python
users = await session.query(User).filter(age__gte=18).all()
```

See [Query API](query.md) for full query builder documentation.

### session.execute_raw

Execute raw SQL through the session.

```python
async def execute_raw(
    self,
    sql: str,
    params: list[Any],
) -> QueryResult
```

```python
result = await session.execute_raw(
    "SELECT * FROM users WHERE age > ?",
    [18]
)
```

### session.execute

Execute a statement object.

```python
async def execute(self, statement: Statement) -> Result
```

```python
from ormkit import select

stmt = select(User).where(User.age >= 18)
result = await session.execute(stmt)
users = result.scalars().all()
```

---

## Transaction Operations

### session.begin

Start a transaction context.

```python
def begin(self) -> TransactionContext
```

```python
async with session.begin() as tx:
    tx.add(User(name="Alice"))
    tx.add(User(name="Bob"))
# Auto-commits on exit
```

### session.transaction

Alternative transaction context manager.

```python
def transaction(self) -> TransactionContext
```

```python
async with session.transaction():
    await session.insert(User(name="Alice"))
    await session.insert(User(name="Bob"))
```

### session.add

Add a model to the pending changes (within a transaction).

```python
def add(self, instance: T) -> None
```

### session.delete

Mark a model for deletion (within a transaction).

```python
def delete(self, instance: T) -> None
```

### session.commit

Commit pending changes.

```python
async def commit(self) -> None
```

### session.rollback

Rollback pending changes.

```python
async def rollback(self) -> None
```

---

## session_context

Context manager that auto-commits on success.

```python
@asynccontextmanager
async def session_context(engine: Engine) -> AsyncSession
```

```python
from ormkit import session_context

async with session_context(engine) as session:
    await session.insert(User(name="Alice"))
    await session.insert(User(name="Bob"))
# Commits automatically
```

---

## TransactionContext

Returned by `session.begin()`.

### tx.add

Add a model to the transaction.

```python
def add(self, instance: T) -> None
```

### tx.delete

Mark a model for deletion.

```python
def delete(self, instance: T) -> None
```

### tx.commit

Commit the transaction early.

```python
async def commit(self) -> None
```

### Example

```python
async with session.begin() as tx:
    user = User(name="Alice")
    tx.add(user)

    if some_condition:
        await tx.commit()  # Commit early
        return user

    # More operations...
    post = Post(title="Hello", author_id=user.id)
    tx.add(post)
# Auto-commits if not already committed
```
