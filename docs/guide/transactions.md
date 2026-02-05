# Transactions

Transactions ensure that multiple operations succeed or fail together atomically.

## Basic Transaction

Use `session.begin()` as a context manager:

```python
async with session.begin() as tx:
    user = User(name="Alice", email="alice@example.com")
    tx.add(user)

    post = Post(title="Hello", author_id=user.id)
    tx.add(post)

# Commits automatically when exiting the context
```

If an exception occurs, the transaction rolls back automatically:

```python
try:
    async with session.begin() as tx:
        tx.add(User(name="Alice", email="alice@example.com"))
        tx.add(User(name="Bob", email="alice@example.com"))  # Duplicate!
        # Raises constraint violation
except Exception as e:
    # Transaction is rolled back, no users created
    print(f"Failed: {e}")
```

## Session Context

For simple auto-commit behavior:

```python
from ormkit import session_context

async with session_context(engine) as session:
    await session.insert(User(name="Alice", email="alice@example.com"))
    await session.insert(User(name="Bob", email="bob@example.com"))
    # Commits all changes on successful exit
```

## Manual Commit/Rollback

For fine-grained control:

```python
session = AsyncSession(engine)

try:
    user = User(name="Alice", email="alice@example.com")
    session.add(user)

    post = Post(title="Hello", author_id=user.id)
    session.add(post)

    await session.commit()
except Exception:
    await session.rollback()
    raise
```

## Transaction Patterns

### Read-Modify-Write

```python
async with session.begin() as tx:
    # Read current value
    user = await session.get(User, user_id)

    # Modify
    new_balance = user.balance - amount
    if new_balance < 0:
        raise ValueError("Insufficient balance")

    # Write
    await session.update(user, balance=new_balance)
```

### Batch Operations

```python
async with session.begin() as tx:
    for user_data in users_to_create:
        tx.add(User(**user_data))
    # All users created atomically
```

### Conditional Commit

```python
async with session.begin() as tx:
    user = User(name="Alice", email="alice@example.com")
    tx.add(user)

    if some_condition:
        # Commit early
        await tx.commit()
        return

    # More operations...
    post = Post(title="Hello", author_id=user.id)
    tx.add(post)
```

## Raw SQL Transactions

Use `pool.transaction()` for raw SQL operations:

```python
async with await engine.transaction() as tx:
    await tx.execute(
        "UPDATE accounts SET balance = balance - $1 WHERE id = $2",
        [100, sender_id]
    )
    await tx.execute(
        "UPDATE accounts SET balance = balance + $1 WHERE id = $2",
        [100, receiver_id]
    )
```

## Performance

OrmKit optimizes transaction performance:

1. **Deferred BEGIN** - The `BEGIN` statement isn't sent until the first query
2. **Pipelined Queries** - Multiple queries are batched when possible
3. **Fast COMMIT** - Uses simple query protocol for minimal overhead

!!! tip "Transaction Overhead"
    OrmKit's transaction overhead is minimal. In benchmarks, a read-modify-write transaction takes ~0.21ms—competitive with raw asyncpg.

## Error Handling

### Constraint Violations

```python
from ormkit.exceptions import IntegrityError

try:
    async with session.begin() as tx:
        tx.add(User(email="existing@example.com"))
except IntegrityError as e:
    if "unique" in str(e).lower():
        print("Email already exists")
    else:
        raise
```

### Deadlocks

For high-concurrency scenarios:

```python
import asyncio
from ormkit.exceptions import OperationalError

async def transfer_with_retry(from_id: int, to_id: int, amount: float, retries: int = 3):
    for attempt in range(retries):
        try:
            async with session.begin() as tx:
                # Always lock in consistent order to avoid deadlocks
                ids = sorted([from_id, to_id])
                for id in ids:
                    await session.execute_raw(
                        "SELECT * FROM accounts WHERE id = ? FOR UPDATE",
                        [id]
                    )

                # Transfer
                await session.execute_raw(
                    "UPDATE accounts SET balance = balance - ? WHERE id = ?",
                    [amount, from_id]
                )
                await session.execute_raw(
                    "UPDATE accounts SET balance = balance + ? WHERE id = ?",
                    [amount, to_id]
                )
                return
        except OperationalError as e:
            if "deadlock" in str(e).lower() and attempt < retries - 1:
                await asyncio.sleep(0.1 * (attempt + 1))
                continue
            raise
```

## Best Practices

1. **Keep transactions short** - Long transactions hold locks
2. **Don't do I/O in transactions** - No HTTP calls, file operations, etc.
3. **Handle errors** - Always have a strategy for constraint violations
4. **Use context managers** - They ensure proper cleanup

```python
# Good: Short transaction
async with session.begin() as tx:
    tx.add(User(name="Alice"))

# Bad: Long transaction with I/O
async with session.begin() as tx:
    tx.add(User(name="Alice"))
    await send_welcome_email()  # Don't do this!
    tx.add(audit_log)
```

## Next Steps

- [Write raw SQL](raw-sql.md) for complex operations
- [Optimize performance](../performance/optimization.md)
- [See the benchmarks](../performance/benchmarks.md)
