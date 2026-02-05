# Engine API

The engine manages database connections and executes raw queries.

## create_engine

Create a database engine with connection pooling.

```python
async def create_engine(
    url: str,
    *,
    min_connections: int = 1,
    max_connections: int = 10,
) -> Engine
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `url` | `str` | required | Database connection URL |
| `min_connections` | `int` | `1` | Minimum idle connections in pool |
| `max_connections` | `int` | `10` | Maximum connections in pool |

### Connection URL Format

```python
# PostgreSQL
"postgresql://user:password@host:port/database"
"postgresql://localhost/mydb"

# SQLite
"sqlite:///path/to/database.db"
"sqlite::memory:"
```

### Example

```python
from ormkit import create_engine

engine = await create_engine(
    "postgresql://user:pass@localhost:5432/mydb",
    min_connections=5,
    max_connections=20,
)
```

---

## Engine.execute

Execute a raw SQL query.

```python
async def execute(
    self,
    sql: str,
    params: list[Any],
) -> QueryResult
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `sql` | `str` | SQL query string |
| `params` | `list` | Query parameters |

### Parameter Syntax

- PostgreSQL: `$1`, `$2`, `$3`, ...
- SQLite: `?`, `?`, `?`, ...

### Example

```python
# PostgreSQL
result = await engine.execute(
    "SELECT * FROM users WHERE age > $1 AND status = $2",
    [18, "active"]
)

# SQLite
result = await engine.execute(
    "SELECT * FROM users WHERE age > ? AND status = ?",
    [18, "active"]
)
```

---

## Engine.transaction

Start a transaction for raw SQL operations.

```python
async def transaction(self) -> Transaction
```

### Example

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
# Auto-commits on success, rolls back on exception
```

---

## Engine.create_all

Create all tables defined by registered models.

```python
async def create_all(self) -> None
```

### Example

```python
engine = await create_engine("sqlite::memory:")
await engine.create_all()
```

!!! note
    This creates tables that don't exist but won't modify existing tables. For production, use a migration tool.

---

## Engine.close

Close the engine and all connections.

```python
async def close(self) -> None
```

### Example

```python
engine = await create_engine("postgresql://localhost/mydb")
try:
    # Use engine...
finally:
    await engine.close()
```

---

## QueryResult

Result of a query execution.

### QueryResult.all

Get all rows as dictionaries.

```python
def all(self) -> list[dict[str, Any]]
```

```python
result = await engine.execute("SELECT * FROM users", [])
rows = result.all()
# [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
```

### QueryResult.tuples

Get all rows as tuples (faster than dicts).

```python
def tuples(self) -> list[tuple[Any, ...]]
```

```python
result = await engine.execute("SELECT id, name FROM users", [])
rows = result.tuples()
# [(1, "Alice"), (2, "Bob")]
```

### QueryResult.first

Get the first row or None.

```python
def first(self) -> dict[str, Any] | None
```

```python
result = await engine.execute(
    "SELECT * FROM users WHERE id = $1", [1]
)
user = result.first()
# {"id": 1, "name": "Alice"} or None
```

### QueryResult.column

Get a single column as a list.

```python
def column(self, name: str) -> list[Any]
```

```python
result = await engine.execute("SELECT name FROM users", [])
names = result.column("name")
# ["Alice", "Bob", "Charlie"]
```

### QueryResult.rowcount

Number of rows affected or returned.

```python
@property
def rowcount(self) -> int
```

```python
result = await engine.execute("DELETE FROM users WHERE status = $1", ["inactive"])
print(f"Deleted {result.rowcount} users")
```

---

## Transaction

A database transaction context.

### Transaction.execute

Execute a query within the transaction.

```python
async def execute(
    self,
    sql: str,
    params: list[Any],
) -> QueryResult
```

### Example

```python
async with await engine.transaction() as tx:
    result = await tx.execute(
        "SELECT balance FROM accounts WHERE id = $1 FOR UPDATE",
        [account_id]
    )
    balance = result.first()["balance"]

    await tx.execute(
        "UPDATE accounts SET balance = $1 WHERE id = $2",
        [balance - amount, account_id]
    )
```
