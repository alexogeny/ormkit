# Raw SQL

Sometimes you need the full power of SQL. OrmKit makes raw queries easy and fast.

## Execute Raw Queries

### Through Session

```python
result = await session.execute_raw(
    "SELECT * FROM users WHERE age > ?",
    [18]
)

for row in result.all():
    print(row["name"], row["age"])
```

### Through Engine

```python
result = await engine.execute(
    "SELECT id, name, email FROM users WHERE status = $1",
    ["active"]
)
```

!!! note "Parameter Syntax"
    - PostgreSQL uses `$1`, `$2`, etc.
    - SQLite uses `?`

## Result Formats

### Dictionaries (Default)

```python
result = await engine.execute("SELECT * FROM users", [])
rows = result.all()

for row in rows:
    print(row["name"])  # Access by column name
```

### Tuples (Fastest)

```python
result = await engine.execute("SELECT id, name FROM users", [])
tuples = result.tuples()

for id, name in tuples:
    print(f"{id}: {name}")
```

### Single Column

```python
result = await engine.execute("SELECT name FROM users", [])
names = result.column("name")  # ["Alice", "Bob", "Charlie"]
```

### First Row

```python
result = await engine.execute(
    "SELECT * FROM users WHERE email = $1",
    ["alice@example.com"]
)
row = result.first()  # Single dict or None
```

## Complex Queries

### JOINs

```python
result = await engine.execute("""
    SELECT
        p.id,
        p.title,
        u.name as author_name,
        COUNT(c.id) as comment_count
    FROM posts p
    JOIN users u ON p.author_id = u.id
    LEFT JOIN comments c ON c.post_id = p.id
    WHERE p.published = $1
    GROUP BY p.id, p.title, u.name
    ORDER BY comment_count DESC
    LIMIT $2
""", [True, 10])

for row in result.all():
    print(f"{row['title']} by {row['author_name']}: {row['comment_count']} comments")
```

### Subqueries

```python
result = await engine.execute("""
    SELECT * FROM users
    WHERE id IN (
        SELECT DISTINCT author_id FROM posts
        WHERE created_at > NOW() - INTERVAL '7 days'
    )
""", [])
```

### Window Functions

```python
result = await engine.execute("""
    SELECT
        name,
        department,
        salary,
        RANK() OVER (PARTITION BY department ORDER BY salary DESC) as rank
    FROM employees
""", [])
```

### CTEs (WITH clauses)

```python
result = await engine.execute("""
    WITH active_users AS (
        SELECT * FROM users WHERE last_login > NOW() - INTERVAL '30 days'
    ),
    user_post_counts AS (
        SELECT author_id, COUNT(*) as post_count
        FROM posts
        GROUP BY author_id
    )
    SELECT u.name, COALESCE(p.post_count, 0) as posts
    FROM active_users u
    LEFT JOIN user_post_counts p ON u.id = p.author_id
    ORDER BY posts DESC
""", [])
```

## Transactions with Raw SQL

```python
async with await engine.transaction() as tx:
    # Transfer money atomically
    await tx.execute(
        "UPDATE accounts SET balance = balance - $1 WHERE id = $2",
        [100, sender_id]
    )
    await tx.execute(
        "UPDATE accounts SET balance = balance + $1 WHERE id = $2",
        [100, receiver_id]
    )
    # Commits on exit, rolls back on exception
```

## Bulk Operations

### Bulk Insert

```python
# PostgreSQL - use unnest for best performance
await engine.execute("""
    INSERT INTO users (name, email)
    SELECT * FROM unnest($1::text[], $2::text[])
""", [names_list, emails_list])

# SQLite - use INSERT with multiple VALUES
values = ", ".join(["(?, ?)" for _ in users])
params = [v for u in users for v in (u["name"], u["email"])]
await engine.execute(f"INSERT INTO users (name, email) VALUES {values}", params)
```

### Bulk Update

```python
# PostgreSQL
await engine.execute("""
    UPDATE users
    SET status = 'archived'
    WHERE last_login < NOW() - INTERVAL '1 year'
""", [])

# With RETURNING
result = await engine.execute("""
    UPDATE users
    SET status = 'archived'
    WHERE last_login < $1
    RETURNING id, email
""", [cutoff_date])
archived = result.all()
```

## Performance Tips

!!! tip "Use Tuples for Large Results"
    ```python
    result = await engine.execute("SELECT * FROM big_table", [])
    rows = result.tuples()  # 15% faster than dicts
    ```

!!! tip "Use LIMIT"
    Always limit results when you don't need everything:
    ```python
    result = await engine.execute(
        "SELECT * FROM logs ORDER BY created_at DESC LIMIT $1",
        [100]
    )
    ```

!!! tip "Use Prepared Statements"
    Repeated queries are automatically prepared and cached. The second execution is faster:
    ```python
    # First call: parse + execute
    await engine.execute("SELECT * FROM users WHERE id = $1", [1])

    # Subsequent calls: just execute (cached)
    await engine.execute("SELECT * FROM users WHERE id = $1", [2])
    await engine.execute("SELECT * FROM users WHERE id = $1", [3])
    ```

## Mixing ORM and Raw SQL

```python
# Use ORM for simple operations
user = await session.insert(User(name="Alice", email="alice@example.com"))

# Use raw SQL for complex queries
result = await engine.execute("""
    SELECT
        date_trunc('day', created_at) as day,
        COUNT(*) as signups
    FROM users
    WHERE created_at > $1
    GROUP BY day
    ORDER BY day
""", [start_date])

stats = result.all()
```

## Database-Specific Features

### PostgreSQL

```python
# JSONB operations
await engine.execute("""
    SELECT * FROM users
    WHERE metadata @> $1::jsonb
""", ['{"premium": true}'])

# Array operations
await engine.execute("""
    SELECT * FROM posts
    WHERE $1 = ANY(tags)
""", ["python"])

# Full-text search
await engine.execute("""
    SELECT * FROM posts
    WHERE to_tsvector('english', content) @@ plainto_tsquery('english', $1)
""", ["python async"])
```

### SQLite

```python
# JSON operations (SQLite 3.38+)
await engine.execute("""
    SELECT * FROM users
    WHERE json_extract(metadata, '$.premium') = 1
""", [])

# FTS5 full-text search
await engine.execute("""
    SELECT * FROM posts_fts
    WHERE posts_fts MATCH ?
""", ["python"])
```

## Next Steps

- [Optimize performance](../performance/optimization.md)
- [See the benchmarks](../performance/benchmarks.md)
- [API reference](../api/engine.md)
