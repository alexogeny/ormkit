# Architecture

OrmKit uses a layered architecture with Python for the API and Rust for performance-critical operations.

## Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Python Layer                              │
│  - Declarative model definitions (like SQLAlchemy 2.0)      │
│  - Type hints with Mapped[] / mapped_column()               │
│  - Pythonic query builder API                               │
│  - Async session management                                  │
│  - Relationship loading (selectinload, joinedload)          │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Rust Core (PyO3)                         │
│  - Query execution                                          │
│  - Connection pool management                               │
│  - Lazy row conversion (data stays in Rust until accessed)  │
│  - Type-safe parameter binding                              │
│  - Model instantiation                                      │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   Custom Database Drivers                   │
│  - PostgreSQL: Custom wire protocol implementation          │
│  - SQLite: rusqlite with tokio-rusqlite async wrapper       │
└─────────────────────────────────────────────────────────────┘
```

## Python Layer

### Models (`python/ormkit/base.py`)

Defines the declarative model API:

- `Base` - Base class for all models (via `ModelMeta` metaclass)
- `Mapped[]` - Type hint wrapper
- `mapped_column()` - Column definition
- `relationship()` - Relationship definition
- `ForeignKey()` - Foreign key reference

### Fields (`python/ormkit/fields.py`)

Column and field definitions:

- `ColumnInfo` - Column metadata container
- `ForeignKey` - Foreign key reference
- `JSON` - JSON column marker

### Session (`python/ormkit/session.py`)

Provides the ORM interface:

- `AsyncSession` - Main session class
- Unit of Work pattern with identity map
- Query builder with Django-style filters
- Relationship loading strategies
- Transaction management

### Query (`python/ormkit/query.py`)

Query building and statement generation:

- `select()`, `insert()`, `update()`, `delete()` statement builders
- Statement classes for SQLAlchemy-style API

### Relationships (`python/ormkit/relationships.py`)

Relationship definitions and loading:

- `relationship()` - Relationship definition
- `selectinload`, `joinedload`, `noload` - Eager loading strategies

### Mixins (`python/ormkit/mixins.py`)

Optional model mixins:

- `SoftDeleteMixin` - Adds soft delete functionality

### Migrations (`python/ormkit/migrations/`)

Database migration system:

- Alembic-compatible migration runner
- Auto-generation of migration scripts
- Schema introspection

### Key Design Decisions

1. **Metaclass-based models** - `ModelMeta` processes model definitions
2. **Explicit over implicit** - Relationships must be eagerly loaded
3. **Async-only** - No sync API to avoid complexity
4. **Django-style filters** - Familiar `field__operator` syntax

## Rust Core

### Pool (`src/pool.rs`)

Connection pool router:

- `ConnectionPool` - Routes between PostgreSQL and SQLite pools
- Manages connection lifecycle
- Handles async execution via tokio

### Executor (`src/executor.rs`)

Query execution and result handling:

- `QueryResult` - Result container with lazy row access
- `LazyRow` - Deferred row conversion
- `RowValue` - Rust-native value representation
- Model instantiation via PyO3

### PostgreSQL Driver (`src/pg/`)

Custom PostgreSQL driver with wire protocol implementation:

```
src/pg/
├── connection.rs   # Connection state and queries
├── protocol.rs     # Wire protocol messages
├── pool.rs         # PostgreSQL-specific pooling
├── types.rs        # Type conversion (Rust ↔ PostgreSQL)
├── auth.rs         # SCRAM-SHA-256 authentication
└── error.rs        # Error types
```

### SQLite Driver (`src/sqlite/`)

SQLite driver using rusqlite:

```
src/sqlite/
├── connection.rs   # Connection wrapper
├── pool.rs         # SQLite-specific pooling
└── types.rs        # Type conversion
```

### Schema (`src/schema.rs`)

Schema introspection for migrations:

- Table and column metadata extraction
- Foreign key relationship detection

## Performance Optimizations

### 1. Lazy Row Conversion

Rows stay in Rust until accessed:

```rust
pub struct LazyRow {
    values: SmallVec<[RowValue; 16]>,  // Inline storage for small rows
    columns: Arc<Vec<String>>,
}

impl LazyRow {
    // Only converts when Python accesses the row
    fn to_dict(&self, py: Python) -> PyResult<PyDict> {
        // Convert RowValue → Python objects here
    }
}
```

The `SmallVec` with inline capacity of 16 avoids heap allocation for most tables.

### 2. Statement Caching

Prepared statements are cached per connection:

```rust
pub struct StatementCache {
    cache: LruCache<String, PreparedStatement>,
    counter: usize,  // For generating statement names
}
```

### 3. Pipelined Transactions

BEGIN is deferred until the first query:

```
Traditional:
  BEGIN → wait → Query1 → wait → Query2 → wait → COMMIT

OrmKit:
  [BEGIN + Query1] → wait → Query2 → wait → COMMIT
```

### 4. Simple Query Protocol for COMMIT

COMMIT uses PostgreSQL's simple query protocol:

```rust
// Extended protocol (4 messages):
// Parse → Bind → Execute → Sync

// Simple protocol (1 message):
pub async fn commit(&mut self) -> PgResult<()> {
    self.send_message(&QueryMessage { query: "COMMIT" }).await?;
    // ...
}
```

### 5. Fast Model Instantiation

Rust-side model creation is 4.9x faster than Python:

```rust
// Creates Python model instances directly from Rust
pub fn to_models(&self, py: Python, model_cls: &PyType) -> PyResult<Vec<PyObject>> {
    // Direct attribute setting without going through __init__
}
```

## Wire Protocol

OrmKit implements the PostgreSQL wire protocol directly:

### Message Types

```rust
pub enum FrontendMessage {
    Query(String),           // Simple query
    Parse { ... },           // Prepare statement
    Bind { ... },            // Bind parameters
    Execute { ... },         // Execute prepared
    Describe { ... },        // Get column info
    Sync,                    // End extended query
    Flush,                   // Flush without sync
}

pub enum BackendMessage {
    RowDescription { ... },  // Column metadata
    DataRow { ... },         // Row data
    CommandComplete { ... }, // Query finished
    ReadyForQuery { ... },   // Can send next query
    // ...
}
```

### Connection State Machine

```
Idle ──Query──→ InQuery ──Complete──→ Idle
  │                                     ▲
  └──BEGIN──→ InTransaction ──COMMIT──┘
                    │
                    └──ROLLBACK──→ Idle
```

## Testing Strategy

### Unit Tests

Test Python API in isolation:

```python
# tests/test_models.py
def test_model_definition():
    class User(Base):
        __tablename__ = "users"
        id: Mapped[int] = mapped_column(primary_key=True)

    assert User.__tablename__ == "users"
```

### Integration Tests

Test against real databases:

```python
# tests/test_session.py
async def test_insert_and_query():
    engine = await create_engine("sqlite::memory:")
    session = AsyncSession(engine)

    user = await session.insert(User(name="Alice"))
    assert user.id is not None

    found = await session.get(User, user.id)
    assert found.name == "Alice"
```

### Benchmark Tests

Ensure performance doesn't regress:

```python
# benchmarks/runner.py
async def bench_select_by_id():
    # Warm up
    for _ in range(WARMUP):
        await session.get(User, 1)

    # Measure
    start = time.perf_counter()
    for _ in range(ITERATIONS):
        await session.get(User, 1)
    return (time.perf_counter() - start) / ITERATIONS
```

## Extension Points

### Adding a New Database

1. Implement connection in `src/<db>/connection.rs`
2. Add protocol handling in `src/<db>/protocol.rs`
3. Add type conversion in `src/<db>/types.rs`
4. Register in pool creation in `src/pool.rs`

### Adding a Query Operator

1. Add to operator map in `python/ormkit/session.py`:

```python
OPERATORS = {
    "gt": ">",
    "gte": ">=",
    # Add new operator here
    "contains": "LIKE",  # Will need special value handling
}
```

2. Handle special value transformations if needed

### Adding a Model Mixin

1. Create mixin class in `python/ormkit/mixins.py`:

```python
class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(default=datetime.now)
    updated_at: Mapped[datetime | None] = mapped_column(nullable=True)
```

2. Add session support if needed for special operations

## File Structure

```
ormkit/
├── python/ormkit/          # Python package
│   ├── __init__.py         # Public exports
│   ├── base.py             # Base class and metaclass
│   ├── session.py          # AsyncSession
│   ├── query.py            # Statement builders
│   ├── fields.py           # Column definitions
│   ├── relationships.py    # Relationship support
│   ├── mixins.py           # Optional mixins
│   └── migrations/         # Migration system
├── src/                    # Rust source
│   ├── lib.rs              # PyO3 module definition
│   ├── pool.rs             # Connection pool
│   ├── executor.rs         # Query execution
│   ├── schema.rs           # Schema introspection
│   ├── error.rs            # Error types
│   ├── pg/                 # PostgreSQL driver
│   └── sqlite/             # SQLite driver
├── tests/                  # Test suite
└── benchmarks/             # Performance benchmarks
```
