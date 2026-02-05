# Installation

## Requirements

- **Python**: 3.10, 3.11, 3.12, or 3.13
- **Operating System**: Linux, macOS, or Windows
- **Database**: PostgreSQL 12+ or SQLite 3.35+

## Install from PyPI

=== "pip"

    ```bash
    pip install ormkit
    ```

=== "uv"

    ```bash
    uv add ormkit
    ```

=== "poetry"

    ```bash
    poetry add ormkit
    ```

=== "pdm"

    ```bash
    pdm add ormkit
    ```

!!! note "No Extra Dependencies"
    Unlike other async ORMs, OrmKit includes its database drivers. You don't need to install `asyncpg`, `aiosqlite`, or `psycopg` separately.

## Verify Installation

```python
import ormkit
print(ormkit.__version__)
```

## Database Setup

### PostgreSQL

Make sure PostgreSQL is running and create a database:

```bash
createdb myapp
```

Or using SQL:

```sql
CREATE DATABASE myapp;
```

### SQLite

No setup required! SQLite databases are created automatically:

```python
# Creates the file if it doesn't exist
engine = await create_engine("sqlite:///myapp.db")

# Or use an in-memory database for testing
engine = await create_engine("sqlite::memory:")
```

## Development Installation

If you want to contribute or build from source:

```bash
# Clone the repository
git clone https://github.com/yourusername/ormkit.git
cd ormkit

# Create virtual environment
uv venv
source .venv/bin/activate

# Install in development mode
uv pip install maturin
maturin develop --release

# Run tests
pytest tests/ -v
```

## Troubleshooting

### Rust Compilation Errors

OrmKit includes pre-built wheels for most platforms. If you see Rust compilation errors:

1. Make sure you have Rust installed: `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
2. Ensure you have a C compiler (gcc, clang, or MSVC)
3. Try installing with verbose output: `pip install ormkit -v`

### Import Errors

If you get `ImportError: cannot import name '_ormkit'`:

1. Make sure you're not in the source directory
2. Try reinstalling: `pip install --force-reinstall ormkit`

### PostgreSQL Connection Issues

```python
# Check your connection string format
# Correct:
"postgresql://user:password@localhost:5432/dbname"

# Common mistakes:
"postgres://..."  # Use 'postgresql', not 'postgres'
"postgresql://localhost/dbname"  # Missing user - may work depending on pg_hba.conf
```

## Next Steps

Now that OrmKit is installed, continue to the [Quick Start](quickstart.md) guide.
