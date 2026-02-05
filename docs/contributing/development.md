# Development Setup

## Prerequisites

- Python 3.10+
- Rust 1.70+ (install via [rustup](https://rustup.rs/))
- PostgreSQL 12+ (for integration tests)
- [uv](https://github.com/astral-sh/uv) (recommended) or pip

## Clone and Setup

```bash
# Clone the repository
git clone https://github.com/yourusername/ormkit.git
cd ormkit

# Create virtual environment
uv venv
source .venv/bin/activate

# Install dependencies
uv sync --group dev
```

## Building

### Development Build

```bash
# Fast build for development (debug mode)
uv run maturin develop

# Or with release optimizations
uv run maturin develop --release
```

### Rebuild on Changes

The Rust extension needs rebuilding when you change `.rs` files:

```bash
# Quick rebuild
uv run maturin develop
```

## Running Tests

```bash
# Run all tests
uv run pytest tests/ -v

# Run specific test file
uv run pytest tests/test_session.py -v

# Run with coverage
uv run pytest tests/ --cov=ormkit --cov-report=html
```

### Integration Tests

Some tests require a running PostgreSQL database:

```bash
# Start PostgreSQL with Docker
docker run -d --name fk-test-postgres \
  -e POSTGRES_USER=test \
  -e POSTGRES_PASSWORD=test \
  -e POSTGRES_DB=test \
  -p 5433:5432 \
  postgres:16

# Run integration tests
POSTGRES_URL="postgresql://test:test@localhost:5433/test" \
  uv run pytest tests/test_postgres.py -v

# Cleanup
docker stop fk-test-postgres && docker rm fk-test-postgres
```

## Code Quality

### Linting

```bash
# Python linting
uv run ruff check python/ tests/

# Auto-fix
uv run ruff check python/ tests/ --fix

# Format
uv run ruff format python/ tests/
```

### Type Checking

```bash
uv run pyright python/ tests/
```

### Rust

```bash
# Check Rust code
cargo check

# Clippy lints
cargo clippy

# Format
cargo fmt
```

## Running Benchmarks

```bash
# Install benchmark dependencies
uv sync --group bench

# Start benchmark PostgreSQL
docker run -d --name bench-postgres \
  -e POSTGRES_USER=bench \
  -e POSTGRES_PASSWORD=bench \
  -e POSTGRES_DB=bench \
  -p 5499:5432 \
  postgres:16

# Run benchmarks
uv run python benchmarks/runner.py postgres

# With more runs for stability
uv run python benchmarks/runner.py postgres --runs 5
```

## Project Structure

```
ormkit/
├── python/ormkit/     # Python package
│   ├── __init__.py       # Public API exports
│   ├── models.py         # Model definition (Base, mapped_column, etc.)
│   ├── session.py        # AsyncSession and query builder
│   └── ...
├── src/                   # Rust source
│   ├── lib.rs            # PyO3 module definition
│   ├── pool.rs           # Connection pool and Python bindings
│   ├── executor.rs       # Query execution
│   └── pg/               # PostgreSQL driver
│       ├── connection.rs # Low-level connection
│       ├── protocol.rs   # Wire protocol
│       └── ...
├── tests/                 # Python tests
├── benchmarks/            # Benchmark suite
└── docs/                  # Documentation (MkDocs)
```

## Making Changes

### Python Changes

Edit files in `python/ormkit/`. Changes take effect immediately.

### Rust Changes

1. Edit files in `src/`
2. Rebuild: `uv run maturin develop`
3. Test your changes

### Documentation Changes

```bash
# Install docs dependencies
uv pip install mkdocs-material mkdocs-minify-plugin

# Serve locally with hot reload
mkdocs serve

# Build static site
mkdocs build
```

## Pull Request Process

1. **Fork** the repository
2. **Create a branch** for your changes
3. **Make changes** with tests
4. **Run checks**: `uv run ruff check && uv run pyright && uv run pytest`
5. **Open a PR** with a clear description

### PR Guidelines

- Keep PRs focused on a single change
- Add tests for new functionality
- Update docs if needed
- Follow existing code style
- Write clear commit messages

## Release Process

Releases are automated via GitHub Actions:

1. Update version in `pyproject.toml`
2. Create a git tag: `git tag v0.1.0`
3. Push: `git push origin v0.1.0`
4. GitHub Actions builds and publishes to PyPI
