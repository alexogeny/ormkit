"""Tests for migration system (Alembic-compatible)."""

from __future__ import annotations

from pathlib import Path
from textwrap import dedent

import pytest

from ormkit import Base, Mapped, mapped_column


# Test models for migration generation
class User(Base):
    """Test model for migration tests."""

    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(max_length=100)
    email: Mapped[str] = mapped_column(max_length=255, unique=True)


class Post(Base):
    """Test model with foreign key."""

    __tablename__ = "posts"

    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column(max_length=200)
    user_id: Mapped[int]  # FK to users.id


@pytest.fixture
def alembic_dir(tmp_path: Path) -> Path:
    """Create a mock alembic directory structure."""
    alembic_path = tmp_path / "alembic"
    alembic_path.mkdir()
    (alembic_path / "versions").mkdir()

    # Create alembic.ini
    ini_content = dedent("""
        [alembic]
        script_location = alembic
        sqlalchemy.url = sqlite:///test.db

        [alembic:exclude]
        tables = alembic_version
    """).strip()
    (tmp_path / "alembic.ini").write_text(ini_content)

    return alembic_path


@pytest.fixture
def sample_migration(alembic_dir: Path) -> Path:
    """Create a sample Alembic migration file."""
    migration_content = dedent('''
        """Create users table

        Revision ID: abc123def456
        Revises: None
        Create Date: 2024-03-15 12:34:56.789012
        """
        from alembic import op
        import sqlalchemy as sa

        # revision identifiers, used by Alembic.
        revision = 'abc123def456'
        down_revision = None
        branch_labels = None
        depends_on = None

        def upgrade():
            op.create_table(
                'users',
                sa.Column('id', sa.Integer(), primary_key=True),
                sa.Column('name', sa.String(100), nullable=False),
                sa.Column('email', sa.String(255), nullable=False),
                sa.UniqueConstraint('email'),
            )

        def downgrade():
            op.drop_table('users')
    ''').strip()

    migration_path = alembic_dir / "versions" / "abc123def456_create_users_table.py"
    migration_path.write_text(migration_content)
    return migration_path


class TestAlembicCompatibility:
    """Test that OrmKit can work with existing Alembic migrations."""

    def test_detect_alembic_directory(self, alembic_dir: Path, tmp_path: Path) -> None:
        """Detect alembic/ directory and alembic.ini."""
        from ormkit.migrations.config import AlembicConfig

        config = AlembicConfig.detect(tmp_path)
        assert config is not None
        assert config.script_location == alembic_dir

    def test_read_alembic_config(self, alembic_dir: Path, tmp_path: Path) -> None:
        """Parse alembic.ini for script_location, sqlalchemy.url."""
        from ormkit.migrations.config import AlembicConfig

        config = AlembicConfig.from_ini(tmp_path / "alembic.ini")
        # script_location is resolved to absolute path relative to config file
        assert config.script_location == alembic_dir
        assert config.sqlalchemy_url == "sqlite:///test.db"

    def test_load_alembic_migration_file(self, sample_migration: Path) -> None:
        """Load and parse an Alembic migration script."""
        from ormkit.migrations.script import MigrationScript

        script = MigrationScript.load(sample_migration)
        assert script.revision == "abc123def456"
        assert script.down_revision is None
        assert script.branch_labels is None
        assert script.depends_on is None

    async def test_execute_alembic_upgrade(self, sqlite_pool, sample_migration: Path) -> None:
        """Run upgrade() from an Alembic migration."""
        from ormkit.migrations.runner import MigrationRunner
        from ormkit.migrations.script import MigrationScript

        script = MigrationScript.load(sample_migration)
        runner = MigrationRunner(sqlite_pool)

        await runner.run_upgrade(script)

        # Verify table was created
        result = await sqlite_pool.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='users'"
        )
        tables = result.all()
        assert len(tables) == 1

    async def test_execute_alembic_downgrade(self, sqlite_pool, sample_migration: Path) -> None:
        """Run downgrade() from an Alembic migration."""
        from ormkit.migrations.runner import MigrationRunner
        from ormkit.migrations.script import MigrationScript

        script = MigrationScript.load(sample_migration)
        runner = MigrationRunner(sqlite_pool)

        # First upgrade
        await runner.run_upgrade(script)

        # Then downgrade
        await runner.run_downgrade(script)

        # Verify table was dropped
        result = await sqlite_pool.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='users'"
        )
        tables = result.all()
        assert len(tables) == 0

    async def test_track_version_in_alembic_version_table(self, sqlite_pool) -> None:
        """Store applied versions in alembic_version table."""
        from ormkit.migrations.runner import MigrationRunner

        runner = MigrationRunner(sqlite_pool)

        # Stamp a version
        await runner.stamp("abc123def456")

        # Verify it's tracked
        current = await runner.get_current_revision()
        assert current == "abc123def456"

        # Verify table exists and has correct data
        result = await sqlite_pool.execute(
            "SELECT version_num FROM alembic_version"
        )
        versions = result.all()
        assert len(versions) == 1
        assert versions[0]["version_num"] == "abc123def456"


class TestMigrationGeneration:
    """Test auto-generating migrations from models."""

    async def test_generate_create_table(self, sqlite_pool, tmp_path: Path) -> None:
        """Generate migration for new model."""
        from ormkit.migrations.autogen import AutogenContext

        context = AutogenContext(sqlite_pool, [User])
        operations = await context.diff()

        # Should have a create_table operation
        assert len(operations) >= 1
        create_op = operations[0]
        assert create_op.operation_type == "create_table"
        assert create_op.table_name == "users"

    async def test_generate_add_column(self, sqlite_pool, tmp_path: Path) -> None:
        """Generate migration when column added to model."""
        from ormkit.migrations.autogen import AutogenContext

        # Create table without 'age' column
        await sqlite_pool.execute(
            """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL UNIQUE
            )
            """
        )

        # Model with age column
        class UserWithAge(Base):
            __tablename__ = "users"
            id: Mapped[int] = mapped_column(primary_key=True)
            name: Mapped[str] = mapped_column(max_length=100)
            email: Mapped[str] = mapped_column(max_length=255, unique=True)
            age: Mapped[int | None] = mapped_column(nullable=True)

        context = AutogenContext(sqlite_pool, [UserWithAge])
        operations = await context.diff()

        # Should have an add_column operation
        add_ops = [op for op in operations if op.operation_type == "add_column"]
        assert len(add_ops) == 1
        assert add_ops[0].column_name == "age"

    async def test_generate_drop_column(self, sqlite_pool, tmp_path: Path) -> None:
        """Generate migration when column removed from model."""
        from ormkit.migrations.autogen import AutogenContext

        # Create table with 'deprecated' column
        await sqlite_pool.execute(
            """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL UNIQUE,
                deprecated TEXT
            )
            """
        )

        # Model without deprecated column
        context = AutogenContext(sqlite_pool, [User])
        operations = await context.diff()

        # Should have a drop_column operation
        drop_ops = [op for op in operations if op.operation_type == "drop_column"]
        assert len(drop_ops) == 1
        assert drop_ops[0].column_name == "deprecated"

    async def test_generate_add_index(self, sqlite_pool, tmp_path: Path) -> None:
        """Generate migration for new index."""
        from ormkit.migrations.autogen import AutogenContext

        # Create table without index on name
        await sqlite_pool.execute(
            """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL UNIQUE
            )
            """
        )

        # Model with index on name
        class UserWithIndex(Base):
            __tablename__ = "users"
            id: Mapped[int] = mapped_column(primary_key=True)
            name: Mapped[str] = mapped_column(max_length=100, index=True)
            email: Mapped[str] = mapped_column(max_length=255, unique=True)

        context = AutogenContext(sqlite_pool, [UserWithIndex])
        operations = await context.diff()

        # Should have a create_index operation
        index_ops = [op for op in operations if op.operation_type == "create_index"]
        assert len(index_ops) >= 1

    async def test_output_alembic_compatible_format(self, sqlite_pool, tmp_path: Path) -> None:
        """Generated migrations should be valid Alembic files."""
        from ormkit.migrations.autogen import AutogenContext

        context = AutogenContext(sqlite_pool, [User])
        operations = await context.diff()

        # Render as Alembic migration
        content = context.render_migration("create users table", operations)

        # Should have required Alembic structure
        assert "revision = " in content
        assert "down_revision = " in content
        assert "def upgrade():" in content
        assert "def downgrade():" in content
        assert "from alembic import op" in content


class TestMigrationCLI:
    """Test CLI commands."""

    def test_migrate_init_creates_alembic_structure(self, tmp_path: Path) -> None:
        """ormkit migrate init creates alembic/ and alembic.ini."""
        from ormkit.cli import migrate_init

        migrate_init(tmp_path)

        assert (tmp_path / "alembic").exists()
        assert (tmp_path / "alembic" / "versions").exists()
        assert (tmp_path / "alembic.ini").exists()
        assert (tmp_path / "alembic" / "env.py").exists()

    def test_migrate_create_generates_empty_migration(self, alembic_dir: Path, tmp_path: Path) -> None:
        """ormkit migrate create NAME generates timestamped file."""
        from ormkit.cli import migrate_create

        migration_path = migrate_create(tmp_path, "add users table")

        assert migration_path.exists()
        assert "add_users_table" in migration_path.name
        content = migration_path.read_text()
        assert "def upgrade():" in content
        assert "def downgrade():" in content

    async def test_migrate_up_applies_pending(self, sqlite_pool, alembic_dir: Path, sample_migration: Path, tmp_path: Path) -> None:
        """ormkit migrate up runs all pending migrations."""
        from ormkit.cli import migrate_up

        await migrate_up(sqlite_pool, tmp_path)

        # Verify migration was applied
        result = await sqlite_pool.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='users'"
        )
        assert len(result.all()) == 1

    async def test_migrate_down_reverts_last(self, sqlite_pool, alembic_dir: Path, sample_migration: Path, tmp_path: Path) -> None:
        """ormkit migrate down reverts most recent migration."""
        from ormkit.cli import migrate_up, migrate_down

        # Apply
        await migrate_up(sqlite_pool, tmp_path)

        # Revert
        await migrate_down(sqlite_pool, tmp_path)

        # Verify table was dropped
        result = await sqlite_pool.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='users'"
        )
        assert len(result.all()) == 0

    async def test_migrate_status_shows_current_revision(self, sqlite_pool, alembic_dir: Path, sample_migration: Path, tmp_path: Path) -> None:
        """ormkit migrate status displays current state."""
        from ormkit.cli import migrate_status, migrate_up

        # Before applying
        status = await migrate_status(sqlite_pool, tmp_path)
        assert status["current_revision"] is None
        assert len(status["pending"]) == 1

        # After applying
        await migrate_up(sqlite_pool, tmp_path)
        status = await migrate_status(sqlite_pool, tmp_path)
        assert status["current_revision"] == "abc123def456"
        assert len(status["pending"]) == 0


class TestSchemaIntrospection:
    """Test database schema reading (Rust backend)."""

    async def test_get_sqlite_tables(self, sqlite_pool) -> None:
        """List all tables in SQLite database."""
        # Create some tables
        await sqlite_pool.execute(
            "CREATE TABLE test_table1 (id INTEGER PRIMARY KEY)"
        )
        await sqlite_pool.execute(
            "CREATE TABLE test_table2 (id INTEGER PRIMARY KEY)"
        )

        tables = await sqlite_pool.get_tables()

        assert "test_table1" in tables
        assert "test_table2" in tables
        assert "sqlite_sequence" not in tables  # Internal SQLite table excluded

    async def test_get_sqlite_columns(self, sqlite_pool) -> None:
        """Get column info for SQLite table."""
        await sqlite_pool.execute(
            """
            CREATE TABLE test_columns (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT UNIQUE,
                age INTEGER
            )
            """
        )

        columns = await sqlite_pool.get_columns("test_columns")

        assert len(columns) == 4

        id_col = next(c for c in columns if c.name == "id")
        assert id_col.is_primary_key is True

        name_col = next(c for c in columns if c.name == "name")
        assert name_col.nullable is False

        age_col = next(c for c in columns if c.name == "age")
        assert age_col.nullable is True

    async def test_get_sqlite_indexes(self, sqlite_pool) -> None:
        """Get index info for SQLite table."""
        await sqlite_pool.execute(
            """
            CREATE TABLE test_indexes (
                id INTEGER PRIMARY KEY,
                name TEXT,
                email TEXT
            )
            """
        )
        await sqlite_pool.execute(
            "CREATE INDEX idx_name ON test_indexes (name)"
        )
        await sqlite_pool.execute(
            "CREATE UNIQUE INDEX idx_email ON test_indexes (email)"
        )

        indexes = await sqlite_pool.get_indexes("test_indexes")

        name_idx = next((i for i in indexes if i.name == "idx_name"), None)
        assert name_idx is not None
        assert name_idx.unique is False
        assert "name" in name_idx.columns

        email_idx = next((i for i in indexes if i.name == "idx_email"), None)
        assert email_idx is not None
        assert email_idx.unique is True

    @pytest.mark.skipif(True, reason="Requires PostgreSQL")
    async def test_get_postgres_tables(self, postgres_pool) -> None:
        """List all tables in PostgreSQL database."""
        tables = await postgres_pool.get_tables()
        assert isinstance(tables, list)

    @pytest.mark.skipif(True, reason="Requires PostgreSQL")
    async def test_get_postgres_columns(self, postgres_pool) -> None:
        """Get column info for PostgreSQL table."""
        # Assumes a test table exists
        columns = await postgres_pool.get_columns("test_table")
        assert isinstance(columns, list)

    @pytest.mark.skipif(True, reason="Requires PostgreSQL")
    async def test_get_postgres_constraints(self, postgres_pool) -> None:
        """Get constraints for PostgreSQL table."""
        constraints = await postgres_pool.get_constraints("test_table")
        assert isinstance(constraints, list)


class TestMigrationOperations:
    """Test migration operation classes."""

    def test_create_table_operation(self) -> None:
        """CreateTable operation generates correct SQL."""
        from ormkit.migrations.operations import CreateTable, ColumnDef

        op = CreateTable(
            "users",
            [
                ColumnDef("id", "INTEGER", primary_key=True),
                ColumnDef("name", "VARCHAR(100)", nullable=False),
                ColumnDef("email", "VARCHAR(255)", unique=True),
            ],
        )

        sql = op.to_sql("sqlite")
        assert "CREATE TABLE users" in sql
        assert "id INTEGER" in sql
        assert "PRIMARY KEY" in sql
        assert "name VARCHAR(100) NOT NULL" in sql

    def test_drop_table_operation(self) -> None:
        """DropTable operation generates correct SQL."""
        from ormkit.migrations.operations import DropTable

        op = DropTable("users")
        sql = op.to_sql("sqlite")
        assert sql == "DROP TABLE users"

    def test_add_column_operation(self) -> None:
        """AddColumn operation generates correct SQL."""
        from ormkit.migrations.operations import AddColumn, ColumnDef

        op = AddColumn("users", ColumnDef("age", "INTEGER", nullable=True))
        sql = op.to_sql("sqlite")
        assert "ALTER TABLE users ADD COLUMN age INTEGER" in sql

    def test_drop_column_operation(self) -> None:
        """DropColumn operation generates correct SQL."""
        from ormkit.migrations.operations import DropColumn

        op = DropColumn("users", "deprecated_field")
        sql = op.to_sql("sqlite")
        assert "ALTER TABLE users DROP COLUMN deprecated_field" in sql

    def test_create_index_operation(self) -> None:
        """CreateIndex operation generates correct SQL."""
        from ormkit.migrations.operations import CreateIndex

        op = CreateIndex("idx_users_email", "users", ["email"], unique=True)
        sql = op.to_sql("sqlite")
        assert "CREATE UNIQUE INDEX idx_users_email ON users (email)" in sql
