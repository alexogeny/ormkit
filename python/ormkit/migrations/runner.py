"""Migration runner - executes migrations against a database."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from ormkit.migrations.config import AlembicConfig
from ormkit.migrations.operations import Operations
from ormkit.migrations.script import MigrationScript

if TYPE_CHECKING:
    from ormkit._ormkit import ConnectionPool


@dataclass
class MigrationState:
    """Current migration state."""

    current_revision: str | None
    """Current applied revision (None if no migrations applied)."""

    pending_count: int
    """Number of pending migrations."""

    applied_revisions: list[str]
    """List of applied revision IDs."""


class MigrationRunner:
    """Execute migrations against a database.

    This runner:
    - Tracks applied migrations in the version table
    - Loads migration scripts from the versions directory
    - Executes upgrade/downgrade operations
    - Supports Alembic-compatible migrations

    Example:
        runner = MigrationRunner(pool, config)
        await runner.upgrade()  # Apply all pending
        await runner.downgrade("-1")  # Rollback last
    """

    def __init__(
        self,
        pool: ConnectionPool,
        config: AlembicConfig | None = None,
    ) -> None:
        """Initialize the migration runner.

        Args:
            pool: Database connection pool
            config: Alembic configuration (optional for simple operations)
        """
        self.pool = pool
        self.config = config
        self._dialect = "postgresql" if pool.is_postgres() else "sqlite"
        self._version_table = config.version_table if config else "alembic_version"

    async def ensure_version_table(self) -> None:
        """Create the version table if it doesn't exist."""
        table = self._version_table
        if self._dialect == "postgresql":
            sql = f"""
                CREATE TABLE IF NOT EXISTS "{table}" (
                    version_num VARCHAR(32) NOT NULL,
                    CONSTRAINT {table}_pkc PRIMARY KEY (version_num)
                )
            """
        else:  # sqlite
            sql = f"""
                CREATE TABLE IF NOT EXISTS "{table}" (
                    version_num VARCHAR(32) NOT NULL PRIMARY KEY
                )
            """
        await self.pool.execute(sql)

    async def get_current_revision(self) -> str | None:
        """Get the current applied revision.

        Returns:
            Current revision ID, or None if no migrations applied
        """
        await self.ensure_version_table()
        table = self._version_table

        result = await self.pool.execute(f'SELECT version_num FROM "{table}" LIMIT 1')
        row = result.first()
        if row:
            return row["version_num"]
        return None

    async def get_applied_revisions(self) -> list[str]:
        """Get all applied revision IDs.

        Returns:
            List of applied revision IDs
        """
        await self.ensure_version_table()
        table = self._version_table

        result = await self.pool.execute(f'SELECT version_num FROM "{table}"')
        return [row["version_num"] for row in result.all()]

    async def stamp(self, revision: str) -> None:
        """Stamp a revision as current without running migrations.

        Args:
            revision: Revision ID to stamp
        """
        await self.ensure_version_table()
        table = self._version_table

        # Remove all existing versions
        await self.pool.execute(f'DELETE FROM "{table}"')

        # Insert new version
        if self._dialect == "postgresql":
            await self.pool.execute(
                f'INSERT INTO "{table}" (version_num) VALUES ($1)',
                [revision]
            )
        else:
            await self.pool.execute(
                f'INSERT INTO "{table}" (version_num) VALUES (?)',
                [revision]
            )

    def load_migrations(self) -> list[MigrationScript]:
        """Load all migration scripts from the versions directory.

        Returns:
            List of migration scripts, sorted by dependency order
        """
        if not self.config:
            return []

        versions_dir = self.config.versions_dir
        if not versions_dir.exists():
            return []

        scripts = []
        for path in versions_dir.glob("*.py"):
            if path.name.startswith("_"):
                continue
            try:
                script = MigrationScript.load(path)
                scripts.append(script)
            except (ValueError, FileNotFoundError):
                # Skip invalid files
                pass

        # Sort by dependency order
        return self._sort_migrations(scripts)

    def _sort_migrations(self, scripts: list[MigrationScript]) -> list[MigrationScript]:
        """Sort migrations by dependency order.

        Args:
            scripts: Unsorted migration scripts

        Returns:
            Scripts sorted so each migration comes after its dependency
        """
        # Build revision -> script mapping
        by_revision: dict[str, MigrationScript] = {s.revision: s for s in scripts}

        # Topological sort
        sorted_scripts: list[MigrationScript] = []
        visited: set[str] = set()

        def visit(script: MigrationScript) -> None:
            if script.revision in visited:
                return
            visited.add(script.revision)

            # Visit dependency first
            if script.down_revision and script.down_revision in by_revision:
                visit(by_revision[script.down_revision])

            sorted_scripts.append(script)

        for script in scripts:
            visit(script)

        return sorted_scripts

    async def get_pending_migrations(self) -> list[MigrationScript]:
        """Get migrations that haven't been applied yet.

        Returns:
            List of pending migrations in order
        """
        applied = set(await self.get_applied_revisions())
        all_migrations = self.load_migrations()
        return [m for m in all_migrations if m.revision not in applied]

    async def get_state(self) -> MigrationState:
        """Get the current migration state.

        Returns:
            MigrationState with current revision and pending count
        """
        current = await self.get_current_revision()
        applied = await self.get_applied_revisions()
        pending = await self.get_pending_migrations()

        return MigrationState(
            current_revision=current,
            pending_count=len(pending),
            applied_revisions=applied,
        )

    async def run_upgrade(self, script: MigrationScript) -> None:
        """Run a single migration's upgrade function.

        Args:
            script: Migration script to run
        """
        await self._apply_migration(script, direction="upgrade")

    async def run_downgrade(self, script: MigrationScript) -> None:
        """Run a single migration's downgrade function.

        Args:
            script: Migration script to run
        """
        await self._apply_migration(script, direction="downgrade")

    async def upgrade(self, target: str = "head") -> list[MigrationScript]:
        """Apply pending migrations.

        Args:
            target: Target revision ("head" for all, or specific revision)

        Returns:
            List of applied migrations
        """
        pending = await self.get_pending_migrations()
        if not pending:
            return []

        applied = []

        for script in pending:
            # Check if we've reached target
            if target != "head" and applied:
                # Stop if we've applied the target
                if any(s.revision.startswith(target) for s in applied):
                    break

            # Execute upgrade
            await self._apply_migration(script, direction="upgrade")
            applied.append(script)

            # Stop at specific target
            if target != "head" and script.revision.startswith(target):
                break

        return applied

    async def downgrade(self, target: str = "-1") -> list[MigrationScript]:
        """Rollback migrations.

        Args:
            target: Target revision:
                - "-1" for last migration
                - "-N" for last N migrations
                - Specific revision ID to rollback to

        Returns:
            List of rolled back migrations
        """
        applied = await self.get_applied_revisions()
        if not applied:
            return []

        all_migrations = self.load_migrations()
        by_revision = {m.revision: m for m in all_migrations}

        # Determine how many to rollback
        if target.startswith("-"):
            try:
                count = int(target)
                # count is negative, make positive
                count = abs(count)
            except ValueError:
                count = 1
        else:
            # Rollback until we reach target
            count = len(applied)
            for i, rev in enumerate(reversed(applied)):
                if rev.startswith(target):
                    count = i
                    break

        rolled_back = []

        # Get migrations to rollback in reverse order
        applied_scripts = [by_revision[r] for r in applied if r in by_revision]
        applied_scripts.reverse()

        for script in applied_scripts[:count]:
            await self._apply_migration(script, direction="downgrade")
            rolled_back.append(script)

        return rolled_back

    async def _apply_migration(
        self,
        script: MigrationScript,
        direction: str,
    ) -> None:
        """Apply a single migration.

        Args:
            script: Migration script to apply
            direction: "upgrade" or "downgrade"
        """
        # Ensure version table exists
        await self.ensure_version_table()

        # Create operations context
        op = Operations(dialect=self._dialect)

        # Execute the migration function
        if direction == "upgrade":
            script.upgrade(op)
        else:
            script.downgrade(op)

        # Get SQL statements
        sql_statements = op.get_sql()

        # Execute all statements
        for sql in sql_statements:
            if sql:  # Skip empty strings
                await self.pool.execute(sql)

        # Update version table
        table = self._version_table
        if direction == "upgrade":
            # Add this version
            if self._dialect == "postgresql":
                await self.pool.execute(
                    f'INSERT INTO "{table}" (version_num) VALUES ($1)',
                    [script.revision]
                )
            else:
                await self.pool.execute(
                    f'INSERT INTO "{table}" (version_num) VALUES (?)',
                    [script.revision]
                )
        else:
            # Remove this version
            if self._dialect == "postgresql":
                await self.pool.execute(
                    f'DELETE FROM "{table}" WHERE version_num = $1',
                    [script.revision]
                )
            else:
                await self.pool.execute(
                    f'DELETE FROM "{table}" WHERE version_num = ?',
                    [script.revision]
                )

    async def create_migration(
        self,
        message: str,
        empty: bool = False,
    ) -> MigrationScript:
        """Create a new migration file.

        Args:
            message: Migration description
            empty: If True, create empty migration (no auto-generation)

        Returns:
            Created migration script

        Raises:
            ValueError: If no config is set
        """
        if not self.config:
            raise ValueError("Config required to create migrations")

        from ormkit.migrations.script import generate_revision_id, slugify

        # Get current head revision
        all_migrations = self.load_migrations()
        down_revision = all_migrations[-1].revision if all_migrations else None

        # Generate new revision
        revision = generate_revision_id()
        now = datetime.now()

        # Create script
        script = MigrationScript(
            revision=revision,
            down_revision=down_revision,
            message=message,
            create_date=now,
        )

        # Generate filename
        slug = slugify(message, self.config.truncate_slug_length)
        filename = self.config.format_filename(
            revision=revision,
            slug=slug,
            year=now.year,
            month=now.month,
            day=now.day,
            hour=now.hour,
            minute=now.minute,
        )

        # Write file
        versions_dir = self.config.versions_dir
        versions_dir.mkdir(parents=True, exist_ok=True)

        path = versions_dir / f"{filename}.py"
        path.write_text(script.render())
        script.path = path

        return script
