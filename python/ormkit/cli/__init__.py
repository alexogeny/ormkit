"""OrmKit CLI - Command-line interface for migrations and database operations."""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ormkit.migrations.config import AlembicConfig


def main(args: list[str] | None = None) -> int:
    """Main entry point for the CLI.

    Args:
        args: Command-line arguments (defaults to sys.argv)

    Returns:
        Exit code (0 for success)
    """
    parser = argparse.ArgumentParser(
        prog="ormkit",
        description="OrmKit - A blazingly fast Python ORM powered by Rust",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # migrate subcommand
    migrate_parser = subparsers.add_parser("migrate", help="Database migration commands")
    migrate_subparsers = migrate_parser.add_subparsers(dest="subcommand", help="Migration commands")

    # migrate init
    init_parser = migrate_subparsers.add_parser(
        "init", help="Initialize migration directory structure"
    )
    init_parser.add_argument(
        "-d", "--directory",
        default=".",
        help="Target directory (default: current directory)",
    )
    init_parser.add_argument(
        "--url",
        help="Database URL to configure",
    )

    # migrate create
    create_parser = migrate_subparsers.add_parser(
        "create", help="Create a new empty migration"
    )
    create_parser.add_argument("message", help="Migration description")
    create_parser.add_argument(
        "-c", "--config",
        help="Path to alembic.ini",
    )

    # migrate auto
    auto_parser = migrate_subparsers.add_parser(
        "auto", help="Auto-generate migration from model changes"
    )
    auto_parser.add_argument("message", help="Migration description")
    auto_parser.add_argument(
        "-c", "--config",
        help="Path to alembic.ini",
    )
    auto_parser.add_argument(
        "--url",
        help="Database URL (overrides config)",
    )
    auto_parser.add_argument(
        "-m", "--models",
        help="Python module containing models (e.g., 'app.models')",
    )

    # migrate up
    up_parser = migrate_subparsers.add_parser(
        "up", help="Apply pending migrations"
    )
    up_parser.add_argument(
        "target",
        nargs="?",
        default="head",
        help="Target revision (default: head)",
    )
    up_parser.add_argument(
        "-c", "--config",
        help="Path to alembic.ini",
    )
    up_parser.add_argument(
        "--url",
        help="Database URL (overrides config)",
    )

    # migrate down
    down_parser = migrate_subparsers.add_parser(
        "down", help="Rollback migrations"
    )
    down_parser.add_argument(
        "target",
        nargs="?",
        default="-1",
        help="Target revision or -N for last N migrations (default: -1)",
    )
    down_parser.add_argument(
        "-c", "--config",
        help="Path to alembic.ini",
    )
    down_parser.add_argument(
        "--url",
        help="Database URL (overrides config)",
    )

    # migrate status
    status_parser = migrate_subparsers.add_parser(
        "status", help="Show current migration status"
    )
    status_parser.add_argument(
        "-c", "--config",
        help="Path to alembic.ini",
    )
    status_parser.add_argument(
        "--url",
        help="Database URL (overrides config)",
    )

    # migrate history
    history_parser = migrate_subparsers.add_parser(
        "history", help="Show migration history"
    )
    history_parser.add_argument(
        "-c", "--config",
        help="Path to alembic.ini",
    )
    history_parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Show detailed information",
    )

    # Parse arguments
    parsed = parser.parse_args(args)

    if parsed.command is None:
        parser.print_help()
        return 1

    if parsed.command == "migrate":
        return asyncio.run(_handle_migrate(parsed))

    return 0


async def _handle_migrate(args: Any) -> int:
    """Handle migrate subcommands.

    Args:
        args: Parsed arguments

    Returns:
        Exit code
    """
    from ormkit.migrations.config import AlembicConfig, create_default_config

    if args.subcommand == "init":
        return _migrate_init(args)

    elif args.subcommand == "create":
        return await _migrate_create(args)

    elif args.subcommand == "auto":
        return await _migrate_auto(args)

    elif args.subcommand == "up":
        return await _migrate_up(args)

    elif args.subcommand == "down":
        return await _migrate_down(args)

    elif args.subcommand == "status":
        return await _migrate_status(args)

    elif args.subcommand == "history":
        return _migrate_history(args)

    else:
        print("Unknown migrate subcommand. Use --help for usage.")
        return 1


def _migrate_init(args: Any) -> int:
    """Initialize migration directory structure."""
    from ormkit.migrations.config import create_default_config

    directory = Path(args.directory).resolve()
    ini_path, alembic_dir = create_default_config(directory, args.url)

    print("Created migration structure:")
    print(f"  {ini_path}")
    print(f"  {alembic_dir}/")
    print(f"  {alembic_dir}/versions/")
    print(f"  {alembic_dir}/env.py")
    print("\nEdit alembic.ini to configure your database URL.")
    return 0


async def _migrate_create(args: Any) -> int:
    """Create a new empty migration."""
    from ormkit import create_engine
    from ormkit.migrations.config import AlembicConfig
    from ormkit.migrations.runner import MigrationRunner

    # Load config
    config = _load_config(args)
    if config is None:
        return 1

    # We need a pool for the runner, but for empty migrations we don't need real DB
    # Use the configured URL if available
    url = getattr(args, "url", None) or config.sqlalchemy_url
    if not url:
        # For empty migrations, we can use a dummy SQLite
        url = "sqlite::memory:"

    pool = await create_engine(url)
    try:
        runner = MigrationRunner(pool, config)
        script = await runner.create_migration(args.message, empty=True)
        print(f"Created migration: {script.path}")
        print(f"  Revision: {script.revision}")
        print(f"  Down revision: {script.down_revision or 'None'}")
    finally:
        await pool.close()

    return 0


async def _migrate_auto(args: Any) -> int:
    """Auto-generate migration from model changes."""
    from ormkit import create_engine
    from ormkit.migrations.autogen import AutogenContext
    from ormkit.migrations.config import AlembicConfig
    from ormkit.migrations.runner import MigrationRunner

    # Load config
    config = _load_config(args)
    if config is None:
        return 1

    # Get database URL
    url = getattr(args, "url", None) or config.sqlalchemy_url
    if not url:
        print("Error: No database URL configured. Use --url or set sqlalchemy.url in alembic.ini")
        return 1

    # Load models
    models = _load_models(args.models)
    if not models:
        print("Error: No models found. Use --models to specify the models module.")
        return 1

    pool = await create_engine(url)
    try:
        runner = MigrationRunner(pool, config)

        # Get current head
        all_migrations = runner.load_migrations()
        down_revision = all_migrations[-1].revision if all_migrations else None

        # Generate migration
        context = AutogenContext(pool, models)
        operations = await context.diff()

        if not operations:
            print("No changes detected.")
            return 0

        # Render migration
        source = context.render_migration(args.message, operations, down_revision)

        # Write file
        from datetime import datetime

        from ormkit.migrations.script import generate_revision_id, slugify

        revision = generate_revision_id()
        now = datetime.now()
        slug = slugify(args.message, config.truncate_slug_length)
        filename = config.format_filename(
            revision=revision,
            slug=slug,
            year=now.year,
            month=now.month,
            day=now.day,
            hour=now.hour,
            minute=now.minute,
        )

        versions_dir = config.versions_dir
        versions_dir.mkdir(parents=True, exist_ok=True)
        path = versions_dir / f"{filename}.py"
        path.write_text(source)

        print(f"Created migration: {path}")
        print(f"  {len(operations)} operation(s)")
        for op in operations:
            print(f"    - {type(op).__name__}")

    finally:
        await pool.close()

    return 0


async def _migrate_up(args: Any) -> int:
    """Apply pending migrations."""
    from ormkit import create_engine
    from ormkit.migrations.config import AlembicConfig
    from ormkit.migrations.runner import MigrationRunner

    # Load config
    config = _load_config(args)
    if config is None:
        return 1

    # Get database URL
    url = getattr(args, "url", None) or config.sqlalchemy_url
    if not url:
        print("Error: No database URL configured. Use --url or set sqlalchemy.url in alembic.ini")
        return 1

    pool = await create_engine(url)
    try:
        runner = MigrationRunner(pool, config)
        applied = await runner.upgrade(args.target)

        if not applied:
            print("No pending migrations.")
        else:
            print(f"Applied {len(applied)} migration(s):")
            for script in applied:
                print(f"  -> {script.short_revision}: {script.message}")

    finally:
        await pool.close()

    return 0


async def _migrate_down(args: Any) -> int:
    """Rollback migrations."""
    from ormkit import create_engine
    from ormkit.migrations.config import AlembicConfig
    from ormkit.migrations.runner import MigrationRunner

    # Load config
    config = _load_config(args)
    if config is None:
        return 1

    # Get database URL
    url = getattr(args, "url", None) or config.sqlalchemy_url
    if not url:
        print("Error: No database URL configured. Use --url or set sqlalchemy.url in alembic.ini")
        return 1

    pool = await create_engine(url)
    try:
        runner = MigrationRunner(pool, config)
        rolled_back = await runner.downgrade(args.target)

        if not rolled_back:
            print("No migrations to rollback.")
        else:
            print(f"Rolled back {len(rolled_back)} migration(s):")
            for script in rolled_back:
                print(f"  <- {script.short_revision}: {script.message}")

    finally:
        await pool.close()

    return 0


async def _migrate_status(args: Any) -> int:
    """Show current migration status."""
    from ormkit import create_engine
    from ormkit.migrations.config import AlembicConfig
    from ormkit.migrations.runner import MigrationRunner

    # Load config
    config = _load_config(args)
    if config is None:
        return 1

    # Get database URL
    url = getattr(args, "url", None) or config.sqlalchemy_url
    if not url:
        print("Error: No database URL configured. Use --url or set sqlalchemy.url in alembic.ini")
        return 1

    pool = await create_engine(url)
    try:
        runner = MigrationRunner(pool, config)
        state = await runner.get_state()

        print("Migration status:")
        print(f"  Current revision: {state.current_revision or '(none)'}")
        print(f"  Pending migrations: {state.pending_count}")

        if state.pending_count > 0:
            pending = await runner.get_pending_migrations()
            print("\nPending:")
            for script in pending:
                print(f"  - {script.short_revision}: {script.message}")

    finally:
        await pool.close()

    return 0


def _migrate_history(args: Any) -> int:
    """Show migration history."""
    from ormkit.migrations.config import AlembicConfig
    from ormkit.migrations.runner import MigrationRunner

    # Load config
    config = _load_config(args)
    if config is None:
        return 1

    # Load migrations (doesn't require DB connection)
    from ormkit.migrations.script import MigrationScript

    versions_dir = config.versions_dir
    if not versions_dir.exists():
        print("No migrations directory found.")
        return 0

    scripts = []
    for path in versions_dir.glob("*.py"):
        if path.name.startswith("_"):
            continue
        try:
            script = MigrationScript.load(path)
            scripts.append(script)
        except Exception:
            pass

    if not scripts:
        print("No migrations found.")
        return 0

    # Sort by path name (timestamp-based)
    scripts.sort(key=lambda s: s.path.name if s.path else "")

    print(f"Migration history ({len(scripts)} migrations):")
    for script in scripts:
        rev = script.short_revision
        down = script.down_revision[:8] if script.down_revision else "(base)"
        print(f"  {rev} <- {down}: {script.message}")

        if args.verbose and script.path:
            print(f"       File: {script.path.name}")

    return 0


def _load_config(args: Any) -> "AlembicConfig | None":
    """Load Alembic config from args or auto-detect."""
    from ormkit.migrations.config import AlembicConfig

    if hasattr(args, "config") and args.config:
        config_path = Path(args.config)
    else:
        config_path = Path.cwd() / "alembic.ini"

    if config_path.exists():
        return AlembicConfig.from_ini(config_path)

    # Try auto-detection
    config = AlembicConfig.auto_detect()
    if config:
        return config

    print(f"Error: Config file not found: {config_path}")
    print("Run 'ormkit migrate init' to create one.")
    return None


def _load_models(models_path: str | None) -> list[type]:
    """Load models from a Python module path.

    Args:
        models_path: Module path like 'app.models'

    Returns:
        List of model classes
    """
    if not models_path:
        return []

    import importlib

    try:
        module = importlib.import_module(models_path)
    except ImportError as e:
        print(f"Error importing models: {e}")
        return []

    # Find all Base subclasses
    from ormkit.base import Base

    models = []
    for name in dir(module):
        obj = getattr(module, name)
        if (
            isinstance(obj, type)
            and issubclass(obj, Base)
            and obj is not Base
            and hasattr(obj, "__tablename__")
        ):
            models.append(obj)

    return models


# =============================================================================
# Standalone functions for programmatic use (also used by tests)
# =============================================================================


def migrate_init(directory: Path | str, url: str | None = None) -> None:
    """Initialize migration directory structure.

    Args:
        directory: Target directory
        url: Optional database URL to configure
    """
    from ormkit.migrations.config import create_default_config

    directory = Path(directory)
    create_default_config(directory, url)


def migrate_create(directory: Path | str, message: str) -> Path:
    """Create a new empty migration.

    Args:
        directory: Directory containing alembic.ini
        message: Migration description

    Returns:
        Path to created migration file
    """
    from datetime import datetime

    from ormkit.migrations.config import AlembicConfig
    from ormkit.migrations.script import MigrationScript, generate_revision_id, slugify

    directory = Path(directory)
    config = AlembicConfig.from_ini(directory / "alembic.ini")

    # Load existing migrations to get head revision (no db connection needed)
    versions_dir = config.versions_dir
    versions_dir.mkdir(parents=True, exist_ok=True)

    existing_migrations: list[MigrationScript] = []
    for path in versions_dir.glob("*.py"):
        if path.name.startswith("_"):
            continue
        try:
            script = MigrationScript.load(path)
            existing_migrations.append(script)
        except (ValueError, FileNotFoundError):
            pass

    # Sort by dependency order to get head
    down_revision: str | None = None
    if existing_migrations:
        # Find the one that no one depends on
        all_revisions = {m.revision for m in existing_migrations}
        down_revisions = {m.down_revision for m in existing_migrations if m.down_revision}
        heads = all_revisions - down_revisions
        if heads:
            down_revision = heads.pop()

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
    slug = slugify(message, config.truncate_slug_length)
    filename = config.format_filename(
        revision=revision,
        slug=slug,
        year=now.year,
        month=now.month,
        day=now.day,
        hour=now.hour,
        minute=now.minute,
    )

    # Write file
    path = versions_dir / f"{filename}.py"
    path.write_text(script.render())
    script.path = path

    return path


async def migrate_up(
    pool: Any,
    directory: Path | str,
    target: str = "head",
) -> list[Any]:
    """Apply pending migrations.

    Args:
        pool: Database connection pool
        directory: Directory containing alembic.ini
        target: Target revision (default: head)

    Returns:
        List of applied migrations
    """
    from ormkit.migrations.config import AlembicConfig
    from ormkit.migrations.runner import MigrationRunner

    directory = Path(directory)
    config = AlembicConfig.from_ini(directory / "alembic.ini")
    runner = MigrationRunner(pool, config)
    return await runner.upgrade(target)


async def migrate_down(
    pool: Any,
    directory: Path | str,
    target: str = "-1",
) -> list[Any]:
    """Rollback migrations.

    Args:
        pool: Database connection pool
        directory: Directory containing alembic.ini
        target: Target revision (default: -1)

    Returns:
        List of rolled back migrations
    """
    from ormkit.migrations.config import AlembicConfig
    from ormkit.migrations.runner import MigrationRunner

    directory = Path(directory)
    config = AlembicConfig.from_ini(directory / "alembic.ini")
    runner = MigrationRunner(pool, config)
    return await runner.downgrade(target)


async def migrate_status(
    pool: Any,
    directory: Path | str,
) -> dict[str, Any]:
    """Get current migration status.

    Args:
        pool: Database connection pool
        directory: Directory containing alembic.ini

    Returns:
        Dict with current_revision and pending list
    """
    from ormkit.migrations.config import AlembicConfig
    from ormkit.migrations.runner import MigrationRunner

    directory = Path(directory)
    config = AlembicConfig.from_ini(directory / "alembic.ini")
    runner = MigrationRunner(pool, config)

    state = await runner.get_state()
    pending = await runner.get_pending_migrations()

    return {
        "current_revision": state.current_revision,
        "pending": pending,
    }


if __name__ == "__main__":
    sys.exit(main())
