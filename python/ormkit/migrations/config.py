"""Alembic configuration parsing."""

from __future__ import annotations

import configparser
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class AlembicConfig:
    """Parse and represent alembic.ini configuration.

    This is compatible with Alembic's configuration format, allowing
    OrmKit to be a drop-in replacement for existing Alembic projects.

    Example alembic.ini:
        [alembic]
        script_location = alembic
        sqlalchemy.url = postgresql://localhost/mydb

        [logging]
        ...
    """

    script_location: Path
    """Directory containing migration scripts (alembic/versions/)."""

    sqlalchemy_url: str | None = None
    """Database connection URL from config (can be overridden)."""

    version_table: str = "alembic_version"
    """Table name for tracking applied migrations."""

    file_template: str = "%%(year)d%%(month).2d%%(day).2d_%%(hour).2d%%(minute).2d_%%(rev)s_%%(slug)s"
    """Template for migration filename generation."""

    revision_environment: bool = False
    """Whether to run env.py for every revision."""

    truncate_slug_length: int = 40
    """Max length for slug in filenames."""

    timezone: str | None = None
    """Timezone for timestamps (e.g., 'UTC')."""

    extra: dict[str, Any] = field(default_factory=dict)
    """Additional configuration options."""

    _config_path: Path | None = None
    """Path to the config file (internal)."""

    @classmethod
    def from_ini(cls, path: Path | str) -> AlembicConfig:
        """Load configuration from an alembic.ini file.

        Args:
            path: Path to alembic.ini

        Returns:
            Parsed AlembicConfig

        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If required options are missing
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")

        config = configparser.ConfigParser()
        config.read(path)

        if "alembic" not in config:
            raise ValueError(f"No [alembic] section in {path}")

        section = config["alembic"]

        # Required: script_location
        script_location = section.get("script_location")
        if not script_location:
            raise ValueError("script_location is required in alembic.ini")

        # Resolve relative to config file
        script_path = Path(script_location)
        if not script_path.is_absolute():
            script_path = path.parent / script_path

        # Optional settings
        sqlalchemy_url = section.get("sqlalchemy.url")
        version_table = section.get("version_table", "alembic_version")
        file_template = section.get(
            "file_template",
            "%%(year)d%%(month).2d%%(day).2d_%%(hour).2d%%(minute).2d_%%(rev)s_%%(slug)s",
        )
        revision_environment = section.getboolean("revision_environment", False)
        truncate_slug_length = section.getint("truncate_slug_length", 40)
        timezone = section.get("timezone")

        # Collect extra options
        known_keys = {
            "script_location",
            "sqlalchemy.url",
            "version_table",
            "file_template",
            "revision_environment",
            "truncate_slug_length",
            "timezone",
        }
        extra = {k: v for k, v in section.items() if k not in known_keys}

        return cls(
            script_location=script_path,
            sqlalchemy_url=sqlalchemy_url,
            version_table=version_table,
            file_template=file_template,
            revision_environment=revision_environment,
            truncate_slug_length=truncate_slug_length,
            timezone=timezone,
            extra=extra,
            _config_path=path,
        )

    @classmethod
    def auto_detect(cls, start_path: Path | str | None = None) -> AlembicConfig | None:
        """Auto-detect alembic.ini by searching up from start_path.

        Args:
            start_path: Directory to start searching from (default: cwd)

        Returns:
            AlembicConfig if found, None otherwise
        """
        start_path = Path.cwd() if start_path is None else Path(start_path)

        current = start_path
        while current != current.parent:
            ini_path = current / "alembic.ini"
            if ini_path.exists():
                return cls.from_ini(ini_path)
            current = current.parent

        return None

    @classmethod
    def detect(cls, start_path: Path | str | None = None) -> AlembicConfig | None:
        """Alias for auto_detect for backwards compatibility."""
        return cls.auto_detect(start_path)

    @property
    def versions_dir(self) -> Path:
        """Get the versions directory path."""
        return self.script_location / "versions"

    @property
    def env_py_path(self) -> Path:
        """Get the env.py file path."""
        return self.script_location / "env.py"

    def get_url(self, override: str | None = None) -> str:
        """Get database URL with optional override.

        Args:
            override: URL to use instead of config value

        Returns:
            Database URL

        Raises:
            ValueError: If no URL available
        """
        url = override or self.sqlalchemy_url
        if not url:
            raise ValueError("No database URL configured")
        return url

    def format_filename(
        self,
        revision: str,
        slug: str,
        year: int,
        month: int,
        day: int,
        hour: int,
        minute: int,
    ) -> str:
        """Format a migration filename using the template.

        Args:
            revision: Revision ID (e.g., "abc123def456")
            slug: Slugified message
            year, month, day, hour, minute: Timestamp components

        Returns:
            Formatted filename (without .py extension)
        """
        # Truncate slug
        if len(slug) > self.truncate_slug_length:
            slug = slug[: self.truncate_slug_length]

        # Format using %-style formatting (Alembic uses %% for literal %)
        template = self.file_template.replace("%%", "%")
        return template % {
            "rev": revision[:12],  # Short revision
            "slug": slug,
            "year": year,
            "month": month,
            "day": day,
            "hour": hour,
            "minute": minute,
        }


def create_default_config(
    directory: Path,
    url: str | None = None,
) -> tuple[Path, Path]:
    """Create default alembic.ini and directory structure.

    Args:
        directory: Root directory for migrations
        url: Optional database URL

    Returns:
        Tuple of (alembic.ini path, alembic dir path)
    """
    alembic_dir = directory / "alembic"
    versions_dir = alembic_dir / "versions"
    ini_path = directory / "alembic.ini"

    # Create directories
    alembic_dir.mkdir(parents=True, exist_ok=True)
    versions_dir.mkdir(exist_ok=True)

    # Create alembic.ini
    url_line = f"sqlalchemy.url = {url}" if url else "# sqlalchemy.url = driver://user:pass@localhost/dbname"
    ini_content = f"""# OrmKit Migration Configuration (Alembic-compatible)

[alembic]
script_location = alembic
{url_line}

# Template for migration filenames
file_template = %%(year)d%%(month).2d%%(day).2d_%%(hour).2d%%(minute).2d_%%(rev)s_%%(slug)s

# Version table name
version_table = alembic_version

# Truncate slug length
truncate_slug_length = 40

# Timezone for timestamps (optional, e.g., UTC)
# timezone =

[logging]
# Logging configuration (optional)
"""
    ini_path.write_text(ini_content)

    # Create env.py stub
    env_content = '''"""Alembic environment configuration.

This file is executed before migrations run.
OrmKit provides its own runner, but maintains Alembic compatibility.
"""

from __future__ import annotations

# OrmKit migrations don't require env.py customization by default.
# This file exists for Alembic compatibility.
#
# To customize migration behavior, you can define:
#
# def run_migrations_online():
#     """Run migrations in 'online' mode."""
#     pass
#
# def run_migrations_offline():
#     """Run migrations in 'offline' mode."""
#     pass
'''
    env_path = alembic_dir / "env.py"
    env_path.write_text(env_content)

    # Create README
    readme_content = """# Database Migrations

This directory contains database migration scripts managed by OrmKit.

## Directory Structure

- `versions/` - Individual migration files
- `env.py` - Environment configuration (Alembic compatibility)

## Commands

```bash
# Initialize migrations (already done)
ormkit migrate init

# Create a new migration
ormkit migrate create "description"

# Auto-generate from model changes
ormkit migrate auto "description"

# Apply all pending migrations
ormkit migrate up

# Rollback last migration
ormkit migrate down

# Show current status
ormkit migrate status

# Show migration history
ormkit migrate history
```

## Alembic Compatibility

These migrations are compatible with Alembic. You can use either:
- `ormkit migrate up` (OrmKit)
- `alembic upgrade head` (Alembic)
"""
    readme_path = alembic_dir / "README.md"
    readme_path.write_text(readme_content)

    return ini_path, alembic_dir
