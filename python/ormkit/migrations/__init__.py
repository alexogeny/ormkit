"""OrmKit Migrations - Alembic-compatible database migrations.

This module provides:
- Configuration parsing (alembic.ini)
- Migration script loading and execution
- Auto-generation from model diffs
- Schema introspection using Rust backend
"""

from __future__ import annotations

from ormkit.migrations.autogen import AutogenContext, generate_migration
from ormkit.migrations.config import AlembicConfig
from ormkit.migrations.operations import (
    AddColumn,
    AlterColumn,
    Column,
    ColumnDef,
    CreateForeignKey,
    CreateIndex,
    CreateTable,
    DropColumn,
    DropConstraint,
    DropIndex,
    DropTable,
    Execute,
    Operation,
    Operations,
)
from ormkit.migrations.runner import MigrationRunner
from ormkit.migrations.script import MigrationScript

__all__ = [
    # Config
    "AlembicConfig",
    # Scripts
    "MigrationScript",
    # Runner
    "MigrationRunner",
    # Operations
    "Operations",
    "Operation",
    "Column",
    "ColumnDef",
    "CreateTable",
    "DropTable",
    "AddColumn",
    "DropColumn",
    "AlterColumn",
    "CreateIndex",
    "DropIndex",
    "CreateForeignKey",
    "DropConstraint",
    "Execute",
    # Autogen
    "AutogenContext",
    "generate_migration",
]
