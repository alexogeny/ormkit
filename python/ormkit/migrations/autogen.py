"""Auto-generation of migrations from model diffs.

This module compares OrmKit models against the current database schema
and generates migration operations to synchronize them.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING

from ormkit.migrations.operations import (
    AddColumn,
    AlterColumn,
    Column,
    CreateForeignKey,
    CreateIndex,
    CreateTable,
    DropColumn,
    DropConstraint,
    DropIndex,
    DropTable,
    Operation,
)
from ormkit.migrations.script import generate_revision_id

if TYPE_CHECKING:
    from ormkit._ormkit import ConnectionPool
    from ormkit.base import Base


# Mapping from Python types to SQL types
PYTHON_TO_SQL = {
    int: {"postgresql": "INTEGER", "sqlite": "INTEGER"},
    str: {"postgresql": "TEXT", "sqlite": "TEXT"},
    float: {"postgresql": "DOUBLE PRECISION", "sqlite": "REAL"},
    bool: {"postgresql": "BOOLEAN", "sqlite": "INTEGER"},
    bytes: {"postgresql": "BYTEA", "sqlite": "BLOB"},
    dict: {"postgresql": "JSONB", "sqlite": "TEXT"},
    list: {"postgresql": "JSONB", "sqlite": "TEXT"},
}


@dataclass
class TableSchema:
    """Schema representation of a database table."""

    name: str
    columns: dict[str, ColumnSchema] = field(default_factory=dict)
    indexes: dict[str, IndexSchema] = field(default_factory=dict)
    constraints: dict[str, ConstraintSchema] = field(default_factory=dict)


@dataclass
class ColumnSchema:
    """Schema representation of a column."""

    name: str
    data_type: str
    nullable: bool = True
    default: str | None = None
    is_primary_key: bool = False


@dataclass
class IndexSchema:
    """Schema representation of an index."""

    name: str
    columns: list[str]
    unique: bool = False


@dataclass
class ConstraintSchema:
    """Schema representation of a constraint."""

    name: str
    constraint_type: str
    columns: list[str]
    references_table: str | None = None
    references_column: str | None = None


class AutogenContext:
    """Context for auto-generating migrations from model diffs.

    This compares registered OrmKit models against the current database
    schema and generates operations to synchronize them.

    Example:
        context = AutogenContext(pool, [User, Post])
        operations = await context.diff()
        source = context.render_migration("add_posts_table", operations)
    """

    def __init__(
        self,
        pool: ConnectionPool,
        models: list[type[Base]],
    ) -> None:
        """Initialize the autogen context.

        Args:
            pool: Database connection pool
            models: List of OrmKit model classes
        """
        self.pool = pool
        self.models = models
        self._dialect = "postgresql" if pool.is_postgres() else "sqlite"

    async def get_database_schema(self) -> dict[str, TableSchema]:
        """Get the current database schema.

        Returns:
            Dict mapping table names to TableSchema
        """
        tables = await self.pool.get_tables()
        schema: dict[str, TableSchema] = {}

        for table_name in tables:
            # Get columns
            columns_info = await self.pool.get_columns(table_name)
            columns = {}
            for col in columns_info:
                columns[col.name] = ColumnSchema(
                    name=col.name,
                    data_type=col.data_type,
                    nullable=col.nullable,
                    default=col.default,
                    is_primary_key=col.is_primary_key,
                )

            # Get indexes
            indexes_info = await self.pool.get_indexes(table_name)
            indexes = {}
            for idx in indexes_info:
                indexes[idx.name] = IndexSchema(
                    name=idx.name,
                    columns=idx.columns,
                    unique=idx.unique,
                )

            # Get constraints
            constraints_info = await self.pool.get_constraints(table_name)
            constraints = {}
            for con in constraints_info:
                constraints[con.name] = ConstraintSchema(
                    name=con.name,
                    constraint_type=con.constraint_type,
                    columns=con.columns,
                    references_table=con.references_table,
                    references_column=con.references_column,
                )

            schema[table_name] = TableSchema(
                name=table_name,
                columns=columns,
                indexes=indexes,
                constraints=constraints,
            )

        return schema

    def get_model_schema(self) -> dict[str, TableSchema]:
        """Get schema from OrmKit models.

        Returns:
            Dict mapping table names to TableSchema
        """
        schema: dict[str, TableSchema] = {}

        for model in self.models:
            table_name = model.__tablename__
            columns = {}

            for col_name, col_info in model.__columns__.items():
                # Get SQL type
                sql_type = col_info.sql_type(self._dialect)

                columns[col_name] = ColumnSchema(
                    name=col_name,
                    data_type=sql_type,
                    nullable=col_info.nullable,
                    default=str(col_info.default) if col_info.default is not None else None,
                    is_primary_key=col_info.primary_key,
                )

            # Extract indexes from model columns
            indexes: dict[str, IndexSchema] = {}
            for col_name, col_info in model.__columns__.items():
                # Single-column index
                if col_info.index:
                    index_name = f"ix_{table_name}_{col_name}"
                    indexes[index_name] = IndexSchema(
                        name=index_name,
                        columns=[col_name],
                        unique=False,
                    )
                # Unique constraint creates an implicit index
                if col_info.unique:
                    index_name = f"uq_{table_name}_{col_name}"
                    indexes[index_name] = IndexSchema(
                        name=index_name,
                        columns=[col_name],
                        unique=True,
                    )

            schema[table_name] = TableSchema(
                name=table_name,
                columns=columns,
                indexes=indexes,
            )

        return schema

    async def diff(self) -> list[Operation]:
        """Compare models to database and return operations.

        Returns:
            List of operations to synchronize database with models
        """
        db_schema = await self.get_database_schema()
        model_schema = self.get_model_schema()

        operations: list[Operation] = []

        # Find new tables (in models but not in DB)
        for table_name, table in model_schema.items():
            if table_name not in db_schema:
                # Create table
                columns = []
                for col_name, col in table.columns.items():
                    columns.append(
                        Column(
                            name=col.name,
                            type_=col.data_type,
                            nullable=col.nullable,
                            primary_key=col.is_primary_key,
                            default=col.default,
                        )
                    )
                operations.append(CreateTable(table_name, columns))

        # Find dropped tables (in DB but not in models)
        model_table_names = set(model_schema.keys())
        for table_name in db_schema:
            if table_name not in model_table_names:
                # Don't auto-drop tables - this is dangerous
                # User must explicitly drop
                pass

        # Find column changes in existing tables
        for table_name, model_table in model_schema.items():
            if table_name not in db_schema:
                continue  # Already handled as new table

            db_table = db_schema[table_name]

            # New columns
            for col_name, model_col in model_table.columns.items():
                if col_name not in db_table.columns:
                    operations.append(
                        AddColumn(
                            table_name,
                            Column(
                                name=model_col.name,
                                type_=model_col.data_type,
                                nullable=model_col.nullable,
                                primary_key=model_col.is_primary_key,
                                default=model_col.default,
                            ),
                        )
                    )

            # Dropped columns
            for col_name in db_table.columns:
                if col_name not in model_table.columns:
                    # Generate drop column operation
                    # Note: This can be dangerous - review carefully before applying
                    operations.append(DropColumn(table_name, col_name))

            # Changed columns
            for col_name, model_col in model_table.columns.items():
                if col_name not in db_table.columns:
                    continue  # Already handled as new column

                db_col = db_table.columns[col_name]

                # Check for type changes
                type_changed = not self._types_match(model_col.data_type, db_col.data_type)
                nullable_changed = model_col.nullable != db_col.nullable

                if type_changed or nullable_changed:
                    operations.append(
                        AlterColumn(
                            table_name,
                            col_name,
                            type_=model_col.data_type if type_changed else None,
                            nullable=model_col.nullable if nullable_changed else None,
                            existing_type=db_col.data_type,
                            existing_nullable=db_col.nullable,
                        )
                    )

            # Index changes
            db_indexes = db_table.indexes
            model_indexes = model_table.indexes

            # New indexes (in model but not in DB)
            for index_name, model_idx in model_indexes.items():
                # Check if an equivalent index exists (same columns)
                found = False
                for _db_idx_name, db_idx in db_indexes.items():
                    if set(db_idx.columns) == set(model_idx.columns):
                        found = True
                        break
                if not found:
                    operations.append(
                        CreateIndex(
                            index_name=index_name,
                            table_name=table_name,
                            columns=model_idx.columns,
                            unique=model_idx.unique,
                        )
                    )

        return operations

    def _types_match(self, model_type: str, db_type: str) -> bool:
        """Check if model and DB types are compatible.

        Args:
            model_type: Type from model
            db_type: Type from database

        Returns:
            True if types are compatible
        """
        # Normalize types for comparison
        model_upper = model_type.upper()
        db_upper = db_type.upper()

        # Direct match
        if model_upper == db_upper:
            return True

        # Common equivalences
        equivalences = [
            {"INTEGER", "INT", "INT4", "SERIAL"},
            {"BIGINT", "INT8", "BIGSERIAL"},
            {"TEXT", "VARCHAR", "CHARACTER VARYING"},
            {"DOUBLE PRECISION", "FLOAT8", "REAL", "FLOAT"},
            {"BOOLEAN", "BOOL"},
        ]

        for equiv_set in equivalences:
            # Check if both types are in the same equivalence set
            model_in = any(t in model_upper for t in equiv_set)
            db_in = any(t in db_upper for t in equiv_set)
            if model_in and db_in:
                return True

        return False

    def render_migration(
        self,
        message: str,
        operations: list[Operation],
        down_revision: str | None = None,
    ) -> str:
        """Render operations as Alembic migration file content.

        Args:
            message: Migration description
            operations: Operations to include
            down_revision: Previous revision ID

        Returns:
            Python source code for migration file
        """
        revision = generate_revision_id()
        now = datetime.now()
        date_str = now.strftime("%Y-%m-%d %H:%M:%S.%f")

        # Format revision values
        down_rev = f"'{down_revision}'" if down_revision else "None"

        # Generate upgrade operations
        upgrade_lines = []
        for op in operations:
            upgrade_lines.extend(self._render_operation(op))

        # Generate downgrade operations (reverse)
        downgrade_lines = []
        for op in reversed(operations):
            rev = op.reverse()
            if rev:
                downgrade_lines.extend(self._render_operation(rev))

        upgrade_code = "\n    ".join(upgrade_lines) if upgrade_lines else "pass"
        downgrade_code = "\n    ".join(downgrade_lines) if downgrade_lines else "pass"

        return f'''"""{message}

Revision ID: {revision}
Revises: {down_revision or 'None'}
Create Date: {date_str}
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '{revision}'
down_revision = {down_rev}
branch_labels = None
depends_on = None


def upgrade():
    {upgrade_code}


def downgrade():
    {downgrade_code}
'''

    def _render_operation(self, op: Operation) -> list[str]:
        """Render a single operation as Python code.

        Args:
            op: Operation to render

        Returns:
            List of Python code lines
        """
        if isinstance(op, CreateTable):
            cols = []
            for col in op.columns:
                col_args = [f"'{col.name}'", f"sa.{self._sa_type(col.type_)}()"]
                if col.primary_key:
                    col_args.append("primary_key=True")
                if not col.nullable and not col.primary_key:
                    col_args.append("nullable=False")
                cols.append(f"        sa.Column({', '.join(col_args)}),")

            return [
                "op.create_table(",
                f"    '{op.table_name}',",
                *cols,
                ")"
            ]

        elif isinstance(op, DropTable):
            return [f"op.drop_table('{op.table_name}')"]

        elif isinstance(op, AddColumn):
            col = op.column
            col_args = [f"sa.{self._sa_type(col.type_)}()"]
            if not col.nullable:
                col_args.append("nullable=False")
            return [
                f"op.add_column('{op.table_name}', sa.Column('{col.name}', {', '.join(col_args)}))"
            ]

        elif isinstance(op, DropColumn):
            return [f"op.drop_column('{op.table_name}', '{op.column_name}')"]

        elif isinstance(op, AlterColumn):
            args = [f"'{op.table_name}'", f"'{op.column_name}'"]
            if op.type_:
                args.append(f"type_=sa.{self._sa_type(op.type_)}()")
            if op.nullable is not None:
                args.append(f"nullable={op.nullable}")
            if op.existing_type:
                args.append(f"existing_type=sa.{self._sa_type(op.existing_type)}()")
            if op.existing_nullable is not None:
                args.append(f"existing_nullable={op.existing_nullable}")
            return [f"op.alter_column({', '.join(args)})"]

        elif isinstance(op, CreateIndex):
            cols = ", ".join(f"'{c}'" for c in op.columns)
            unique = ", unique=True" if op.unique else ""
            return [f"op.create_index('{op.index_name}', '{op.table_name}', [{cols}]{unique})"]

        elif isinstance(op, DropIndex):
            return [f"op.drop_index('{op.index_name}', table_name='{op.table_name}')"]

        elif isinstance(op, CreateForeignKey):
            src_cols = ", ".join(f"'{c}'" for c in op.source_columns)
            ref_cols = ", ".join(f"'{c}'" for c in op.referent_columns)
            return [
                f"op.create_foreign_key('{op.constraint_name}', '{op.source_table}', "
                f"'{op.referent_table}', [{src_cols}], [{ref_cols}])"
            ]

        elif isinstance(op, DropConstraint):
            return [f"op.drop_constraint('{op.constraint_name}', '{op.table_name}')"]

        return []

    def _sa_type(self, type_str: str) -> str:
        """Convert SQL type to SQLAlchemy type name.

        Args:
            type_str: SQL type string

        Returns:
            SQLAlchemy type name
        """
        type_upper = type_str.upper()

        mappings = {
            "INTEGER": "Integer",
            "INT": "Integer",
            "SERIAL": "Integer",
            "BIGINT": "BigInteger",
            "BIGSERIAL": "BigInteger",
            "TEXT": "Text",
            "VARCHAR": "String",
            "CHARACTER VARYING": "String",
            "BOOLEAN": "Boolean",
            "BOOL": "Boolean",
            "DOUBLE PRECISION": "Float",
            "FLOAT": "Float",
            "REAL": "Float",
            "BYTEA": "LargeBinary",
            "BLOB": "LargeBinary",
            "JSONB": "JSON",
            "JSON": "JSON",
            "TIMESTAMP": "DateTime",
            "TIMESTAMPTZ": "DateTime",
            "DATE": "Date",
            "TIME": "Time",
        }

        for sql_type, sa_type in mappings.items():
            if sql_type in type_upper:
                return sa_type

        # Default to Text for unknown types
        return "Text"


async def generate_migration(
    pool: ConnectionPool,
    models: list[type[Base]],
    message: str,
    down_revision: str | None = None,
) -> tuple[str, list[Operation]]:
    """Generate a migration from model diffs.

    Args:
        pool: Database connection pool
        models: List of OrmKit model classes
        message: Migration description
        down_revision: Previous revision ID

    Returns:
        Tuple of (migration source code, operations)
    """
    context = AutogenContext(pool, models)
    operations = await context.diff()
    source = context.render_migration(message, operations, down_revision)
    return source, operations
