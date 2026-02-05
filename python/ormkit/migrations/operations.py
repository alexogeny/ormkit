"""Migration operations - Alembic-compatible schema operations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class Operation(Protocol):
    """Protocol for migration operations."""

    def to_sql(self, dialect: str) -> list[str]:
        """Generate SQL statements for this operation."""
        ...

    def reverse(self) -> Operation | None:
        """Return the reverse operation, or None if not reversible."""
        ...


@dataclass
class ColumnDef:
    """Column definition for CreateTable and AddColumn operations."""

    name: str
    type_: str
    nullable: bool = True
    primary_key: bool = False
    unique: bool = False
    default: Any | None = None
    autoincrement: bool = False
    comment: str | None = None

    def to_sql(self, dialect: str) -> str:
        """Generate SQL column definition."""
        parts = [f"{self.name} {self.type_}"]

        if self.primary_key:
            if dialect == "sqlite" and self.autoincrement:
                parts.append("PRIMARY KEY AUTOINCREMENT")
            elif dialect == "postgresql" and self.autoincrement:
                # PostgreSQL uses SERIAL types for autoincrement
                if "INTEGER" in self.type_.upper():
                    parts[0] = f"{self.name} SERIAL"
                    parts.append("PRIMARY KEY")
                elif "BIGINT" in self.type_.upper():
                    parts[0] = f"{self.name} BIGSERIAL"
                    parts.append("PRIMARY KEY")
                else:
                    parts.append("PRIMARY KEY")
            else:
                parts.append("PRIMARY KEY")
        elif not self.nullable:
            parts.append("NOT NULL")

        if self.unique and not self.primary_key:
            parts.append("UNIQUE")

        if self.default is not None:
            if isinstance(self.default, str):
                parts.append(f"DEFAULT '{self.default}'")
            elif isinstance(self.default, bool):
                parts.append(f"DEFAULT {'TRUE' if self.default else 'FALSE'}")
            else:
                parts.append(f"DEFAULT {self.default}")

        return " ".join(parts)


# Alias for backwards compatibility
Column = ColumnDef


@dataclass
class CreateTable:
    """Create a new table."""

    table_name: str
    columns: list[ColumnDef]
    if_not_exists: bool = False

    @property
    def operation_type(self) -> str:
        """Return the operation type."""
        return "create_table"

    def to_sql(self, dialect: str) -> str:
        """Generate CREATE TABLE SQL."""
        exists_clause = "IF NOT EXISTS " if self.if_not_exists else ""
        col_defs = ", ".join(col.to_sql(dialect) for col in self.columns)
        return f"CREATE TABLE {exists_clause}{self.table_name} ({col_defs})"

    def reverse(self) -> DropTable:
        """Reverse is DROP TABLE."""
        return DropTable(self.table_name)


@dataclass
class DropTable:
    """Drop a table."""

    table_name: str
    if_exists: bool = False

    @property
    def operation_type(self) -> str:
        """Return the operation type."""
        return "drop_table"

    def to_sql(self, dialect: str) -> str:
        """Generate DROP TABLE SQL."""
        exists_clause = "IF EXISTS " if self.if_exists else ""
        return f"DROP TABLE {exists_clause}{self.table_name}"

    def reverse(self) -> None:
        """Cannot reverse DROP TABLE without schema info."""
        return None


@dataclass
class AddColumn:
    """Add a column to a table."""

    table_name: str
    column: ColumnDef

    @property
    def operation_type(self) -> str:
        """Return the operation type."""
        return "add_column"

    @property
    def column_name(self) -> str:
        """Return the column name for convenience."""
        return self.column.name

    def to_sql(self, dialect: str) -> str:
        """Generate ALTER TABLE ADD COLUMN SQL."""
        return f"ALTER TABLE {self.table_name} ADD COLUMN {self.column.to_sql(dialect)}"

    def reverse(self) -> DropColumn:
        """Reverse is DROP COLUMN."""
        return DropColumn(self.table_name, self.column.name)


@dataclass
class DropColumn:
    """Drop a column from a table."""

    table_name: str
    column_name: str

    @property
    def operation_type(self) -> str:
        """Return the operation type."""
        return "drop_column"

    def to_sql(self, dialect: str) -> str:
        """Generate ALTER TABLE DROP COLUMN SQL."""
        return f"ALTER TABLE {self.table_name} DROP COLUMN {self.column_name}"

    def reverse(self) -> None:
        """Cannot reverse DROP COLUMN without schema info."""
        return None


@dataclass
class AlterColumn:
    """Alter a column's properties."""

    table_name: str
    column_name: str
    type_: str | None = None
    nullable: bool | None = None
    default: Any | None = None
    new_name: str | None = None
    # Store original values for reverse
    existing_type: str | None = None
    existing_nullable: bool | None = None
    existing_default: Any | None = None

    @property
    def operation_type(self) -> str:
        """Return the operation type."""
        return "alter_column"

    def to_sql(self, dialect: str) -> str:
        """Generate ALTER TABLE ALTER COLUMN SQL."""
        statements = []
        table = self.table_name
        col = self.column_name

        if dialect == "postgresql":
            if self.type_ is not None:
                statements.append(
                    f"ALTER TABLE {table} ALTER COLUMN {col} TYPE {self.type_}"
                )
            if self.nullable is not None:
                if self.nullable:
                    statements.append(f"ALTER TABLE {table} ALTER COLUMN {col} DROP NOT NULL")
                else:
                    statements.append(f"ALTER TABLE {table} ALTER COLUMN {col} SET NOT NULL")
            if self.default is not None:
                if self.default == "DROP":
                    statements.append(f"ALTER TABLE {table} ALTER COLUMN {col} DROP DEFAULT")
                else:
                    statements.append(
                        f"ALTER TABLE {table} ALTER COLUMN {col} SET DEFAULT {self.default}"
                    )
            if self.new_name is not None:
                statements.append(
                    f"ALTER TABLE {table} RENAME COLUMN {col} TO {self.new_name}"
                )
        elif dialect == "sqlite":
            # SQLite has limited ALTER TABLE support
            # For type changes, we'd need to recreate the table
            if self.new_name is not None:
                statements.append(
                    f"ALTER TABLE {table} RENAME COLUMN {col} TO {self.new_name}"
                )
            # Other changes require table recreation (not implemented here)

        return "; ".join(statements) if statements else ""

    def reverse(self) -> AlterColumn | None:
        """Reverse the alteration if original values are known."""
        if self.existing_type is None and self.existing_nullable is None:
            return None

        return AlterColumn(
            table_name=self.table_name,
            column_name=self.new_name or self.column_name,
            type_=self.existing_type if self.type_ else None,
            nullable=self.existing_nullable if self.nullable is not None else None,
            default=self.existing_default if self.default else None,
            new_name=self.column_name if self.new_name else None,
        )


@dataclass
class CreateIndex:
    """Create an index on a table."""

    index_name: str
    table_name: str
    columns: list[str]
    unique: bool = False
    if_not_exists: bool = False

    @property
    def operation_type(self) -> str:
        """Return the operation type."""
        return "create_index"

    def to_sql(self, dialect: str) -> str:
        """Generate CREATE INDEX SQL."""
        unique = "UNIQUE " if self.unique else ""
        exists = "IF NOT EXISTS " if self.if_not_exists else ""
        cols = ", ".join(self.columns)
        return f"CREATE {unique}INDEX {exists}{self.index_name} ON {self.table_name} ({cols})"

    def reverse(self) -> DropIndex:
        """Reverse is DROP INDEX."""
        return DropIndex(self.index_name, self.table_name)


@dataclass
class DropIndex:
    """Drop an index."""

    index_name: str
    table_name: str | None = None  # For PostgreSQL compatibility
    if_exists: bool = False

    @property
    def operation_type(self) -> str:
        """Return the operation type."""
        return "drop_index"

    def to_sql(self, dialect: str) -> str:
        """Generate DROP INDEX SQL."""
        exists = "IF EXISTS " if self.if_exists else ""
        return f"DROP INDEX {exists}{self.index_name}"

    def reverse(self) -> None:
        """Cannot reverse DROP INDEX without column info."""
        return None


@dataclass
class CreateForeignKey:
    """Create a foreign key constraint."""

    constraint_name: str
    source_table: str
    source_columns: list[str]
    referent_table: str
    referent_columns: list[str]
    ondelete: str | None = None
    onupdate: str | None = None

    @property
    def operation_type(self) -> str:
        """Return the operation type."""
        return "create_foreign_key"

    def to_sql(self, dialect: str) -> str:
        """Generate ADD CONSTRAINT SQL."""
        src_cols = ", ".join(self.source_columns)
        ref_cols = ", ".join(self.referent_columns)

        sql = (
            f"ALTER TABLE {self.source_table} ADD CONSTRAINT {self.constraint_name} "
            f"FOREIGN KEY ({src_cols}) REFERENCES {self.referent_table} ({ref_cols})"
        )

        if self.ondelete:
            sql += f" ON DELETE {self.ondelete}"
        if self.onupdate:
            sql += f" ON UPDATE {self.onupdate}"

        return sql

    def reverse(self) -> DropConstraint:
        """Reverse is DROP CONSTRAINT."""
        return DropConstraint(self.constraint_name, self.source_table)


@dataclass
class DropConstraint:
    """Drop a constraint."""

    constraint_name: str
    table_name: str
    if_exists: bool = False

    @property
    def operation_type(self) -> str:
        """Return the operation type."""
        return "drop_constraint"

    def to_sql(self, dialect: str) -> str:
        """Generate DROP CONSTRAINT SQL."""
        exists = "IF EXISTS " if self.if_exists else ""
        return f"ALTER TABLE {self.table_name} DROP CONSTRAINT {exists}{self.constraint_name}"

    def reverse(self) -> None:
        """Cannot reverse DROP CONSTRAINT without original info."""
        return None


@dataclass
class Execute:
    """Execute raw SQL."""

    sql: str
    reverse_sql: str | None = None

    @property
    def operation_type(self) -> str:
        """Return the operation type."""
        return "execute"

    def to_sql(self, dialect: str) -> str:
        """Return the raw SQL."""
        return self.sql

    def reverse(self) -> Execute | None:
        """Return reverse SQL if provided."""
        if self.reverse_sql:
            return Execute(self.reverse_sql, self.sql)
        return None


class Operations:
    """Alembic-compatible operations context.

    This collects operations during upgrade/downgrade and can
    execute them against a database connection.

    Example:
        op = Operations(dialect="postgresql")
        op.create_table("users",
            Column("id", "INTEGER", primary_key=True),
            Column("name", "VARCHAR(100)", nullable=False),
        )
        for sql in op.get_sql():
            print(sql)
    """

    def __init__(self, dialect: str = "postgresql") -> None:
        self.dialect = dialect
        self._operations: list[Operation] = []

    def create_table(
        self,
        table_name: str,
        *columns: Column,
        if_not_exists: bool = False,
    ) -> None:
        """Create a new table."""
        self._operations.append(
            CreateTable(table_name, list(columns), if_not_exists)
        )

    def drop_table(self, table_name: str, if_exists: bool = False) -> None:
        """Drop a table."""
        self._operations.append(DropTable(table_name, if_exists))

    def add_column(self, table_name: str, column: Column) -> None:
        """Add a column to a table."""
        self._operations.append(AddColumn(table_name, column))

    def drop_column(self, table_name: str, column_name: str) -> None:
        """Drop a column from a table."""
        self._operations.append(DropColumn(table_name, column_name))

    def alter_column(
        self,
        table_name: str,
        column_name: str,
        type_: str | None = None,
        nullable: bool | None = None,
        default: Any | None = None,
        new_name: str | None = None,
        existing_type: str | None = None,
        existing_nullable: bool | None = None,
    ) -> None:
        """Alter a column's properties."""
        self._operations.append(
            AlterColumn(
                table_name,
                column_name,
                type_=type_,
                nullable=nullable,
                default=default,
                new_name=new_name,
                existing_type=existing_type,
                existing_nullable=existing_nullable,
            )
        )

    def create_index(
        self,
        index_name: str,
        table_name: str,
        columns: list[str],
        unique: bool = False,
        if_not_exists: bool = False,
    ) -> None:
        """Create an index."""
        self._operations.append(
            CreateIndex(index_name, table_name, columns, unique, if_not_exists)
        )

    def drop_index(
        self,
        index_name: str,
        table_name: str | None = None,
        if_exists: bool = False,
    ) -> None:
        """Drop an index."""
        self._operations.append(DropIndex(index_name, table_name, if_exists))

    def create_foreign_key(
        self,
        constraint_name: str,
        source_table: str,
        source_columns: list[str],
        referent_table: str,
        referent_columns: list[str],
        ondelete: str | None = None,
        onupdate: str | None = None,
    ) -> None:
        """Create a foreign key constraint."""
        self._operations.append(
            CreateForeignKey(
                constraint_name,
                source_table,
                source_columns,
                referent_table,
                referent_columns,
                ondelete,
                onupdate,
            )
        )

    def drop_constraint(
        self,
        constraint_name: str,
        table_name: str,
        if_exists: bool = False,
    ) -> None:
        """Drop a constraint."""
        self._operations.append(DropConstraint(constraint_name, table_name, if_exists))

    def execute(self, sql: str, reverse_sql: str | None = None) -> None:
        """Execute raw SQL."""
        self._operations.append(Execute(sql, reverse_sql))

    def get_operations(self) -> list[Operation]:
        """Get all collected operations."""
        return self._operations

    def get_sql(self) -> list[str]:
        """Get all SQL statements."""
        sql = []
        for op in self._operations:
            result = op.to_sql(self.dialect)
            if result:  # Skip empty strings
                sql.append(result)
        return sql

    def get_reverse_operations(self) -> list[Operation]:
        """Get reverse operations for downgrade."""
        reverse = []
        for op in reversed(self._operations):
            rev_op = op.reverse()
            if rev_op is not None:
                reverse.append(rev_op)
        return reverse
