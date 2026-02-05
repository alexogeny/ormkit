"""Migration script loading and representation."""

from __future__ import annotations

import ast
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from ormkit.migrations.operations import Operations


@dataclass
class MigrationScript:
    """Represents an Alembic-compatible migration script.

    This class can:
    - Load existing Alembic migration files
    - Execute upgrade/downgrade operations
    - Generate new Alembic-compatible migration files

    Alembic migration format:
        revision = 'abc123def456'
        down_revision = 'xyz789...'
        branch_labels = None
        depends_on = None

        def upgrade():
            op.create_table(...)

        def downgrade():
            op.drop_table(...)
    """

    revision: str
    """Unique revision identifier."""

    down_revision: str | None = None
    """Previous revision this depends on (None for initial)."""

    message: str = ""
    """Migration description/message."""

    branch_labels: tuple[str, ...] | None = None
    """Branch labels for branch support."""

    depends_on: tuple[str, ...] | None = None
    """Other revisions this depends on."""

    create_date: datetime | None = None
    """When this migration was created."""

    path: Path | None = None
    """Path to the migration file."""

    # Upgrade/downgrade functions (populated when loaded)
    _upgrade_fn: Callable[[Operations], None] | None = None
    _downgrade_fn: Callable[[Operations], None] | None = None

    # Raw source for modifications
    _source: str | None = None

    @classmethod
    def load(cls, path: Path) -> MigrationScript:
        """Load a migration script from a Python file.

        This parses Alembic-format migration files and extracts:
        - revision, down_revision, branch_labels, depends_on
        - upgrade() and downgrade() functions
        - Creation date from docstring

        Args:
            path: Path to the migration .py file

        Returns:
            MigrationScript instance

        Raises:
            ValueError: If file is not a valid migration
        """
        if not path.exists():
            raise FileNotFoundError(f"Migration file not found: {path}")

        source = path.read_text()

        # Parse the AST to extract module-level variables
        try:
            tree = ast.parse(source)
        except SyntaxError as e:
            raise ValueError(f"Invalid Python in {path}: {e}") from e

        # Extract variables and functions
        revision = None
        down_revision = None
        branch_labels = None
        depends_on = None
        message = ""
        create_date = None

        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        name = target.id
                        value = _eval_ast_value(node.value)

                        if name == "revision":
                            revision = value
                        elif name == "down_revision":
                            down_revision = value
                        elif name == "branch_labels":
                            if value:
                                branch_labels = tuple(value) if isinstance(value, (list, tuple)) else (value,)
                        elif name == "depends_on":
                            if value:
                                depends_on = tuple(value) if isinstance(value, (list, tuple)) else (value,)

        # Extract docstring for message and date
        if tree.body and isinstance(tree.body[0], ast.Expr):
            if isinstance(tree.body[0].value, ast.Constant):
                docstring = tree.body[0].value.value
                if isinstance(docstring, str):
                    lines = docstring.strip().split("\n")
                    if lines:
                        message = lines[0].strip()
                    # Try to find creation date
                    for line in lines:
                        if "Create Date:" in line:
                            date_str = line.split("Create Date:", 1)[1].strip()
                            try:
                                create_date = datetime.fromisoformat(date_str.split(".")[0])
                            except ValueError:
                                pass

        if revision is None:
            raise ValueError(f"No 'revision' found in {path}")

        # Create script
        script = cls(
            revision=revision,
            down_revision=down_revision,
            message=message,
            branch_labels=branch_labels,
            depends_on=depends_on,
            create_date=create_date,
            path=path,
            _source=source,
        )

        # Compile the module to get upgrade/downgrade functions
        # We need to provide mock 'alembic' and 'sqlalchemy' modules since migrations import them
        try:
            code = compile(source, str(path), "exec")

            # Create mock modules and inject them into sys.modules
            # This allows `from alembic import op` and `import sqlalchemy as sa` to work
            import sys
            mock_alembic = _create_mock_alembic_module()
            mock_sqlalchemy = _create_mock_sqlalchemy_module()
            old_alembic = sys.modules.get("alembic")
            old_sqlalchemy = sys.modules.get("sqlalchemy")
            sys.modules["alembic"] = mock_alembic  # type: ignore[assignment]
            sys.modules["sqlalchemy"] = mock_sqlalchemy  # type: ignore[assignment]

            try:
                module_dict: dict[str, Any] = {}
                exec(code, module_dict)

                if "upgrade" in module_dict:
                    script._upgrade_fn = module_dict["upgrade"]
                if "downgrade" in module_dict:
                    script._downgrade_fn = module_dict["downgrade"]
            finally:
                # Restore original modules if they existed
                if old_alembic is not None:
                    sys.modules["alembic"] = old_alembic
                elif "alembic" in sys.modules:
                    del sys.modules["alembic"]
                if old_sqlalchemy is not None:
                    sys.modules["sqlalchemy"] = old_sqlalchemy
                elif "sqlalchemy" in sys.modules:
                    del sys.modules["sqlalchemy"]
        except Exception as e:
            # Log but don't fail - we might just be reading metadata
            import warnings
            warnings.warn(f"Failed to load migration functions from {path}: {e}")

        return script

    def upgrade(self, op: Operations) -> None:
        """Execute the upgrade function.

        Args:
            op: Operations context for collecting SQL

        Raises:
            RuntimeError: If upgrade function not loaded
        """
        if self._upgrade_fn is None:
            raise RuntimeError(f"No upgrade() function in migration {self.revision}")

        # Set the global op context for Alembic-style migrations
        _set_current_op(op)
        try:
            # Alembic upgrade() takes no args, our wrapper might take op
            import inspect
            sig = inspect.signature(self._upgrade_fn)
            if len(sig.parameters) > 0:
                self._upgrade_fn(op)
            else:
                self._upgrade_fn()
        finally:
            _set_current_op(None)

    def downgrade(self, op: Operations) -> None:
        """Execute the downgrade function.

        Args:
            op: Operations context for collecting SQL

        Raises:
            RuntimeError: If downgrade function not loaded
        """
        if self._downgrade_fn is None:
            raise RuntimeError(f"No downgrade() function in migration {self.revision}")

        # Set the global op context for Alembic-style migrations
        _set_current_op(op)
        try:
            import inspect
            sig = inspect.signature(self._downgrade_fn)
            if len(sig.parameters) > 0:
                self._downgrade_fn(op)
            else:
                self._downgrade_fn()
        finally:
            _set_current_op(None)

    def render(self) -> str:
        """Render the migration as Alembic-compatible Python source.

        Returns:
            Python source code for the migration file
        """
        now = self.create_date or datetime.now()
        date_str = now.strftime("%Y-%m-%d %H:%M:%S.%f")

        # Format optional values
        down_rev = f"'{self.down_revision}'" if self.down_revision else "None"
        branch = repr(self.branch_labels) if self.branch_labels else "None"
        deps = repr(self.depends_on) if self.depends_on else "None"

        return f'''"""{self.message}

Revision ID: {self.revision}
Revises: {self.down_revision or 'None'}
Create Date: {date_str}
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '{self.revision}'
down_revision = {down_rev}
branch_labels = {branch}
depends_on = {deps}


def upgrade():
    pass


def downgrade():
    pass
'''

    @property
    def short_revision(self) -> str:
        """Get short form of revision (first 12 chars)."""
        return self.revision[:12]

    def __repr__(self) -> str:
        return f"MigrationScript(revision='{self.short_revision}', message='{self.message[:30]}...')"


def _eval_ast_value(node: ast.expr) -> Any:
    """Safely evaluate an AST node to a Python value.

    Only handles literals and simple expressions for security.
    """
    if isinstance(node, ast.Constant):
        return node.value
    elif isinstance(node, ast.List):
        return [_eval_ast_value(el) for el in node.elts]
    elif isinstance(node, ast.Tuple):
        return tuple(_eval_ast_value(el) for el in node.elts)
    elif isinstance(node, ast.Set):
        return {_eval_ast_value(el) for el in node.elts}
    elif isinstance(node, ast.Name):
        if node.id == "None":
            return None
        elif node.id == "True":
            return True
        elif node.id == "False":
            return False
    return None


def generate_revision_id() -> str:
    """Generate a unique revision ID.

    Returns:
        12-character hex string
    """
    import hashlib
    import time

    data = f"{time.time()}-{id(object())}"
    return hashlib.sha256(data.encode()).hexdigest()[:12]


def slugify(text: str, max_length: int = 40) -> str:
    """Convert text to a filename-safe slug.

    Args:
        text: Text to slugify
        max_length: Maximum slug length

    Returns:
        Slugified text
    """
    # Replace non-alphanumeric with underscores
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", text.lower())
    # Remove leading/trailing underscores
    slug = slug.strip("_")
    # Truncate
    if len(slug) > max_length:
        slug = slug[:max_length].rstrip("_")
    return slug or "migration"


# =============================================================================
# Alembic Compatibility Layer
# =============================================================================

# Thread-local storage for the current operations context
import threading

_current_op: threading.local = threading.local()


def _set_current_op(op: Operations | None) -> None:
    """Set the current operations context for Alembic-style migrations."""
    _current_op.instance = op


def _get_current_op() -> Operations:
    """Get the current operations context."""
    op = getattr(_current_op, "instance", None)
    if op is None:
        raise RuntimeError("No migration context active. This should be called within upgrade/downgrade.")
    return op


class _AlembicOpProxy:
    """Proxy that forwards Alembic op.* calls to current Operations instance.

    This allows migrations written for Alembic (using `from alembic import op`)
    to work with OrmKit's Operations class.
    """

    def create_table(self, table_name: str, *columns: Any, **kw: Any) -> None:
        """Create a table."""
        from ormkit.migrations.operations import Column, ColumnDef, CreateTable
        op = _get_current_op()
        col_defs = []
        for col in columns:
            # Handle SQLAlchemy Column objects from migrations
            if hasattr(col, "name") and hasattr(col, "type"):
                # SQLAlchemy Column
                col_type = str(col.type)
                col_defs.append(ColumnDef(
                    name=col.name,
                    type_=col_type,
                    nullable=col.nullable if hasattr(col, "nullable") else True,
                    primary_key=col.primary_key if hasattr(col, "primary_key") else False,
                ))
            elif isinstance(col, (Column, ColumnDef)):
                col_defs.append(col)
        op._operations.append(CreateTable(table_name, col_defs))

    def drop_table(self, table_name: str, **kw: Any) -> None:
        """Drop a table."""
        from ormkit.migrations.operations import DropTable
        op = _get_current_op()
        op._operations.append(DropTable(table_name))

    def add_column(self, table_name: str, column: Any) -> None:
        """Add a column."""
        from ormkit.migrations.operations import AddColumn, Column, ColumnDef
        op = _get_current_op()
        if hasattr(column, "name") and hasattr(column, "type"):
            col_def = ColumnDef(
                name=column.name,
                type_=str(column.type),
                nullable=column.nullable if hasattr(column, "nullable") else True,
            )
        elif isinstance(column, (Column, ColumnDef)):
            col_def = column
        else:
            raise TypeError(f"Unknown column type: {type(column)}")
        op._operations.append(AddColumn(table_name, col_def))

    def drop_column(self, table_name: str, column_name: str, **kw: Any) -> None:
        """Drop a column."""
        from ormkit.migrations.operations import DropColumn
        op = _get_current_op()
        op._operations.append(DropColumn(table_name, column_name))

    def create_index(
        self,
        index_name: str,
        table_name: str,
        columns: list[str],
        unique: bool = False,
        **kw: Any,
    ) -> None:
        """Create an index."""
        from ormkit.migrations.operations import CreateIndex
        op = _get_current_op()
        op._operations.append(CreateIndex(index_name, table_name, columns, unique))

    def drop_index(self, index_name: str, table_name: str | None = None, **kw: Any) -> None:
        """Drop an index."""
        from ormkit.migrations.operations import DropIndex
        op = _get_current_op()
        op._operations.append(DropIndex(index_name, table_name))

    def create_foreign_key(
        self,
        constraint_name: str,
        source_table: str,
        referent_table: str,
        local_cols: list[str],
        remote_cols: list[str],
        **kw: Any,
    ) -> None:
        """Create a foreign key."""
        from ormkit.migrations.operations import CreateForeignKey
        op = _get_current_op()
        op._operations.append(CreateForeignKey(
            constraint_name, source_table, local_cols, referent_table, remote_cols,
            kw.get("ondelete"), kw.get("onupdate"),
        ))

    def drop_constraint(self, constraint_name: str, table_name: str, **kw: Any) -> None:
        """Drop a constraint."""
        from ormkit.migrations.operations import DropConstraint
        op = _get_current_op()
        op._operations.append(DropConstraint(constraint_name, table_name))

    def execute(self, sql: str, **kw: Any) -> None:
        """Execute raw SQL."""
        from ormkit.migrations.operations import Execute
        op = _get_current_op()
        op._operations.append(Execute(sql))


class _MockAlembicModule:
    """Mock alembic module that provides the op proxy."""

    op = _AlembicOpProxy()


class _MockSaColumn:
    """Mock SQLAlchemy Column class for Alembic compatibility."""

    def __init__(
        self,
        name: str | None = None,
        type_: Any = None,
        *args: Any,
        primary_key: bool = False,
        nullable: bool = True,
        unique: bool = False,
        index: bool = False,
        **kwargs: Any
    ) -> None:
        self.name = name
        self.type = type_
        self.primary_key = primary_key
        self.nullable = nullable
        self.unique = unique
        self.index = index


class _MockSaType:
    """Mock SQLAlchemy type classes."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._args = args
        self._kwargs = kwargs

    def __str__(self) -> str:
        return self.__class__.__name__


class _MockInteger(_MockSaType):
    def __str__(self) -> str:
        return "INTEGER"


class _MockString(_MockSaType):
    def __init__(self, length: int | None = None, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.length = length

    def __str__(self) -> str:
        if self.length:
            return f"VARCHAR({self.length})"
        return "TEXT"


class _MockText(_MockSaType):
    def __str__(self) -> str:
        return "TEXT"


class _MockBoolean(_MockSaType):
    def __str__(self) -> str:
        return "BOOLEAN"


class _MockDateTime(_MockSaType):
    def __str__(self) -> str:
        return "TIMESTAMP"


class _MockUniqueConstraint:
    """Mock SQLAlchemy UniqueConstraint."""

    def __init__(self, *columns: str, **kwargs: Any) -> None:
        self.columns = columns


class _MockSqlalchemyModule:
    """Mock sqlalchemy module for Alembic compatibility."""

    Column = _MockSaColumn
    Integer = _MockInteger
    String = _MockString
    Text = _MockText
    Boolean = _MockBoolean
    DateTime = _MockDateTime
    UniqueConstraint = _MockUniqueConstraint


def _create_mock_alembic_module() -> _MockAlembicModule:
    """Create a mock alembic module for executing migrations."""
    return _MockAlembicModule()


def _create_mock_sqlalchemy_module() -> _MockSqlalchemyModule:
    """Create a mock sqlalchemy module for executing migrations."""
    return _MockSqlalchemyModule()
