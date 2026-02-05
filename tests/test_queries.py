"""Tests for query building."""

from ormkit import Base, Mapped, mapped_column, select, insert, update, delete
from ormkit.query import WhereClause


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(max_length=100)
    email: Mapped[str] = mapped_column(unique=True)


def test_select_basic():
    """Test basic SELECT generation."""
    stmt = select(User)
    sql, params = stmt.to_sql("postgresql")
    assert "SELECT" in sql
    assert "FROM users" in sql
    assert params == []


def test_select_with_where():
    """Test SELECT with WHERE clause."""
    stmt = select(User).where(WhereClause("name", "=", "Alice"))
    sql, params = stmt.to_sql("postgresql")
    assert "WHERE name = $1" in sql
    assert params == ["Alice"]


def test_select_with_filter_by():
    """Test SELECT with filter_by."""
    stmt = select(User).filter_by(name="Alice", email="alice@example.com")
    sql, params = stmt.to_sql("postgresql")
    assert "WHERE" in sql
    assert "name = $1" in sql
    assert params == ["Alice", "alice@example.com"]


def test_select_with_limit_offset():
    """Test SELECT with LIMIT and OFFSET."""
    stmt = select(User).limit(10).offset(20)
    sql, params = stmt.to_sql("postgresql")
    assert "LIMIT 10" in sql
    assert "OFFSET 20" in sql


def test_select_with_order_by():
    """Test SELECT with ORDER BY."""
    stmt = select(User).order_by("name")
    sql, params = stmt.to_sql("postgresql")
    assert "ORDER BY name ASC" in sql

    stmt = select(User).order_by("name", desc=True)
    sql, params = stmt.to_sql("postgresql")
    assert "ORDER BY name DESC" in sql


def test_insert_single():
    """Test single row INSERT."""
    stmt = insert(User).values(name="Alice", email="alice@example.com")
    sql, params = stmt.to_sql("postgresql")
    assert "INSERT INTO users" in sql
    assert "VALUES ($1, $2)" in sql or "VALUES ($2, $1)" in sql
    assert "Alice" in params
    assert "alice@example.com" in params


def test_insert_multiple():
    """Test multiple row INSERT."""
    stmt = insert(User).values(
        {"name": "Alice", "email": "alice@example.com"},
        {"name": "Bob", "email": "bob@example.com"},
    )
    sql, params = stmt.to_sql("postgresql")
    assert "INSERT INTO users" in sql
    assert "VALUES" in sql
    assert len(params) == 4


def test_insert_returning():
    """Test INSERT with RETURNING."""
    stmt = insert(User).values(name="Alice", email="a@b.com").returning("id")
    sql, params = stmt.to_sql("postgresql")
    assert "RETURNING id" in sql


def test_update_basic():
    """Test basic UPDATE."""
    stmt = update(User).values(name="Bob")
    sql, params = stmt.to_sql("postgresql")
    assert "UPDATE users SET name = $1" in sql
    assert params == ["Bob"]


def test_update_with_where():
    """Test UPDATE with WHERE."""
    stmt = update(User).values(name="Bob").where(WhereClause("id", "=", 1))
    sql, params = stmt.to_sql("postgresql")
    assert "UPDATE users SET name = $1" in sql
    assert "WHERE id = $2" in sql
    assert params == ["Bob", 1]


def test_delete_basic():
    """Test basic DELETE."""
    stmt = delete(User)
    sql, params = stmt.to_sql("postgresql")
    assert "DELETE FROM users" in sql
    assert params == []


def test_delete_with_where():
    """Test DELETE with WHERE."""
    stmt = delete(User).where(WhereClause("id", "=", 1))
    sql, params = stmt.to_sql("postgresql")
    assert "DELETE FROM users" in sql
    assert "WHERE id = $1" in sql
    assert params == [1]


def test_sqlite_placeholders():
    """Test SQLite uses ? placeholders."""
    stmt = select(User).where(WhereClause("name", "=", "Alice"))
    sql, params = stmt.to_sql("sqlite")
    assert "WHERE name = ?" in sql
    assert "$" not in sql
