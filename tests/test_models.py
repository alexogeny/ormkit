"""Tests for model definition."""

from datetime import datetime

from ormkit import Base, ForeignKey, Mapped, mapped_column, relationship


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(max_length=100)
    email: Mapped[str] = mapped_column(unique=True)
    age: Mapped[int | None] = mapped_column(nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=datetime.now)


class Post(Base):
    __tablename__ = "posts"

    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column(max_length=200)
    author_id: Mapped[int] = mapped_column(ForeignKey("users.id"))


def test_model_tablename():
    """Test that __tablename__ is correctly set."""
    assert User.__tablename__ == "users"
    assert Post.__tablename__ == "posts"


def test_model_columns():
    """Test that columns are correctly parsed."""
    assert "id" in User.__columns__
    assert "name" in User.__columns__
    assert "email" in User.__columns__
    assert "age" in User.__columns__


def test_model_primary_key():
    """Test that primary key is detected."""
    assert User.__primary_key__ == "id"
    assert Post.__primary_key__ == "id"


def test_model_column_properties():
    """Test column properties are correctly set."""
    id_col = User.__columns__["id"]
    assert id_col.primary_key is True

    name_col = User.__columns__["name"]
    assert name_col.max_length == 100
    assert name_col.primary_key is False

    email_col = User.__columns__["email"]
    assert email_col.unique is True

    age_col = User.__columns__["age"]
    assert age_col.nullable is True


def test_model_foreign_key():
    """Test foreign key detection."""
    author_id_col = Post.__columns__["author_id"]
    assert author_id_col.foreign_key is not None
    assert author_id_col.foreign_key.target == "users.id"
    assert author_id_col.foreign_key.table == "users"
    assert author_id_col.foreign_key.column == "id"


def test_model_instantiation():
    """Test creating model instances."""
    user = User(name="Alice", email="alice@example.com")
    assert user.name == "Alice"
    assert user.email == "alice@example.com"


def test_model_to_dict():
    """Test converting model to dictionary."""
    user = User(name="Alice", email="alice@example.com", age=30)
    d = user.to_dict()
    assert d["name"] == "Alice"
    assert d["email"] == "alice@example.com"
    assert d["age"] == 30


def test_model_from_dict():
    """Test creating model from dictionary."""
    data = {"name": "Bob", "email": "bob@example.com", "age": 25}
    user = User.from_dict(data)
    assert user.name == "Bob"
    assert user.email == "bob@example.com"
    assert user.age == 25


def test_model_repr():
    """Test model string representation."""
    user = User(name="Alice", email="alice@example.com")
    user.id = 1
    assert repr(user) == "<User id=1>"


def test_sql_type_generation():
    """Test SQL type generation for columns."""
    id_col = User.__columns__["id"]
    assert "SERIAL" in id_col.sql_type("postgresql") or "INTEGER" in id_col.sql_type("postgresql")

    name_col = User.__columns__["name"]
    assert name_col.sql_type("postgresql") == "VARCHAR(100)"
    assert name_col.sql_type("sqlite") == "TEXT"
