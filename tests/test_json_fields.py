"""Tests for JSON/JSONB field support."""

from __future__ import annotations

import pytest

from ormkit import AsyncSession, Base, Mapped, mapped_column
from ormkit.fields import JSON


class Product(Base):
    """Test model with JSON field."""

    __tablename__ = "products"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(max_length=100)
    metadata: Mapped[dict | None] = mapped_column(JSON, nullable=True)


class TestJSONFieldDefinition:
    """Test JSON field model definition."""

    def test_json_column_type_postgresql(self) -> None:
        """JSON field maps to JSONB in PostgreSQL."""
        col = Product.__columns__["metadata"]
        assert col.sql_type("postgresql") == "JSONB"

    def test_json_column_type_sqlite(self) -> None:
        """JSON field maps to TEXT in SQLite (stored as JSON string)."""
        col = Product.__columns__["metadata"]
        assert col.sql_type("sqlite") == "TEXT"

    def test_dict_type_hint_infers_json(self) -> None:
        """Mapped[dict] should be treated as JSON column."""
        col = Product.__columns__["metadata"]
        assert col.is_json is True

    def test_json_field_nullable(self) -> None:
        """JSON field can be nullable."""
        col = Product.__columns__["metadata"]
        assert col.nullable is True


@pytest.fixture
async def products_table(sqlite_pool):
    """Create products table for testing."""
    await sqlite_pool.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            metadata TEXT
        )
    """, [])
    return sqlite_pool


class TestJSONFieldCRUD:
    """Test CRUD operations with JSON fields."""

    async def test_insert_with_json(self, products_table: AsyncSession) -> None:
        """Insert model with JSON data."""
        session = AsyncSession(products_table)
        product = await session.insert(
            Product(
                name="Widget",
                metadata={"color": "red", "size": "large", "tags": ["sale"]},
            )
        )
        assert product.id is not None

    async def test_select_returns_json_as_dict(self, products_table: AsyncSession) -> None:
        """JSON field returns as Python dict."""
        session = AsyncSession(products_table)
        await session.insert(Product(name="Widget", metadata={"color": "red"}))

        product = await session.query(Product).filter(name="Widget").first()
        assert product is not None
        assert isinstance(product.metadata, dict)
        assert product.metadata["color"] == "red"

    async def test_update_json_field(self, products_table: AsyncSession) -> None:
        """Update JSON field."""
        session = AsyncSession(products_table)
        product = await session.insert(Product(name="Widget", metadata={"v": 1}))

        await session.update(product, metadata={"v": 2, "updated": True})

        reloaded = await session.get(Product, product.id)
        assert reloaded is not None
        assert reloaded.metadata == {"v": 2, "updated": True}

    async def test_json_with_nested_objects(self, products_table: AsyncSession) -> None:
        """JSON with deeply nested structure."""
        session = AsyncSession(products_table)
        product = await session.insert(
            Product(
                name="Complex",
                metadata={
                    "specs": {"dimensions": {"width": 10, "height": 20}, "weight": 1.5},
                    "tags": ["a", "b", "c"],
                },
            )
        )

        loaded = await session.get(Product, product.id)
        assert loaded is not None
        assert loaded.metadata["specs"]["dimensions"]["width"] == 10

    async def test_json_with_null_value(self, products_table: AsyncSession) -> None:
        """JSON field can be None."""
        session = AsyncSession(products_table)
        product = await session.insert(Product(name="NoMeta", metadata=None))
        loaded = await session.get(Product, product.id)
        assert loaded is not None
        assert loaded.metadata is None

    async def test_json_with_array_value(self, products_table: AsyncSession) -> None:
        """JSON field can be a list/array."""
        session = AsyncSession(products_table)
        product = await session.insert(
            Product(name="ArrayMeta", metadata=["a", "b", "c"])  # type: ignore[arg-type]
        )
        loaded = await session.get(Product, product.id)
        assert loaded is not None
        assert loaded.metadata == ["a", "b", "c"]

    async def test_json_with_empty_object(self, products_table: AsyncSession) -> None:
        """JSON field can be an empty dict."""
        session = AsyncSession(products_table)
        product = await session.insert(Product(name="EmptyMeta", metadata={}))
        loaded = await session.get(Product, product.id)
        assert loaded is not None
        assert loaded.metadata == {}

    async def test_json_with_empty_array(self, products_table: AsyncSession) -> None:
        """JSON field can be an empty array."""
        session = AsyncSession(products_table)
        product = await session.insert(
            Product(name="EmptyArray", metadata=[])  # type: ignore[arg-type]
        )
        loaded = await session.get(Product, product.id)
        assert loaded is not None
        assert loaded.metadata == []


class TestJSONQueryOperators:
    """Test querying JSON fields."""

    async def test_json_key_equals(self, products_table: AsyncSession) -> None:
        """Filter by JSON key value: metadata__color='red'."""
        session = AsyncSession(products_table)
        await session.insert(Product(name="Red", metadata={"color": "red"}))
        await session.insert(Product(name="Blue", metadata={"color": "blue"}))

        products = await session.query(Product).filter(metadata__color="red").all()
        assert len(products) == 1
        assert products[0].name == "Red"

    async def test_json_nested_key(self, products_table: AsyncSession) -> None:
        """Filter by nested JSON path: metadata__specs__weight__gt=1.0."""
        session = AsyncSession(products_table)
        await session.insert(Product(name="Heavy", metadata={"specs": {"weight": 2.0}}))
        await session.insert(Product(name="Light", metadata={"specs": {"weight": 0.5}}))

        products = await session.query(Product).filter(metadata__specs__weight__gt=1.0).all()
        assert len(products) == 1
        assert products[0].name == "Heavy"

    async def test_json_array_contains_element(self, products_table: AsyncSession) -> None:
        """Check if JSON array contains element."""
        session = AsyncSession(products_table)
        await session.insert(Product(name="Sale", metadata={"tags": ["sale", "featured"]}))
        await session.insert(Product(name="Regular", metadata={"tags": ["normal"]}))

        # Array element check (SQLite uses json_each)
        products = await session.query(Product).filter(metadata__tags__contains="sale").all()
        assert len(products) == 1
        assert products[0].name == "Sale"


class TestJSONQueryOperatorsPostgreSQL:
    """PostgreSQL-specific JSON query tests."""

    @pytest.mark.skipif(True, reason="Requires PostgreSQL")
    async def test_json_contains_postgresql(self, postgres_pool: AsyncSession) -> None:
        """PostgreSQL @> contains operator."""
        session = AsyncSession(postgres_pool)
        await session.insert(Product(name="TaggedSale", metadata={"tags": ["sale", "new"]}))
        await session.insert(Product(name="TaggedOther", metadata={"tags": ["clearance"]}))

        # Contains check
        products = await session.query(Product).filter(
            metadata__contains={"tags": ["sale"]}
        ).all()
        assert len(products) == 1
        assert products[0].name == "TaggedSale"

    @pytest.mark.skipif(True, reason="Requires PostgreSQL")
    async def test_json_has_key_postgresql(self, postgres_pool: AsyncSession) -> None:
        """PostgreSQL ? has_key operator."""
        session = AsyncSession(postgres_pool)
        await session.insert(Product(name="HasColor", metadata={"color": "red"}))
        await session.insert(Product(name="NoColor", metadata={"size": "large"}))

        products = await session.query(Product).filter(metadata__has_key="color").all()
        assert len(products) == 1
        assert products[0].name == "HasColor"


class TestJSONTypeCoercion:
    """Test type handling in JSON fields."""

    async def test_json_integer_comparison(self, products_table: AsyncSession) -> None:
        """Compare JSON integer values."""
        session = AsyncSession(products_table)
        await session.insert(Product(name="P1", metadata={"count": 10}))
        await session.insert(Product(name="P2", metadata={"count": 20}))

        products = await session.query(Product).filter(metadata__count__gte=15).all()
        assert len(products) == 1
        assert products[0].name == "P2"

    async def test_json_boolean_comparison(self, products_table: AsyncSession) -> None:
        """Compare JSON boolean values."""
        session = AsyncSession(products_table)
        await session.insert(Product(name="Active", metadata={"active": True}))
        await session.insert(Product(name="Inactive", metadata={"active": False}))

        products = await session.query(Product).filter(metadata__active=True).all()
        assert len(products) == 1
        assert products[0].name == "Active"

    async def test_json_float_comparison(self, products_table: AsyncSession) -> None:
        """Compare JSON float values."""
        session = AsyncSession(products_table)
        await session.insert(Product(name="Cheap", metadata={"price": 9.99}))
        await session.insert(Product(name="Expensive", metadata={"price": 99.99}))

        products = await session.query(Product).filter(metadata__price__lt=50.0).all()
        assert len(products) == 1
        assert products[0].name == "Cheap"

    async def test_json_null_value_in_object(self, products_table: AsyncSession) -> None:
        """JSON object with null value."""
        session = AsyncSession(products_table)
        await session.insert(Product(name="WithNull", metadata={"value": None}))

        product = await session.query(Product).filter(name="WithNull").first()
        assert product is not None
        assert product.metadata == {"value": None}

    async def test_json_string_value(self, products_table: AsyncSession) -> None:
        """JSON object with string value."""
        session = AsyncSession(products_table)
        await session.insert(Product(name="WithString", metadata={"status": "active"}))

        products = await session.query(Product).filter(metadata__status="active").all()
        assert len(products) == 1
        assert products[0].name == "WithString"
