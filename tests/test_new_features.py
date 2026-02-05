"""Tests for new ORM features: Q objects, aggregates, streaming, etc."""

import pytest
from ormkit import (
    Base,
    Mapped,
    mapped_column,
    ForeignKey,
    relationship,
    create_engine,
    create_session,
    Q,
    joinedload,
    selectinload,
)


class NewFeatureUser(Base):
    __tablename__ = "new_feature_users"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()
    age: Mapped[int] = mapped_column()
    active: Mapped[bool] = mapped_column()
    score: Mapped[float | None] = mapped_column(nullable=True)
    posts: Mapped[list["NewFeaturePost"]] = relationship(back_populates="author")


class NewFeaturePost(Base):
    __tablename__ = "new_feature_posts"
    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column()
    author_id: Mapped[int] = mapped_column(ForeignKey("new_feature_users.id"))
    author: Mapped[NewFeatureUser] = relationship(back_populates="posts")


@pytest.fixture
async def session():
    """Create a test session with sample data."""
    pool = await create_engine("sqlite::memory:")

    await pool.execute("""
        CREATE TABLE new_feature_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            age INTEGER NOT NULL,
            active INTEGER NOT NULL,
            score REAL
        )
    """)
    await pool.execute("""
        CREATE TABLE new_feature_posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            author_id INTEGER REFERENCES new_feature_users(id)
        )
    """)

    # Insert sample data
    await pool.execute(
        "INSERT INTO new_feature_users (name, age, active, score) VALUES (?, ?, ?, ?)",
        ["Alice", 25, 1, 95.5],
    )
    await pool.execute(
        "INSERT INTO new_feature_users (name, age, active, score) VALUES (?, ?, ?, ?)",
        ["Bob", 30, 1, 85.0],
    )
    await pool.execute(
        "INSERT INTO new_feature_users (name, age, active, score) VALUES (?, ?, ?, ?)",
        ["Charlie", 35, 0, 75.5],
    )
    await pool.execute(
        "INSERT INTO new_feature_users (name, age, active, score) VALUES (?, ?, ?, ?)",
        ["Diana", 28, 1, None],
    )

    # Insert posts
    await pool.execute(
        "INSERT INTO new_feature_posts (title, author_id) VALUES (?, ?)",
        ["Post 1", 1],
    )
    await pool.execute(
        "INSERT INTO new_feature_posts (title, author_id) VALUES (?, ?)",
        ["Post 2", 1],
    )
    await pool.execute(
        "INSERT INTO new_feature_posts (title, author_id) VALUES (?, ?)",
        ["Post 3", 2],
    )

    sess = create_session(pool)
    yield sess
    await pool.close()


# ========== Q Objects Tests ==========


class TestQObjects:
    async def test_simple_q_object(self, session):
        """Test basic Q object filtering."""
        users = await session.query(NewFeatureUser).filter(Q(age__gt=25)).all()
        assert len(users) == 3
        assert all(u.age > 25 for u in users)

    async def test_q_or_condition(self, session):
        """Test OR conditions with Q objects."""
        users = await session.query(NewFeatureUser).filter(
            Q(name="Alice") | Q(name="Bob")
        ).all()
        assert len(users) == 2
        names = {u.name for u in users}
        assert names == {"Alice", "Bob"}

    async def test_q_and_condition(self, session):
        """Test explicit AND conditions with Q objects."""
        users = await session.query(NewFeatureUser).filter(
            Q(age__gte=25) & Q(active=True)
        ).all()
        assert len(users) == 3

    async def test_q_negation(self, session):
        """Test NOT conditions with Q objects."""
        users = await session.query(NewFeatureUser).filter(~Q(active=True)).all()
        assert len(users) == 1
        assert users[0].name == "Charlie"

    async def test_q_complex_condition(self, session):
        """Test complex nested Q conditions."""
        users = await session.query(NewFeatureUser).filter(
            (Q(age__lt=30) | Q(age__gt=32)) & Q(active=True)
        ).all()
        assert len(users) == 2
        names = {u.name for u in users}
        assert names == {"Alice", "Diana"}


# ========== Filter Operators Tests ==========


class TestFilterOperators:
    async def test_in_operator(self, session):
        """Test __in operator."""
        users = await session.query(NewFeatureUser).filter(name__in=["Alice", "Bob"]).all()
        assert len(users) == 2

    async def test_notin_operator(self, session):
        """Test __notin operator."""
        users = await session.query(NewFeatureUser).filter(name__notin=["Alice", "Bob"]).all()
        assert len(users) == 2
        names = {u.name for u in users}
        assert names == {"Charlie", "Diana"}

    async def test_isnull_operator(self, session):
        """Test __isnull operator."""
        users = await session.query(NewFeatureUser).filter(score__isnull=True).all()
        assert len(users) == 1
        assert users[0].name == "Diana"

    async def test_contains_operator(self, session):
        """Test __contains operator (LIKE %value%)."""
        users = await session.query(NewFeatureUser).filter(name__contains="li").all()
        assert len(users) == 2
        names = {u.name for u in users}
        assert names == {"Alice", "Charlie"}

    async def test_startswith_operator(self, session):
        """Test __startswith operator."""
        users = await session.query(NewFeatureUser).filter(name__startswith="A").all()
        assert len(users) == 1
        assert users[0].name == "Alice"

    async def test_endswith_operator(self, session):
        """Test __endswith operator."""
        users = await session.query(NewFeatureUser).filter(name__endswith="e").all()
        assert len(users) == 2
        names = {u.name for u in users}
        assert names == {"Alice", "Charlie"}

    async def test_empty_in_returns_no_results(self, session):
        """Test that __in with empty list returns no results."""
        users = await session.query(NewFeatureUser).filter(name__in=[]).all()
        assert len(users) == 0

    async def test_empty_notin_returns_all(self, session):
        """Test that __notin with empty list returns all results."""
        users = await session.query(NewFeatureUser).filter(name__notin=[]).all()
        assert len(users) == 4


# ========== Aggregate Tests ==========


class TestAggregates:
    async def test_count(self, session):
        """Test COUNT aggregate."""
        count = await session.query(NewFeatureUser).count()
        assert count == 4

    async def test_count_with_filter(self, session):
        """Test COUNT with filter."""
        count = await session.query(NewFeatureUser).filter(active=True).count()
        assert count == 3

    async def test_sum(self, session):
        """Test SUM aggregate."""
        total = await session.query(NewFeatureUser).sum("age")
        assert total == 25 + 30 + 35 + 28  # 118

    async def test_avg(self, session):
        """Test AVG aggregate."""
        avg = await session.query(NewFeatureUser).avg("age")
        assert avg == pytest.approx(29.5, rel=0.01)

    async def test_min(self, session):
        """Test MIN aggregate."""
        min_age = await session.query(NewFeatureUser).min("age")
        assert min_age == 25

    async def test_max(self, session):
        """Test MAX aggregate."""
        max_age = await session.query(NewFeatureUser).max("age")
        assert max_age == 35

    async def test_aggregate_with_filter(self, session):
        """Test aggregate with filter."""
        avg = await session.query(NewFeatureUser).filter(active=True).avg("score")
        # Only Alice (95.5) and Bob (85.0) have scores and are active
        assert avg == pytest.approx(90.25, rel=0.01)


# ========== DISTINCT Tests ==========


class TestDistinct:
    async def test_distinct(self, session):
        """Test DISTINCT query."""
        # Get distinct active values
        result = await session.query(NewFeatureUser).distinct().values("active")
        active_values = [r["active"] for r in result]
        assert len(active_values) == 2  # True and False


# ========== Bulk Update Tests ==========


class TestBulkUpdate:
    async def test_bulk_update(self, session):
        """Test bulk update."""
        count = await session.bulk_update(NewFeatureUser, {"active": False}, age__gt=30)
        assert count == 1  # Only Charlie (35)

        # Verify the update
        charlie = await session.query(NewFeatureUser).filter(name="Charlie").first()
        assert charlie.active == False

    async def test_bulk_update_with_q_object(self, session):
        """Test bulk update with Q object."""
        count = await session.bulk_update(
            NewFeatureUser,
            {"score": 100.0},
            Q(name="Alice") | Q(name="Bob"),
        )
        assert count == 2

    async def test_query_update(self, session):
        """Test update via query builder."""
        count = await session.query(NewFeatureUser).filter(name="Diana").update(score=50.0)
        assert count == 1

        diana = await session.query(NewFeatureUser).filter(name="Diana").first()
        assert diana.score == 50.0


# ========== Values/Values List Tests ==========


class TestValues:
    async def test_values(self, session):
        """Test values() returns dicts with specific columns."""
        result = await session.query(NewFeatureUser).order_by("id").values("id", "name")
        assert len(result) == 4
        assert result[0] == {"id": 1, "name": "Alice"}
        assert "age" not in result[0]

    async def test_values_list(self, session):
        """Test values_list() returns tuples."""
        result = await session.query(NewFeatureUser).order_by("id").values_list("id", "name")
        assert len(result) == 4
        assert result[0] == (1, "Alice")

    async def test_values_list_flat(self, session):
        """Test values_list() with flat=True."""
        result = await session.query(NewFeatureUser).order_by("name").values_list("name", flat=True)
        assert result == ["Alice", "Bob", "Charlie", "Diana"]


# ========== Streaming Tests ==========


class TestStreaming:
    async def test_stream(self, session):
        """Test streaming results."""
        count = 0
        async for user in session.query(NewFeatureUser).order_by("id").stream(batch_size=2):
            count += 1
        assert count == 4

    async def test_stream_with_filter(self, session):
        """Test streaming with filter."""
        names = []
        async for user in session.query(NewFeatureUser).filter(active=True).stream():
            names.append(user.name)
        assert len(names) == 3

    async def test_query_as_async_iterator(self, session):
        """Test using query directly as async iterator."""
        count = 0
        async for user in session.query(NewFeatureUser):
            count += 1
        assert count == 4


# ========== Proper JOINs Tests ==========


class TestJoinedLoad:
    async def test_joinedload_generates_join(self, session):
        """Test that joinedload generates actual LEFT JOIN SQL."""
        query = session.query(NewFeaturePost).options(joinedload("author"))
        sql, _ = query._build_select_sql()

        assert "LEFT JOIN" in sql
        assert "new_feature_users" in sql

    async def test_joinedload_loads_relationship(self, session):
        """Test that joinedload properly loads the relationship."""
        posts = await session.query(NewFeaturePost).options(joinedload("author")).all()

        assert len(posts) == 3
        # Check that authors are loaded without additional queries
        assert posts[0].author is not None
        assert posts[0].author.name == "Alice"

    async def test_joinedload_vs_selectinload(self, session):
        """Test that joinedload and selectinload give same results."""
        posts_joined = await session.query(NewFeaturePost).options(joinedload("author")).order_by("id").all()
        posts_selectin = await session.query(NewFeaturePost).options(selectinload("author")).order_by("id").all()

        assert len(posts_joined) == len(posts_selectin)
        for pj, ps in zip(posts_joined, posts_selectin):
            assert pj.id == ps.id
            assert pj.author.name == ps.author.name


# ========== GROUP BY / HAVING Tests ==========


class TestGroupBy:
    async def test_group_by(self, session):
        """Test GROUP BY query."""
        # Count posts per author
        result = await session.query(NewFeaturePost).group_by("author_id").values("author_id")
        assert len(result) == 2  # Alice and Bob have posts
