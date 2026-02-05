"""Benchmarks for many-to-many relationship operations."""

import asyncio
import time


async def setup_tables(pool) -> None:
    """Create benchmark tables for M2M testing."""
    # Users table
    await pool.execute("DROP TABLE IF EXISTS user_roles", [])
    await pool.execute("DROP TABLE IF EXISTS bench_users", [])
    await pool.execute("DROP TABLE IF EXISTS bench_roles", [])

    await pool.execute(
        """
        CREATE TABLE bench_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL
        )
        """,
        [],
    )

    await pool.execute(
        """
        CREATE TABLE bench_roles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL
        )
        """,
        [],
    )

    await pool.execute(
        """
        CREATE TABLE user_roles (
            user_id INTEGER NOT NULL REFERENCES bench_users(id),
            role_id INTEGER NOT NULL REFERENCES bench_roles(id),
            PRIMARY KEY (user_id, role_id)
        )
        """,
        [],
    )


async def seed_data(pool, user_count: int = 100, role_count: int = 10, roles_per_user: int = 3) -> None:
    """Seed tables with test data.

    Args:
        pool: Database connection pool
        user_count: Number of users to create
        role_count: Number of roles to create
        roles_per_user: Average number of roles per user
    """
    # Insert users
    values = [f"('User {i}')" for i in range(user_count)]
    await pool.execute(
        f"INSERT INTO bench_users (name) VALUES {', '.join(values)}", []
    )

    # Insert roles
    values = [f"('Role {i}')" for i in range(role_count)]
    await pool.execute(
        f"INSERT INTO bench_roles (name) VALUES {', '.join(values)}", []
    )

    # Insert user-role associations
    values = []
    for user_id in range(1, user_count + 1):
        # Each user gets roles_per_user roles (cycling through available roles)
        for j in range(roles_per_user):
            role_id = (user_id + j - 1) % role_count + 1
            values.append(f"({user_id}, {role_id})")

    await pool.execute(
        f"INSERT INTO user_roles (user_id, role_id) VALUES {', '.join(values)}", []
    )


async def bench_load_m2m_selectin(pool, iterations: int = 100) -> float:
    """Benchmark loading M2M with SELECT IN strategy.

    This simulates selectinload behavior:
    1. SELECT * FROM users
    2. SELECT * FROM roles WHERE id IN (SELECT role_id FROM user_roles WHERE user_id IN (...))

    Returns time in milliseconds.
    """
    start = time.perf_counter()
    for _ in range(iterations):
        # First query: get users
        result = await pool.execute_query("SELECT * FROM bench_users", [])
        users = result.all()
        user_ids = [u["id"] for u in users]

        if user_ids:
            # Second query: get roles via junction table
            placeholders = ", ".join(["?"] * len(user_ids))
            result = await pool.execute_query(
                f"""
                SELECT r.*, ur.user_id
                FROM bench_roles r
                JOIN user_roles ur ON r.id = ur.role_id
                WHERE ur.user_id IN ({placeholders})
                """,
                user_ids,
            )
            _ = result.all()

    elapsed = time.perf_counter() - start
    return elapsed * 1000


async def bench_load_m2m_join(pool, iterations: int = 100) -> float:
    """Benchmark loading M2M with JOIN strategy.

    This simulates joinedload behavior (single query with joins).

    Returns time in milliseconds.
    """
    start = time.perf_counter()
    for _ in range(iterations):
        result = await pool.execute_query(
            """
            SELECT u.*, r.id as role_id, r.name as role_name
            FROM bench_users u
            LEFT JOIN user_roles ur ON u.id = ur.user_id
            LEFT JOIN bench_roles r ON ur.role_id = r.id
            """,
            [],
        )
        _ = result.all()

    elapsed = time.perf_counter() - start
    return elapsed * 1000


async def bench_add_m2m_single(pool, count: int = 100) -> float:
    """Benchmark adding single M2M relationships.

    Returns time in milliseconds.
    """
    # Get existing users and roles
    users_result = await pool.execute_query(
        f"SELECT id FROM bench_users LIMIT {count}", []
    )
    users = [r["id"] for r in users_result.all()]

    roles_result = await pool.execute_query("SELECT id FROM bench_roles LIMIT 1", [])
    role_id = roles_result.all()[0]["id"]

    # Delete existing associations for these users to this role
    await pool.execute(
        f"DELETE FROM user_roles WHERE role_id = ?", [role_id]
    )

    start = time.perf_counter()
    for user_id in users:
        await pool.execute(
            "INSERT OR IGNORE INTO user_roles (user_id, role_id) VALUES (?, ?)",
            [user_id, role_id],
        )
    elapsed = time.perf_counter() - start
    return elapsed * 1000


async def bench_add_m2m_batch(pool, user_count: int = 100, roles_per_user: int = 5) -> float:
    """Benchmark batch adding M2M relationships.

    Returns time in milliseconds.
    """
    # Clear junction table
    await pool.execute("DELETE FROM user_roles", [])

    # Build batch insert
    values = []
    params = []
    for user_id in range(1, user_count + 1):
        for role_id in range(1, min(roles_per_user + 1, 11)):
            values.append("(?, ?)")
            params.extend([user_id, role_id])

    start = time.perf_counter()
    await pool.execute(
        f"INSERT INTO user_roles (user_id, role_id) VALUES {', '.join(values)}",
        params,
    )
    elapsed = time.perf_counter() - start
    return elapsed * 1000


async def bench_remove_m2m(pool, count: int = 100) -> float:
    """Benchmark removing M2M relationships.

    Returns time in milliseconds.
    """
    # Get some associations to remove
    result = await pool.execute_query(
        f"SELECT user_id, role_id FROM user_roles LIMIT {count}", []
    )
    associations = result.all()

    start = time.perf_counter()
    for assoc in associations:
        await pool.execute(
            "DELETE FROM user_roles WHERE user_id = ? AND role_id = ?",
            [assoc["user_id"], assoc["role_id"]],
        )
    elapsed = time.perf_counter() - start
    return elapsed * 1000


async def bench_clear_m2m(pool, user_id: int = 1) -> float:
    """Benchmark clearing all M2M relationships for an entity.

    Returns time in milliseconds.
    """
    start = time.perf_counter()
    await pool.execute(
        "DELETE FROM user_roles WHERE user_id = ?", [user_id]
    )
    elapsed = time.perf_counter() - start
    return elapsed * 1000


async def run_benchmarks() -> None:
    """Run all M2M benchmarks."""
    from ormkit import create_engine

    print("\n" + "=" * 60)
    print("Many-to-Many Relationship Benchmarks")
    print("=" * 60)

    pool = await create_engine("sqlite::memory:")
    await setup_tables(pool)
    await seed_data(pool, user_count=100, role_count=10, roles_per_user=3)

    # Loading benchmarks
    print("\n--- Load M2M with SELECT IN (100 iterations) ---")
    elapsed = await bench_load_m2m_selectin(pool, 100)
    print(f"Time: {elapsed:.2f}ms ({100 / (elapsed / 1000):.0f} queries/sec)")

    print("\n--- Load M2M with JOIN (100 iterations) ---")
    elapsed = await bench_load_m2m_join(pool, 100)
    print(f"Time: {elapsed:.2f}ms ({100 / (elapsed / 1000):.0f} queries/sec)")

    # Modification benchmarks
    await setup_tables(pool)
    await seed_data(pool, user_count=100, role_count=10, roles_per_user=3)

    print("\n--- Add single M2M (100 associations) ---")
    elapsed = await bench_add_m2m_single(pool, 100)
    print(f"Time: {elapsed:.2f}ms ({100 / (elapsed / 1000):.0f} ops/sec)")

    await setup_tables(pool)
    print("\n--- Add batch M2M (100 users x 5 roles = 500 associations) ---")
    elapsed = await bench_add_m2m_batch(pool, 100, 5)
    print(f"Time: {elapsed:.2f}ms ({500 / (elapsed / 1000):.0f} ops/sec)")

    await seed_data(pool, user_count=100, role_count=10, roles_per_user=3)
    print("\n--- Remove M2M (100 associations) ---")
    elapsed = await bench_remove_m2m(pool, 100)
    print(f"Time: {elapsed:.2f}ms ({100 / (elapsed / 1000):.0f} ops/sec)")

    await seed_data(pool, user_count=100, role_count=10, roles_per_user=3)
    print("\n--- Clear all M2M for one user ---")
    elapsed = await bench_clear_m2m(pool, 1)
    print(f"Time: {elapsed:.4f}ms")

    # Comparison: SELECT IN vs JOIN
    print("\n--- Strategy Comparison ---")
    await setup_tables(pool)
    await seed_data(pool, user_count=100, role_count=10, roles_per_user=5)

    selectin_time = await bench_load_m2m_selectin(pool, 50)
    join_time = await bench_load_m2m_join(pool, 50)

    print(f"SELECT IN: {selectin_time:.2f}ms for 50 iterations")
    print(f"JOIN: {join_time:.2f}ms for 50 iterations")
    if selectin_time < join_time:
        print(f"SELECT IN is {join_time / selectin_time:.1f}x faster")
    else:
        print(f"JOIN is {selectin_time / join_time:.1f}x faster")

    await pool.close()
    print("\n" + "=" * 60)


if __name__ == "__main__":
    asyncio.run(run_benchmarks())
