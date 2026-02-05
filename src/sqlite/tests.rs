//! Tests for SQLite driver.

use super::*;

#[tokio::test]
async fn test_connection_open_memory() {
    let conn = SqliteConnection::open(":memory:").await.unwrap();
    assert!(!conn.is_closed());
}

#[tokio::test]
async fn test_execute_and_query() {
    let conn = SqliteConnection::open(":memory:").await.unwrap();

    // Create table
    conn.execute(
        "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, value REAL)",
        &[],
    )
    .await
    .unwrap();

    // Insert
    conn.execute(
        "INSERT INTO test (name, value) VALUES (?, ?)",
        &[
            SqliteValue::Text("hello".to_string()),
            SqliteValue::Real(1.5),
        ],
    )
    .await
    .unwrap();

    // Query
    let result = conn.query("SELECT * FROM test", &[]).await.unwrap();
    assert_eq!(result.columns, vec!["id", "name", "value"]);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][1], SqliteValue::Text("hello".to_string()));
}

#[tokio::test]
async fn test_null_values() {
    let conn = SqliteConnection::open(":memory:").await.unwrap();

    conn.execute("CREATE TABLE test (id INTEGER, name TEXT)", &[])
        .await
        .unwrap();

    conn.execute(
        "INSERT INTO test (id, name) VALUES (?, ?)",
        &[SqliteValue::Integer(1), SqliteValue::Null],
    )
    .await
    .unwrap();

    let result = conn.query("SELECT * FROM test", &[]).await.unwrap();
    assert_eq!(result.rows[0][1], SqliteValue::Null);
}

#[tokio::test]
async fn test_blob_values() {
    let conn = SqliteConnection::open(":memory:").await.unwrap();

    conn.execute("CREATE TABLE test (data BLOB)", &[])
        .await
        .unwrap();

    let data = vec![0xDE, 0xAD, 0xBE, 0xEF];
    conn.execute(
        "INSERT INTO test (data) VALUES (?)",
        &[SqliteValue::Blob(data.clone())],
    )
    .await
    .unwrap();

    let result = conn.query("SELECT * FROM test", &[]).await.unwrap();
    assert_eq!(result.rows[0][0], SqliteValue::Blob(data));
}

#[tokio::test]
async fn test_multiple_rows() {
    let conn = SqliteConnection::open(":memory:").await.unwrap();

    conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)", &[])
        .await
        .unwrap();

    for i in 0..100 {
        conn.execute(
            "INSERT INTO test (id) VALUES (?)",
            &[SqliteValue::Integer(i)],
        )
        .await
        .unwrap();
    }

    let result = conn.query("SELECT * FROM test", &[]).await.unwrap();
    assert_eq!(result.rows.len(), 100);
}
