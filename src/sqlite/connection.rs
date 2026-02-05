//! SQLite connection implementation.

use tokio_rusqlite::Connection;

use super::error::{SqliteError, SqliteResult};
use super::types::SqliteValue;

/// Result of a query execution.
#[derive(Debug)]
pub struct QueryResult {
    /// Column names
    pub columns: Vec<String>,
    /// Row data
    pub rows: Vec<Vec<SqliteValue>>,
    /// Rows affected (for INSERT/UPDATE/DELETE)
    pub rows_affected: u64,
}

impl QueryResult {
    fn new() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected: 0,
        }
    }
}

/// A SQLite connection.
pub struct SqliteConnection {
    conn: Connection,
    closed: bool,
}

impl SqliteConnection {
    /// Open a SQLite database.
    ///
    /// Supports:
    /// - `:memory:` for in-memory database
    /// - File path for disk-based database
    ///
    /// Automatically enables WAL mode for file-based databases (10-50x faster writes).
    pub async fn open(path: &str) -> SqliteResult<Self> {
        let path = path.to_string();
        let is_memory = path == ":memory:";
        let conn = if is_memory {
            Connection::open_in_memory().await?
        } else {
            Connection::open(&path).await?
        };

        // Enable performance pragmas for file-based databases
        if !is_memory {
            conn.call(|c| {
                c.execute_batch(
                    "PRAGMA journal_mode=WAL;
                     PRAGMA synchronous=NORMAL;
                     PRAGMA busy_timeout=5000;
                     PRAGMA cache_size=-64000;", // 64MB cache
                )?;
                Ok(())
            })
            .await?;
        }

        Ok(Self {
            conn,
            closed: false,
        })
    }

    /// Execute a query and return results.
    /// Uses prepared statement caching for repeated queries.
    pub async fn query(&self, sql: &str, params: &[SqliteValue]) -> SqliteResult<QueryResult> {
        if self.closed {
            return Err(SqliteError::ConnectionClosed);
        }

        let sql = sql.to_string();
        let params: Vec<SqliteValue> = params.to_vec();

        self.conn
            .call(move |conn| {
                // Use prepare_cached for O(1) lookup of repeated statements
                let mut stmt = conn.prepare_cached(&sql)?;

                // Get column names
                let columns: Vec<String> =
                    stmt.column_names().iter().map(|s| s.to_string()).collect();

                // Bind parameters
                let params_refs: Vec<&dyn rusqlite::ToSql> =
                    params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();

                // Execute and collect rows
                let mut rows_data = Vec::new();
                let mut rows = stmt.query(params_refs.as_slice())?;

                while let Some(row) = rows.next()? {
                    let mut row_values = Vec::with_capacity(columns.len());
                    for i in 0..columns.len() {
                        let value = row.get_ref(i)?;
                        row_values.push(SqliteValue::from_value_ref(value));
                    }
                    rows_data.push(row_values);
                }

                Ok(QueryResult {
                    columns,
                    rows: rows_data,
                    rows_affected: 0,
                })
            })
            .await
            .map_err(SqliteError::from)
    }

    /// Execute a statement that doesn't return rows.
    pub async fn execute(&self, sql: &str, params: &[SqliteValue]) -> SqliteResult<u64> {
        if self.closed {
            return Err(SqliteError::ConnectionClosed);
        }

        let sql = sql.to_string();
        let params: Vec<SqliteValue> = params.to_vec();

        self.conn
            .call(move |conn| {
                let params_refs: Vec<&dyn rusqlite::ToSql> =
                    params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();

                let rows_affected = conn.execute(&sql, params_refs.as_slice())?;
                Ok(rows_affected as u64)
            })
            .await
            .map_err(SqliteError::from)
    }

    /// Execute multiple statements (for DDL, etc.).
    pub async fn execute_batch(&self, sql: &str) -> SqliteResult<()> {
        if self.closed {
            return Err(SqliteError::ConnectionClosed);
        }

        let sql = sql.to_string();

        self.conn
            .call(move |conn| {
                conn.execute_batch(&sql)?;
                Ok(())
            })
            .await
            .map_err(SqliteError::from)
    }

    /// Close the connection.
    pub async fn close(mut self) -> SqliteResult<()> {
        self.closed = true;
        // tokio_rusqlite handles cleanup on drop
        Ok(())
    }

    /// Check if the connection is closed.
    pub fn is_closed(&self) -> bool {
        self.closed
    }
}
