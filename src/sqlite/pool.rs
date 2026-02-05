//! SQLite connection pool.
//!
//! SQLite is single-writer, so we use a simple pool with one write connection
//! and multiple read connections for optimal performance.

use parking_lot::Mutex;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::connection::{QueryResult, SqliteConnection};
use super::error::{SqliteError, SqliteResult};
use super::types::SqliteValue;

/// Pool configuration.
#[derive(Debug, Clone)]
pub struct SqlitePoolConfig {
    /// Database path (or `:memory:`)
    pub path: String,
    /// Maximum number of read connections
    pub max_read_connections: u32,
}

impl SqlitePoolConfig {
    pub fn new(path: &str) -> Self {
        Self {
            path: path.to_string(),
            max_read_connections: 4,
        }
    }

    pub fn max_read_connections(mut self, max: u32) -> Self {
        self.max_read_connections = max;
        self
    }
}

/// A pooled connection.
pub struct PooledConnection {
    conn: Option<SqliteConnection>,
    pool: Arc<SqlitePoolInner>,
    _permit: OwnedSemaphorePermit,
}

impl PooledConnection {
    pub async fn query(&self, sql: &str, params: &[SqliteValue]) -> SqliteResult<QueryResult> {
        self.conn
            .as_ref()
            .ok_or(SqliteError::ConnectionClosed)?
            .query(sql, params)
            .await
    }

    pub async fn execute(&self, sql: &str, params: &[SqliteValue]) -> SqliteResult<u64> {
        self.conn
            .as_ref()
            .ok_or(SqliteError::ConnectionClosed)?
            .execute(sql, params)
            .await
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            if !conn.is_closed() {
                self.pool.idle_connections.lock().push(conn);
            }
        }
    }
}

struct SqlitePoolInner {
    config: SqlitePoolConfig,
    idle_connections: Mutex<Vec<SqliteConnection>>,
    semaphore: Arc<Semaphore>,
}

/// SQLite connection pool.
#[derive(Clone)]
pub struct SqlitePool {
    inner: Arc<SqlitePoolInner>,
}

impl SqlitePool {
    /// Create a new connection pool.
    pub async fn connect(config: SqlitePoolConfig) -> SqliteResult<Self> {
        let inner = Arc::new(SqlitePoolInner {
            semaphore: Arc::new(Semaphore::new(config.max_read_connections as usize)),
            config,
            idle_connections: Mutex::new(Vec::new()),
        });

        let pool = Self { inner };

        // Pre-create one connection
        let conn = pool.create_connection().await?;
        pool.inner.idle_connections.lock().push(conn);

        Ok(pool)
    }

    /// Acquire a connection from the pool.
    pub async fn acquire(&self) -> SqliteResult<PooledConnection> {
        let permit = self
            .inner
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| SqliteError::Pool("Pool closed".to_string()))?;

        let conn = {
            let mut idle = self.inner.idle_connections.lock();
            idle.pop()
        };

        let conn = match conn {
            Some(c) if !c.is_closed() => c,
            _ => self.create_connection().await?,
        };

        Ok(PooledConnection {
            conn: Some(conn),
            pool: Arc::clone(&self.inner),
            _permit: permit,
        })
    }

    /// Execute a query on a pooled connection.
    pub async fn query(&self, sql: &str, params: &[SqliteValue]) -> SqliteResult<QueryResult> {
        let conn = self.acquire().await?;
        conn.query(sql, params).await
    }

    /// Execute a statement on a pooled connection.
    pub async fn execute(&self, sql: &str, params: &[SqliteValue]) -> SqliteResult<u64> {
        let conn = self.acquire().await?;
        conn.execute(sql, params).await
    }

    /// Close all connections.
    pub async fn close(&self) {
        let connections = {
            let mut idle = self.inner.idle_connections.lock();
            std::mem::take(&mut *idle)
        };

        for conn in connections {
            let _ = conn.close().await;
        }
    }

    async fn create_connection(&self) -> SqliteResult<SqliteConnection> {
        SqliteConnection::open(&self.inner.config.path).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_pool_basic() {
        let config = SqlitePoolConfig::new(":memory:");
        let pool = SqlitePool::connect(config).await.unwrap();

        // Create table
        pool.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", &[])
            .await
            .unwrap();

        // Insert
        pool.execute(
            "INSERT INTO test (name) VALUES (?)",
            &[SqliteValue::Text("hello".to_string())],
        )
        .await
        .unwrap();

        // Query
        let result = pool.query("SELECT * FROM test", &[]).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.columns, vec!["id", "name"]);

        pool.close().await;
    }
}
