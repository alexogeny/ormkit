//! PostgreSQL connection pool.
//!
//! This module provides a connection pool built on top of our custom
//! PostgreSQL connection implementation.

use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::connection::{PgConfig, PgConnection, QueryResult};
use super::error::{PgError, PgResult};
use super::types::PgValue;

// ============================================================================
// Pool Configuration
// ============================================================================

/// Connection pool configuration.
#[derive(Debug, Clone)]
pub struct PgPoolConfig {
    /// Database connection URL
    pub url: String,
    /// Minimum number of connections
    pub min_connections: u32,
    /// Maximum number of connections
    pub max_connections: u32,
    /// Statement cache capacity per connection
    pub statement_cache_capacity: usize,
}

impl PgPoolConfig {
    /// Create a new pool configuration.
    pub fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
            min_connections: 1,
            max_connections: 10,
            statement_cache_capacity: 100,
        }
    }

    /// Set the minimum number of connections.
    pub fn min_connections(mut self, min: u32) -> Self {
        self.min_connections = min;
        self
    }

    /// Set the maximum number of connections.
    pub fn max_connections(mut self, max: u32) -> Self {
        self.max_connections = max;
        self
    }

    /// Set the statement cache capacity per connection.
    pub fn statement_cache_capacity(mut self, capacity: usize) -> Self {
        self.statement_cache_capacity = capacity;
        self
    }
}

// ============================================================================
// Pooled Connection
// ============================================================================

/// A connection checked out from the pool.
///
/// When dropped, the connection is returned to the pool.
pub struct PooledConnection {
    /// The actual connection (None when returned to pool)
    conn: Option<PgConnection>,
    /// Reference back to the pool
    pool: Arc<PgPoolInner>,
    /// Semaphore permit (controls pool size)
    _permit: OwnedSemaphorePermit,
}

impl PooledConnection {
    /// Execute a simple query.
    pub async fn simple_query(&mut self, query: &str) -> PgResult<Vec<QueryResult>> {
        self.conn
            .as_mut()
            .ok_or(PgError::ConnectionClosed)?
            .simple_query(query)
            .await
    }

    /// Execute a parameterized query.
    pub async fn query(&mut self, query: &str, params: &[PgValue]) -> PgResult<QueryResult> {
        self.conn
            .as_mut()
            .ok_or(PgError::ConnectionClosed)?
            .query(query, params)
            .await
    }

    /// Execute a parameterized query without syncing (for pipelining).
    ///
    /// Use sync() after all pipelined operations.
    pub async fn query_no_sync(
        &mut self,
        query: &str,
        params: &[PgValue],
    ) -> PgResult<QueryResult> {
        self.conn
            .as_mut()
            .ok_or(PgError::ConnectionClosed)?
            .query_no_sync(query, params)
            .await
    }

    /// Send sync and wait for server to catch up.
    pub async fn sync(&mut self) -> PgResult<()> {
        self.conn
            .as_mut()
            .ok_or(PgError::ConnectionClosed)?
            .sync()
            .await
    }

    /// Begin a transaction.
    pub async fn begin(&mut self) -> PgResult<()> {
        self.conn
            .as_mut()
            .ok_or(PgError::ConnectionClosed)?
            .begin()
            .await
    }

    /// Buffer BEGIN without flushing (deferred BEGIN).
    pub async fn begin_deferred(&mut self) -> PgResult<()> {
        self.conn
            .as_mut()
            .ok_or(PgError::ConnectionClosed)?
            .begin_deferred()
            .await
    }

    /// Execute query in transaction, optionally consuming deferred BEGIN.
    pub async fn query_in_transaction(
        &mut self,
        query: &str,
        params: &[PgValue],
        consume_begin: bool,
    ) -> PgResult<QueryResult> {
        self.conn
            .as_mut()
            .ok_or(PgError::ConnectionClosed)?
            .query_in_transaction(query, params, consume_begin)
            .await
    }

    /// Commit the transaction.
    pub async fn commit(&mut self) -> PgResult<()> {
        self.conn
            .as_mut()
            .ok_or(PgError::ConnectionClosed)?
            .commit()
            .await
    }

    /// Rollback the transaction.
    pub async fn rollback(&mut self) -> PgResult<()> {
        self.conn
            .as_mut()
            .ok_or(PgError::ConnectionClosed)?
            .rollback()
            .await
    }

    /// Check if the connection is healthy.
    pub fn is_healthy(&self) -> bool {
        self.conn.as_ref().map(|c| !c.is_closed()).unwrap_or(false)
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            // Only return healthy connections to the pool
            if !conn.is_closed() {
                let mut idle = self.pool.idle_connections.lock();
                idle.push(conn);
            }
        }
    }
}

// ============================================================================
// Pool Inner
// ============================================================================

/// Internal pool state.
struct PgPoolInner {
    /// Pool configuration
    config: PgPoolConfig,
    /// Idle connections waiting to be used
    idle_connections: Mutex<Vec<PgConnection>>,
    /// Semaphore to limit total connections
    semaphore: Arc<Semaphore>,
}

// ============================================================================
// Connection Pool
// ============================================================================

/// A PostgreSQL connection pool.
///
/// The pool maintains a set of reusable connections, each with its own
/// prepared statement cache.
#[derive(Clone)]
pub struct PgPool {
    inner: Arc<PgPoolInner>,
}

impl PgPool {
    /// Create a new connection pool.
    pub async fn connect(config: PgPoolConfig) -> PgResult<Self> {
        let inner = Arc::new(PgPoolInner {
            semaphore: Arc::new(Semaphore::new(config.max_connections as usize)),
            config,
            idle_connections: Mutex::new(Vec::new()),
        });

        let pool = Self { inner };

        // Pre-create minimum connections
        for _ in 0..pool.inner.config.min_connections {
            let conn = pool.create_connection().await?;
            pool.inner.idle_connections.lock().push(conn);
        }

        Ok(pool)
    }

    /// Get a connection from the pool.
    pub async fn acquire(&self) -> PgResult<PooledConnection> {
        // Acquire a permit (blocks if pool is exhausted)
        // Use Arc::clone() for clarity that this is a cheap reference count increment
        let permit = Arc::clone(&self.inner.semaphore)
            .acquire_owned()
            .await
            .map_err(|_| PgError::Protocol("Pool closed".to_string()))?;

        // Try to get an idle connection
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

    /// Execute a simple query on a pooled connection.
    pub async fn simple_query(&self, query: &str) -> PgResult<Vec<QueryResult>> {
        let mut conn = self.acquire().await?;
        conn.simple_query(query).await
    }

    /// Execute a parameterized query on a pooled connection.
    pub async fn query(&self, query: &str, params: &[PgValue]) -> PgResult<QueryResult> {
        let mut conn = self.acquire().await?;
        conn.query(query, params).await
    }

    /// Execute a query without returning results (INSERT, UPDATE, DELETE).
    pub async fn execute(&self, query: &str, params: &[PgValue]) -> PgResult<u64> {
        let result = self.query(query, params).await?;
        // Parse rows affected from command tag (e.g., "INSERT 0 5" -> 5)
        Ok(parse_rows_affected(&result.command_tag))
    }

    /// Close the pool and all connections.
    pub async fn close(&self) {
        // Drain and close all idle connections
        let connections = {
            let mut idle = self.inner.idle_connections.lock();
            std::mem::take(&mut *idle)
        };

        for mut conn in connections {
            let _ = conn.close().await;
        }
    }

    /// Get the current number of idle connections.
    pub fn idle_count(&self) -> usize {
        self.inner.idle_connections.lock().len()
    }

    /// Get the pool configuration.
    pub fn config(&self) -> &PgPoolConfig {
        &self.inner.config
    }

    /// Create a new connection with the pool's configuration.
    async fn create_connection(&self) -> PgResult<PgConnection> {
        let mut pg_config = PgConfig::from_url(&self.inner.config.url)?;
        pg_config.statement_cache_capacity = self.inner.config.statement_cache_capacity;
        PgConnection::connect_with_config(pg_config).await
    }
}

// ============================================================================
// Helper functions
// ============================================================================

/// Parse rows affected from a PostgreSQL command tag.
fn parse_rows_affected(tag: &str) -> u64 {
    // Common formats:
    // - "INSERT 0 5" -> 5 rows
    // - "UPDATE 3" -> 3 rows
    // - "DELETE 2" -> 2 rows
    // - "SELECT 10" -> 10 rows (though we typically don't use this)

    let parts: Vec<&str> = tag.split_whitespace().collect();
    match parts.as_slice() {
        ["INSERT", _, n] | ["UPDATE", n] | ["DELETE", n] | ["SELECT", n] => n.parse().unwrap_or(0),
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_rows_affected() {
        assert_eq!(parse_rows_affected("INSERT 0 5"), 5);
        assert_eq!(parse_rows_affected("UPDATE 3"), 3);
        assert_eq!(parse_rows_affected("DELETE 2"), 2);
        assert_eq!(parse_rows_affected("SELECT 10"), 10);
        assert_eq!(parse_rows_affected("UNKNOWN"), 0);
    }

    #[test]
    fn test_pool_config() {
        let config = PgPoolConfig::new("postgresql://localhost/test")
            .min_connections(2)
            .max_connections(20)
            .statement_cache_capacity(200);

        assert_eq!(config.min_connections, 2);
        assert_eq!(config.max_connections, 20);
        assert_eq!(config.statement_cache_capacity, 200);
    }
}
