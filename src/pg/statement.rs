//! Prepared statement management and caching.
//!
//! This module provides:
//! - `PreparedStatement`: Represents a server-side prepared statement
//! - `StatementCache`: O(1) LRU cache for prepared statements per connection

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use lru::LruCache;

use super::protocol::FieldDescription;
use super::types::Oid;

/// Shared column descriptions - wrapped in Arc to avoid cloning on every query.
/// This is a significant optimization since columns are read on every query execution.
pub type SharedColumns = Arc<Vec<FieldDescription>>;

// ============================================================================
// Prepared Statement
// ============================================================================

/// A prepared statement that has been parsed by PostgreSQL.
///
/// Prepared statements are created via the Parse message and can be
/// executed multiple times with different parameters via Bind + Execute.
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    /// Server-side name for this statement (e.g., "__fk_1")
    pub name: String,
    /// The SQL query text
    pub query: String,
    /// Parameter type OIDs (from ParameterDescription)
    pub param_types: Vec<Oid>,
    /// Result column descriptions (from RowDescription) - Arc-wrapped to avoid
    /// cloning on every query execution. This is a significant optimization
    /// since columns are accessed for every row decode operation.
    pub columns: SharedColumns,
}

impl PreparedStatement {
    /// Create a new prepared statement.
    pub fn new(name: String, query: String) -> Self {
        Self {
            name,
            query,
            param_types: Vec::new(),
            columns: Arc::new(Vec::new()),
        }
    }

    /// Set the parameter types after receiving ParameterDescription.
    pub fn set_param_types(&mut self, types: Vec<Oid>) {
        self.param_types = types;
    }

    /// Set the column descriptions after receiving RowDescription.
    /// Wraps the columns in Arc for efficient sharing across query results.
    pub fn set_columns(&mut self, columns: Vec<FieldDescription>) {
        self.columns = Arc::new(columns);
    }

    /// Check if this statement returns rows.
    pub fn returns_rows(&self) -> bool {
        !self.columns.is_empty()
    }
}

// ============================================================================
// Statement Cache (O(1) LRU)
// ============================================================================

/// O(1) LRU cache for prepared statements.
///
/// Each connection maintains its own statement cache to avoid re-parsing
/// frequently executed queries. Uses the `lru` crate for O(1) get/insert/evict.
///
/// Statements are stored as `Arc<PreparedStatement>` to avoid cloning the entire
/// statement (name, query, param_types, columns) on every cache hit. Instead,
/// only a cheap reference count increment is performed.
pub struct StatementCache {
    /// The LRU cache: query text → Arc<PreparedStatement>
    cache: LruCache<String, Arc<PreparedStatement>>,
    /// Counter for generating unique statement names
    next_id: AtomicU32,
}

impl StatementCache {
    /// Create a new statement cache with the given capacity.
    pub fn new(capacity: usize) -> Self {
        let cap = NonZeroUsize::new(capacity.max(1)).unwrap();
        Self {
            cache: LruCache::new(cap),
            next_id: AtomicU32::new(0),
        }
    }

    /// Get a cached prepared statement by query text.
    ///
    /// Returns `Some(Arc<PreparedStatement>)` if found. This is O(1).
    /// The Arc clone is cheap (reference count increment only).
    /// Note: Does NOT update LRU order (use `get_and_touch` for that).
    pub fn get(&self, query: &str) -> Option<Arc<PreparedStatement>> {
        self.cache.peek(query).map(Arc::clone)
    }

    /// Get a reference and update access order (marks as recently used).
    ///
    /// This is O(1) and updates LRU order.
    /// Returns Arc clone for cheap sharing.
    pub fn get_and_touch(&mut self, query: &str) -> Option<Arc<PreparedStatement>> {
        self.cache.get(query).map(Arc::clone)
    }

    /// Check if a query is cached (without cloning).
    #[inline]
    pub fn contains(&self, query: &str) -> bool {
        self.cache.contains(query)
    }

    /// Insert a prepared statement into the cache.
    ///
    /// If the cache is at capacity, the least recently used statement
    /// will be evicted. Returns the evicted statement name if any.
    ///
    /// This is O(1). The statement is wrapped in Arc for efficient sharing.
    pub fn insert(&mut self, query: String, statement: PreparedStatement) -> Option<String> {
        self.insert_arc(query, Arc::new(statement))
    }

    /// Insert an Arc-wrapped prepared statement into the cache.
    ///
    /// Use this when you already have an Arc<PreparedStatement>.
    pub fn insert_arc(
        &mut self,
        query: String,
        statement: Arc<PreparedStatement>,
    ) -> Option<String> {
        // Check if we'll evict (at capacity and this is a new key)
        let will_evict = self.cache.len() >= self.cache.cap().get() && !self.cache.contains(&query);

        // Get the LRU entry before inserting (will be evicted)
        let evicted = if will_evict {
            self.cache.peek_lru().map(|(_, stmt)| stmt.name.clone())
        } else {
            None
        };

        // Insert (or update) - this will evict LRU if needed
        self.cache.put(query, statement);

        evicted
    }

    /// Remove a statement from the cache.
    pub fn remove(&mut self, query: &str) -> Option<Arc<PreparedStatement>> {
        self.cache.pop(query)
    }

    /// Generate a unique statement name for this connection.
    pub fn next_statement_name(&mut self) -> String {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        format!("__fk_{}", id)
    }

    /// Get the number of cached statements.
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Check if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Clear all cached statements.
    ///
    /// Note: This does NOT close the statements on the server.
    /// Use `close_all` to properly close server-side statements.
    pub fn clear(&mut self) {
        self.cache.clear();
    }

    /// Get all statement names for closing on the server.
    pub fn statement_names(&self) -> Vec<String> {
        let mut names = Vec::with_capacity(self.cache.len());
        for (_, stmt) in self.cache.iter() {
            names.push(stmt.name.clone());
        }
        names
    }
}

impl Default for StatementCache {
    fn default() -> Self {
        // Default capacity of 100 statements per connection
        Self::new(100)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prepared_statement_creation() {
        let mut stmt = PreparedStatement::new("s1".to_string(), "SELECT $1".to_string());
        assert_eq!(stmt.name, "s1");
        assert_eq!(stmt.query, "SELECT $1");
        assert!(stmt.param_types.is_empty());
        assert!(stmt.columns.is_empty());

        stmt.set_param_types(vec![Oid::INT4]);
        assert_eq!(stmt.param_types.len(), 1);
        assert_eq!(stmt.param_types[0], Oid::INT4);
    }

    #[test]
    fn test_cache_basic_operations() {
        let mut cache = StatementCache::new(10);
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);

        let stmt = PreparedStatement::new("s1".to_string(), "SELECT 1".to_string());
        cache.insert("SELECT 1".to_string(), stmt);

        assert!(!cache.is_empty());
        assert_eq!(cache.len(), 1);

        let found = cache.get("SELECT 1");
        assert!(found.is_some());
        assert_eq!(found.unwrap().name, "s1");

        let not_found = cache.get("SELECT 2");
        assert!(not_found.is_none());
    }

    #[test]
    fn test_cache_lru_eviction() {
        let mut cache = StatementCache::new(2);

        // Insert 2 statements
        cache.insert(
            "q1".to_string(),
            PreparedStatement::new("s1".to_string(), "q1".to_string()),
        );
        cache.insert(
            "q2".to_string(),
            PreparedStatement::new("s2".to_string(), "q2".to_string()),
        );

        // Access q1 to make it recently used
        cache.get_and_touch("q1");

        // Insert q3, should evict q2 (now least recently used)
        let evicted = cache.insert(
            "q3".to_string(),
            PreparedStatement::new("s3".to_string(), "q3".to_string()),
        );

        assert_eq!(evicted, Some("s2".to_string())); // q2 was evicted
        assert!(cache.get("q1").is_some()); // Recently accessed
        assert!(cache.get("q2").is_none()); // Evicted
        assert!(cache.get("q3").is_some()); // Just inserted
    }

    #[test]
    fn test_cache_remove() {
        let mut cache = StatementCache::new(10);
        cache.insert(
            "q1".to_string(),
            PreparedStatement::new("s1".to_string(), "q1".to_string()),
        );

        let removed = cache.remove("q1");
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().name, "s1");
        assert!(cache.get("q1").is_none());
    }

    #[test]
    fn test_unique_statement_names() {
        let mut cache = StatementCache::new(10);

        let names: Vec<String> = (0..10).map(|_| cache.next_statement_name()).collect();

        // All names should be unique
        let unique: std::collections::HashSet<_> = names.iter().collect();
        assert_eq!(unique.len(), 10);

        // All should start with __fk_
        for name in &names {
            assert!(name.starts_with("__fk_"));
        }
    }
}
