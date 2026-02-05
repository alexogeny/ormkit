//! Custom SQLite driver implementation.
//!
//! This module provides a high-performance SQLite client using rusqlite
//! with tokio-rusqlite for async support.

pub mod connection;
pub mod pool;
pub mod error;
pub mod types;

#[cfg(test)]
mod tests;

// Public API re-exports for library consumers
#[allow(unused_imports)]
pub use connection::SqliteConnection;
pub use pool::{SqlitePool, SqlitePoolConfig};
#[allow(unused_imports)]
pub use error::{SqliteError, SqliteResult};
pub use types::SqliteValue;
