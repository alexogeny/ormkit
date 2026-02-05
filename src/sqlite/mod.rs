//! Custom SQLite driver implementation.
//!
//! This module provides a high-performance SQLite client using rusqlite
//! with tokio-rusqlite for async support.

pub mod connection;
pub mod error;
pub mod pool;
pub mod types;

#[cfg(test)]
mod tests;

// Public API re-exports for library consumers
#[allow(unused_imports)]
pub use connection::SqliteConnection;
#[allow(unused_imports)]
pub use error::{SqliteError, SqliteResult};
pub use pool::{SqlitePool, SqlitePoolConfig};
pub use types::SqliteValue;
