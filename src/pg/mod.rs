//! Custom PostgreSQL wire protocol implementation.
//!
//! This module provides a high-performance PostgreSQL client that:
//! - Uses the binary protocol for parameters and results
//! - Caches prepared statements per connection
//! - Supports query pipelining
//!
//! Architecture:
//! - `protocol`: Low-level wire protocol encoding/decoding
//! - `connection`: Connection state machine and management
//! - `types`: PostgreSQL type encoding/decoding
//! - `statement`: Prepared statement cache
//! - `pool`: Connection pool with per-connection statement cache

pub mod protocol;
pub mod types;
pub mod connection;
pub mod statement;
pub mod pool;
pub mod error;
pub mod scram;

#[cfg(test)]
mod tests;

// Public API re-exports for library consumers
#[allow(unused_imports)]
pub use connection::PgConnection;
pub use pool::{PgPool, PgPoolConfig, PooledConnection};
#[allow(unused_imports)]
pub use error::{PgError, PgResult};
#[allow(unused_imports)]
pub use statement::{PreparedStatement, SharedColumns};
#[allow(unused_imports)]
pub use types::{Oid, PgValue};
