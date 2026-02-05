//! SQLite error types.

use std::io;

pub type SqliteResult<T> = Result<T, SqliteError>;

/// SQLite-specific errors.
#[derive(Debug)]
pub enum SqliteError {
    /// I/O error
    Io(io::Error),
    /// SQLite error from rusqlite
    Sqlite(rusqlite::Error),
    /// Connection pool error
    Pool(String),
    /// Type conversion error
    Type(String),
    /// Connection closed
    ConnectionClosed,
}

impl std::fmt::Display for SqliteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SqliteError::Io(e) => write!(f, "I/O error: {}", e),
            SqliteError::Sqlite(e) => write!(f, "SQLite error: {}", e),
            SqliteError::Pool(e) => write!(f, "Pool error: {}", e),
            SqliteError::Type(e) => write!(f, "Type error: {}", e),
            SqliteError::ConnectionClosed => write!(f, "Connection closed"),
        }
    }
}

impl std::error::Error for SqliteError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SqliteError::Io(e) => Some(e),
            SqliteError::Sqlite(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for SqliteError {
    fn from(e: io::Error) -> Self {
        SqliteError::Io(e)
    }
}

impl From<rusqlite::Error> for SqliteError {
    fn from(e: rusqlite::Error) -> Self {
        SqliteError::Sqlite(e)
    }
}

impl From<tokio_rusqlite::Error> for SqliteError {
    fn from(e: tokio_rusqlite::Error) -> Self {
        SqliteError::Pool(e.to_string())
    }
}
