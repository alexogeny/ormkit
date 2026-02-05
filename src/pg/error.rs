//! Error types for the PostgreSQL protocol implementation.

use std::fmt;
use std::io;

/// Result type for PostgreSQL operations.
pub type PgResult<T> = Result<T, PgError>;

/// Errors that can occur during PostgreSQL operations.
#[derive(Debug)]
pub enum PgError {
    /// I/O error during communication.
    Io(io::Error),

    /// Protocol error (unexpected message, invalid format, etc.).
    Protocol(String),

    /// Authentication failed.
    Auth(String),

    /// Server returned an error.
    Server {
        severity: String,
        code: String,
        message: String,
        detail: Option<String>,
        hint: Option<String>,
    },

    /// Type conversion error.
    Type(String),

    /// Connection is closed or in invalid state.
    ConnectionClosed,

    /// Statement not found in cache.
    StatementNotFound(String),

    /// Timeout waiting for response.
    Timeout,
}

impl fmt::Display for PgError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PgError::Io(e) => write!(f, "I/O error: {}", e),
            PgError::Protocol(msg) => write!(f, "Protocol error: {}", msg),
            PgError::Auth(msg) => write!(f, "Authentication failed: {}", msg),
            PgError::Server {
                severity,
                code,
                message,
                detail,
                hint,
            } => {
                write!(f, "{}: {} ({})", severity, message, code)?;
                if let Some(d) = detail {
                    write!(f, "\nDetail: {}", d)?;
                }
                if let Some(h) = hint {
                    write!(f, "\nHint: {}", h)?;
                }
                Ok(())
            }
            PgError::Type(msg) => write!(f, "Type error: {}", msg),
            PgError::ConnectionClosed => write!(f, "Connection is closed"),
            PgError::StatementNotFound(name) => {
                write!(f, "Prepared statement not found: {}", name)
            }
            PgError::Timeout => write!(f, "Operation timed out"),
        }
    }
}

impl std::error::Error for PgError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PgError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for PgError {
    fn from(e: io::Error) -> Self {
        PgError::Io(e)
    }
}
