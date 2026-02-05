//! Error types for ForeignKey ORM.
//!
//! No external database driver dependencies.

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::PyErr;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ForeignKeyError {
    #[error("Database connection error: {0}")]
    ConnectionError(String),

    #[error("Query execution error: {0}")]
    QueryError(String),

    #[error("Type conversion error: {0}")]
    TypeError(String),

    #[error("Pool error: {0}")]
    PoolError(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Transaction error: {0}")]
    TransactionError(String),

    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
}

impl From<ForeignKeyError> for PyErr {
    fn from(err: ForeignKeyError) -> PyErr {
        match err {
            ForeignKeyError::TypeError(_) | ForeignKeyError::ConfigError(_) => {
                PyValueError::new_err(err.to_string())
            }
            _ => PyRuntimeError::new_err(err.to_string()),
        }
    }
}

pub type Result<T> = std::result::Result<T, ForeignKeyError>;
