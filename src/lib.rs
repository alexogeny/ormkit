use pyo3::prelude::*;

mod error;
mod executor;
mod pg;
mod pool;
mod schema;
mod sqlite;

// No more sqlx types module - we use our own drivers

use executor::QueryResult;
use pool::{ConnectionPool, PoolConfig, Transaction};
use schema::{ColumnInfo, ConstraintInfo, IndexInfo, TableInfo};

/// Create a new database connection pool
#[pyfunction]
#[pyo3(signature = (url, min_connections=1, max_connections=10))]
fn create_pool<'py>(
    py: Python<'py>,
    url: String,
    min_connections: u32,
    max_connections: u32,
) -> PyResult<Bound<'py, PyAny>> {
    let config = PoolConfig {
        url,
        min_connections,
        max_connections,
    };

    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let pool = ConnectionPool::connect(config)
            .await
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(pool)
    })
}

/// OrmKit - A blazingly fast Python ORM powered by Rust
#[pymodule]
fn _ormkit(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(create_pool, m)?)?;
    m.add_class::<ConnectionPool>()?;
    m.add_class::<QueryResult>()?;
    m.add_class::<Transaction>()?;
    // Schema introspection types
    m.add_class::<ColumnInfo>()?;
    m.add_class::<IndexInfo>()?;
    m.add_class::<ConstraintInfo>()?;
    m.add_class::<TableInfo>()?;
    Ok(())
}
