//! Connection pool implementation using custom drivers.
//!
//! No sqlx. Pure Rust. Maximum performance.

use pyo3::prelude::*;
use smallvec::SmallVec;
use std::sync::Arc;

use crate::error::{ForeignKeyError, Result};
use crate::executor::{LazyRow, QueryResult, RowValue};
use crate::pg::{PgPool, PgPoolConfig, PgValue, PooledConnection as PgPooledConnection};
use crate::schema::{ColumnInfo, ConstraintInfo, IndexInfo, TableInfo};
use crate::sqlite::{SqlitePool, SqlitePoolConfig, SqliteValue};

pub struct PoolConfig {
    pub url: String,
    pub min_connections: u32,
    pub max_connections: u32,
}

#[derive(Clone)]
enum PoolInner {
    Postgres(PgPool),
    Sqlite(SqlitePool),
}

/// A database connection pool
#[pyclass]
#[derive(Clone)]
pub struct ConnectionPool {
    inner: Arc<PoolInner>,
    url: String,
}

impl ConnectionPool {
    pub async fn connect(config: PoolConfig) -> Result<Self> {
        let url = config.url.clone();

        if url.starts_with("postgresql://") || url.starts_with("postgres://") {
            let pg_config = PgPoolConfig::new(&url)
                .min_connections(config.min_connections)
                .max_connections(config.max_connections);

            let pool = PgPool::connect(pg_config)
                .await
                .map_err(|e| ForeignKeyError::ConnectionError(e.to_string()))?;

            Ok(Self {
                inner: Arc::new(PoolInner::Postgres(pool)),
                url,
            })
        } else if url.starts_with("sqlite://") || url.starts_with("sqlite:") {
            // Parse SQLite URL: sqlite://:memory: or sqlite://path/to/db
            let path = url
                .strip_prefix("sqlite://")
                .or_else(|| url.strip_prefix("sqlite:"))
                .unwrap_or(":memory:");

            let sqlite_config = SqlitePoolConfig::new(path)
                .max_read_connections(config.max_connections);

            let pool = SqlitePool::connect(sqlite_config)
                .await
                .map_err(|e| ForeignKeyError::ConnectionError(e.to_string()))?;

            Ok(Self {
                inner: Arc::new(PoolInner::Sqlite(pool)),
                url,
            })
        } else {
            Err(ForeignKeyError::ConfigError(format!(
                "Unsupported database URL scheme: {}",
                url
            )))
        }
    }

    /// Execute a raw SQL query and return results
    pub async fn execute_query(&self, sql: &str, params: Vec<SqlParam>) -> Result<QueryResult> {
        match self.inner.as_ref() {
            PoolInner::Postgres(pool) => self.execute_pg(pool, sql, params).await,
            PoolInner::Sqlite(pool) => self.execute_sqlite(pool, sql, params).await,
        }
    }

    /// Execute PostgreSQL query - optimized path
    async fn execute_pg(&self, pool: &PgPool, sql: &str, params: Vec<SqlParam>) -> Result<QueryResult> {
        let pg_params: Vec<PgValue> = params.into_iter().map(sql_param_to_pg).collect();

        let result = pool
            .query(sql, &pg_params)
            .await
            .map_err(|e| ForeignKeyError::QueryError(e.to_string()))?;

        // Convert to our QueryResult format - extract column names from Arc<Vec<FieldDescription>>
        let columns: Vec<String> = result.columns.iter().map(|f| f.name.clone()).collect();

        let lazy_rows: Vec<LazyRow> = result
            .rows
            .into_iter()
            .map(|row| {
                // Use SmallVec::from_iter for efficient inline storage (avoids heap for ≤16 columns)
                let values: SmallVec<[RowValue; 16]> = row.into_iter().map(pg_value_to_row).collect();
                LazyRow { values }
            })
            .collect();

        Ok(QueryResult::from_lazy(lazy_rows, columns))
    }

    /// Execute SQLite query - optimized path
    async fn execute_sqlite(&self, pool: &SqlitePool, sql: &str, params: Vec<SqlParam>) -> Result<QueryResult> {
        let sqlite_params: Vec<SqliteValue> = params.into_iter().map(sql_param_to_sqlite).collect();

        let result = pool
            .query(sql, &sqlite_params)
            .await
            .map_err(|e| ForeignKeyError::QueryError(e.to_string()))?;

        let columns = result.columns;

        let lazy_rows: Vec<LazyRow> = result
            .rows
            .into_iter()
            .map(|row| {
                // Use SmallVec::from_iter for efficient inline storage (avoids heap for ≤16 columns)
                let values: SmallVec<[RowValue; 16]> = row.into_iter().map(sqlite_value_to_row).collect();
                LazyRow { values }
            })
            .collect();

        Ok(QueryResult::from_lazy(lazy_rows, columns))
    }

    /// Execute a statement that doesn't return rows (INSERT, UPDATE, DELETE)
    pub async fn execute_statement(&self, sql: &str, params: Vec<SqlParam>) -> Result<u64> {
        match self.inner.as_ref() {
            PoolInner::Postgres(pool) => {
                let pg_params: Vec<PgValue> = params.into_iter().map(sql_param_to_pg).collect();
                pool.execute(sql, &pg_params)
                    .await
                    .map_err(|e| ForeignKeyError::QueryError(e.to_string()))
            }
            PoolInner::Sqlite(pool) => {
                let sqlite_params: Vec<SqliteValue> = params.into_iter().map(sql_param_to_sqlite).collect();
                pool.execute(sql, &sqlite_params)
                    .await
                    .map_err(|e| ForeignKeyError::QueryError(e.to_string()))
            }
        }
    }

    // ========================================================================
    // Schema Introspection Methods
    // ========================================================================

    /// Get all table names in the database
    pub async fn get_tables_impl(&self) -> Result<Vec<String>> {
        match self.inner.as_ref() {
            PoolInner::Postgres(pool) => {
                let result = pool
                    .query(crate::schema::PG_TABLES_QUERY, &[])
                    .await
                    .map_err(|e| ForeignKeyError::QueryError(e.to_string()))?;

                let tables: Vec<String> = result
                    .rows
                    .into_iter()
                    .filter_map(|row| {
                        row.into_iter().next().and_then(|v| match v {
                            PgValue::Text(s) => Some(s),
                            _ => None,
                        })
                    })
                    .collect();
                Ok(tables)
            }
            PoolInner::Sqlite(pool) => {
                let result = pool
                    .query(crate::schema::SQLITE_TABLES_QUERY, &[])
                    .await
                    .map_err(|e| ForeignKeyError::QueryError(e.to_string()))?;

                let tables: Vec<String> = result
                    .rows
                    .into_iter()
                    .filter_map(|row| {
                        row.into_iter().next().and_then(|v| match v {
                            SqliteValue::Text(s) => Some(s),
                            _ => None,
                        })
                    })
                    .collect();
                Ok(tables)
            }
        }
    }

    /// Get column information for a table
    pub async fn get_columns_impl(&self, table: &str) -> Result<Vec<ColumnInfo>> {
        match self.inner.as_ref() {
            PoolInner::Postgres(pool) => {
                let result = pool
                    .query(crate::schema::PG_COLUMNS_QUERY, &[PgValue::Text(table.to_string())])
                    .await
                    .map_err(|e| ForeignKeyError::QueryError(e.to_string()))?;

                let columns: Vec<ColumnInfo> = result
                    .rows
                    .into_iter()
                    .map(|row| {
                        let mut iter = row.into_iter();
                        let name = match iter.next() {
                            Some(PgValue::Text(s)) => s,
                            _ => String::new(),
                        };
                        let data_type = match iter.next() {
                            Some(PgValue::Text(s)) => s,
                            _ => String::new(),
                        };
                        let nullable = match iter.next() {
                            Some(PgValue::Bool(b)) => b,
                            _ => true,
                        };
                        let default = match iter.next() {
                            Some(PgValue::Text(s)) => Some(s),
                            Some(PgValue::Null) => None,
                            _ => None,
                        };
                        let is_primary_key = match iter.next() {
                            Some(PgValue::Bool(b)) => b,
                            _ => false,
                        };
                        ColumnInfo {
                            name,
                            data_type,
                            nullable,
                            default,
                            is_primary_key,
                        }
                    })
                    .collect();
                Ok(columns)
            }
            PoolInner::Sqlite(pool) => {
                // Use PRAGMA table_info for SQLite
                let pragma = crate::schema::sqlite_table_info_pragma(table);
                let result = pool
                    .query(&pragma, &[])
                    .await
                    .map_err(|e| ForeignKeyError::QueryError(e.to_string()))?;

                // PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
                let columns: Vec<ColumnInfo> = result
                    .rows
                    .into_iter()
                    .map(|row| {
                        let mut iter = row.into_iter();
                        let _cid = iter.next(); // Skip column id
                        let name = match iter.next() {
                            Some(SqliteValue::Text(s)) => s,
                            _ => String::new(),
                        };
                        let data_type = match iter.next() {
                            Some(SqliteValue::Text(s)) => s,
                            _ => String::new(),
                        };
                        let notnull = match iter.next() {
                            Some(SqliteValue::Integer(i)) => i != 0,
                            _ => false,
                        };
                        let default = match iter.next() {
                            Some(SqliteValue::Text(s)) => Some(s),
                            Some(SqliteValue::Null) => None,
                            _ => None,
                        };
                        let pk = match iter.next() {
                            Some(SqliteValue::Integer(i)) => i != 0,
                            _ => false,
                        };
                        ColumnInfo {
                            name,
                            data_type,
                            nullable: !notnull,
                            default,
                            is_primary_key: pk,
                        }
                    })
                    .collect();
                Ok(columns)
            }
        }
    }

    /// Get index information for a table
    pub async fn get_indexes_impl(&self, table: &str) -> Result<Vec<IndexInfo>> {
        match self.inner.as_ref() {
            PoolInner::Postgres(pool) => {
                let result = pool
                    .query(crate::schema::PG_INDEXES_QUERY, &[PgValue::Text(table.to_string())])
                    .await
                    .map_err(|e| ForeignKeyError::QueryError(e.to_string()))?;

                let indexes: Vec<IndexInfo> = result
                    .rows
                    .into_iter()
                    .map(|row| {
                        let mut iter = row.into_iter();
                        let name = match iter.next() {
                            Some(PgValue::Text(s)) => s,
                            _ => String::new(),
                        };
                        // Columns come as an array - parse from text representation
                        let columns: Vec<String> = match iter.next() {
                            Some(PgValue::Text(s)) => {
                                // PostgreSQL array format: {col1,col2}
                                s.trim_matches(|c| c == '{' || c == '}')
                                    .split(',')
                                    .map(|s| s.trim().to_string())
                                    .filter(|s| !s.is_empty())
                                    .collect()
                            }
                            _ => vec![],
                        };
                        let unique = match iter.next() {
                            Some(PgValue::Bool(b)) => b,
                            _ => false,
                        };
                        IndexInfo {
                            name,
                            columns,
                            unique,
                        }
                    })
                    .collect();
                Ok(indexes)
            }
            PoolInner::Sqlite(pool) => {
                // Get list of indexes
                let index_list_pragma = crate::schema::sqlite_index_list_pragma(table);
                let result = pool
                    .query(&index_list_pragma, &[])
                    .await
                    .map_err(|e| ForeignKeyError::QueryError(e.to_string()))?;

                // PRAGMA index_list returns: seq, name, unique, origin, partial
                let mut indexes = Vec::new();
                for row in result.rows {
                    let mut iter = row.into_iter();
                    let _seq = iter.next();
                    let name = match iter.next() {
                        Some(SqliteValue::Text(s)) => s,
                        _ => continue,
                    };
                    let unique = match iter.next() {
                        Some(SqliteValue::Integer(i)) => i != 0,
                        _ => false,
                    };
                    let origin = match iter.next() {
                        Some(SqliteValue::Text(s)) => s,
                        _ => String::new(),
                    };

                    // Skip auto-generated indexes for primary keys
                    if origin == "pk" {
                        continue;
                    }

                    // Get columns for this index
                    let index_info_pragma = crate::schema::sqlite_index_info_pragma(&name);
                    let col_result = pool
                        .query(&index_info_pragma, &[])
                        .await
                        .map_err(|e| ForeignKeyError::QueryError(e.to_string()))?;

                    // PRAGMA index_info returns: seqno, cid, name
                    let columns: Vec<String> = col_result
                        .rows
                        .into_iter()
                        .filter_map(|row| {
                            let mut iter = row.into_iter();
                            let _seqno = iter.next();
                            let _cid = iter.next();
                            match iter.next() {
                                Some(SqliteValue::Text(s)) => Some(s),
                                _ => None,
                            }
                        })
                        .collect();

                    indexes.push(IndexInfo {
                        name,
                        columns,
                        unique,
                    });
                }
                Ok(indexes)
            }
        }
    }

    /// Get constraint information for a table
    pub async fn get_constraints_impl(&self, table: &str) -> Result<Vec<ConstraintInfo>> {
        match self.inner.as_ref() {
            PoolInner::Postgres(pool) => {
                let result = pool
                    .query(crate::schema::PG_CONSTRAINTS_QUERY, &[PgValue::Text(table.to_string())])
                    .await
                    .map_err(|e| ForeignKeyError::QueryError(e.to_string()))?;

                let constraints: Vec<ConstraintInfo> = result
                    .rows
                    .into_iter()
                    .map(|row| {
                        let mut iter = row.into_iter();
                        let name = match iter.next() {
                            Some(PgValue::Text(s)) => s,
                            _ => String::new(),
                        };
                        let constraint_type = match iter.next() {
                            Some(PgValue::Text(s)) => s,
                            _ => String::new(),
                        };
                        // Columns come as an array
                        let columns: Vec<String> = match iter.next() {
                            Some(PgValue::Text(s)) => {
                                s.trim_matches(|c| c == '{' || c == '}')
                                    .split(',')
                                    .map(|s| s.trim().to_string())
                                    .filter(|s| !s.is_empty())
                                    .collect()
                            }
                            _ => vec![],
                        };
                        let references_table = match iter.next() {
                            Some(PgValue::Text(s)) => Some(s),
                            Some(PgValue::Null) => None,
                            _ => None,
                        };
                        let references_column = match iter.next() {
                            Some(PgValue::Text(s)) => Some(s),
                            Some(PgValue::Null) => None,
                            _ => None,
                        };
                        ConstraintInfo {
                            name,
                            constraint_type,
                            columns,
                            references_table,
                            references_column,
                        }
                    })
                    .collect();
                Ok(constraints)
            }
            PoolInner::Sqlite(pool) => {
                // SQLite: Get foreign keys using PRAGMA foreign_key_list
                let fk_pragma = crate::schema::sqlite_foreign_key_list_pragma(table);
                let result = pool
                    .query(&fk_pragma, &[])
                    .await
                    .map_err(|e| ForeignKeyError::QueryError(e.to_string()))?;

                // PRAGMA foreign_key_list returns: id, seq, table, from, to, on_update, on_delete, match
                let mut constraints = Vec::new();
                let mut current_id: Option<i64> = None;
                let mut current_constraint: Option<ConstraintInfo> = None;

                for row in result.rows {
                    let mut iter = row.into_iter();
                    let id = match iter.next() {
                        Some(SqliteValue::Integer(i)) => i,
                        _ => continue,
                    };
                    let _seq = iter.next();
                    let ref_table = match iter.next() {
                        Some(SqliteValue::Text(s)) => s,
                        _ => continue,
                    };
                    let from_col = match iter.next() {
                        Some(SqliteValue::Text(s)) => s,
                        _ => continue,
                    };
                    let to_col = match iter.next() {
                        Some(SqliteValue::Text(s)) => s,
                        _ => continue,
                    };

                    if current_id != Some(id) {
                        if let Some(c) = current_constraint.take() {
                            constraints.push(c);
                        }
                        current_id = Some(id);
                        current_constraint = Some(ConstraintInfo {
                            name: format!("fk_{}_{}_{}", table, ref_table, id),
                            constraint_type: "FOREIGN KEY".to_string(),
                            columns: vec![from_col],
                            references_table: Some(ref_table),
                            references_column: Some(to_col),
                        });
                    } else if let Some(ref mut c) = current_constraint {
                        c.columns.push(from_col);
                    }
                }

                if let Some(c) = current_constraint {
                    constraints.push(c);
                }

                // Also detect primary key constraint from table_info
                let pragma = crate::schema::sqlite_table_info_pragma(table);
                let pk_result = pool
                    .query(&pragma, &[])
                    .await
                    .map_err(|e| ForeignKeyError::QueryError(e.to_string()))?;

                let pk_columns: Vec<String> = pk_result
                    .rows
                    .into_iter()
                    .filter_map(|row| {
                        let mut iter = row.into_iter();
                        let _cid = iter.next();
                        let name = match iter.next() {
                            Some(SqliteValue::Text(s)) => s,
                            _ => return None,
                        };
                        let _type = iter.next();
                        let _notnull = iter.next();
                        let _default = iter.next();
                        let pk = match iter.next() {
                            Some(SqliteValue::Integer(i)) => i,
                            _ => 0,
                        };
                        if pk > 0 {
                            Some(name)
                        } else {
                            None
                        }
                    })
                    .collect();

                if !pk_columns.is_empty() {
                    constraints.insert(
                        0,
                        ConstraintInfo {
                            name: format!("{}_pkey", table),
                            constraint_type: "PRIMARY KEY".to_string(),
                            columns: pk_columns,
                            references_table: None,
                            references_column: None,
                        },
                    );
                }

                Ok(constraints)
            }
        }
    }

    /// Get full table information including columns, indexes, and constraints
    pub async fn get_table_info_impl(&self, table: &str) -> Result<TableInfo> {
        let columns = self.get_columns_impl(table).await?;
        let indexes = self.get_indexes_impl(table).await?;
        let constraints = self.get_constraints_impl(table).await?;

        Ok(TableInfo {
            name: table.to_string(),
            columns,
            indexes,
            constraints,
        })
    }
}

// ============================================================================
// Type Conversions - Optimized for speed
// ============================================================================

/// Hex lookup table for fast byte-to-hex conversion
const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

/// Fast UUID formatting using pre-allocated buffer and lookup table.
/// This is significantly faster than format!() with 16 specifiers.
#[inline(always)]
fn format_uuid(u: &[u8; 16]) -> String {
    // UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx (36 chars)
    let mut buf = [0u8; 36];
    let mut pos = 0;

    // Helper to write a hex byte
    #[inline(always)]
    fn write_hex(buf: &mut [u8], pos: &mut usize, byte: u8) {
        buf[*pos] = HEX_CHARS[(byte >> 4) as usize];
        buf[*pos + 1] = HEX_CHARS[(byte & 0x0f) as usize];
        *pos += 2;
    }

    // xxxxxxxx (bytes 0-3)
    for &b in &u[0..4] {
        write_hex(&mut buf, &mut pos, b);
    }
    buf[pos] = b'-';
    pos += 1;

    // xxxx (bytes 4-5)
    for &b in &u[4..6] {
        write_hex(&mut buf, &mut pos, b);
    }
    buf[pos] = b'-';
    pos += 1;

    // xxxx (bytes 6-7)
    for &b in &u[6..8] {
        write_hex(&mut buf, &mut pos, b);
    }
    buf[pos] = b'-';
    pos += 1;

    // xxxx (bytes 8-9)
    for &b in &u[8..10] {
        write_hex(&mut buf, &mut pos, b);
    }
    buf[pos] = b'-';
    pos += 1;

    // xxxxxxxxxxxx (bytes 10-15)
    for &b in &u[10..16] {
        write_hex(&mut buf, &mut pos, b);
    }

    // SAFETY: buf contains only valid ASCII hex digits and hyphens
    // Optimized: use to_owned() directly from str instead of going through to_vec()
    unsafe { std::str::from_utf8_unchecked(&buf).to_owned() }
}

/// Convert PgValue to RowValue (hot path)
#[inline(always)]
fn pg_value_to_row(value: PgValue) -> RowValue {
    match value {
        PgValue::Null => RowValue::Null,
        PgValue::Bool(b) => RowValue::Bool(b),
        PgValue::Int2(i) => RowValue::Int(i as i64),
        PgValue::Int4(i) => RowValue::Int(i as i64),
        PgValue::Int8(i) => RowValue::Int(i),
        PgValue::Float4(f) => RowValue::Float(f as f64),
        PgValue::Float8(f) => RowValue::Float(f),
        PgValue::Text(s) => RowValue::String(s),
        PgValue::Bytea(b) => RowValue::Bytes(b),
        PgValue::Uuid(u) => {
            // Fast UUID formatting using lookup table
            RowValue::String(format_uuid(&u))
        }
        PgValue::Timestamp(ts) => {
            // Convert PostgreSQL timestamp (microseconds since 2000-01-01) to string
            // For now, just return as integer - can improve later
            RowValue::Int(ts)
        }
        PgValue::Date(d) => RowValue::Int(d as i64),
        PgValue::Time(t) => RowValue::Int(t),
        PgValue::Json(s) => {
            // Parse JSON string into serde_json::Value for proper Python conversion
            match serde_json::from_str(&s) {
                Ok(json) => RowValue::Json(json),
                Err(_) => RowValue::String(s), // Fallback to string if parse fails
            }
        }
        PgValue::Raw { data, .. } => RowValue::Bytes(data),
    }
}

/// Convert SqliteValue to RowValue (hot path)
#[inline(always)]
fn sqlite_value_to_row(value: SqliteValue) -> RowValue {
    match value {
        SqliteValue::Null => RowValue::Null,
        SqliteValue::Integer(i) => RowValue::Int(i),
        SqliteValue::Real(f) => RowValue::Float(f),
        SqliteValue::Text(s) => RowValue::String(s),
        SqliteValue::Blob(b) => RowValue::Bytes(b),
    }
}

/// Convert SqlParam to PgValue
#[inline]
fn sql_param_to_pg(param: SqlParam) -> PgValue {
    match param {
        SqlParam::Null => PgValue::Null,
        SqlParam::Bool(b) => PgValue::Bool(b),
        SqlParam::Int(i) => PgValue::Int8(i),
        SqlParam::Float(f) => PgValue::Float8(f),
        SqlParam::String(s) => PgValue::Text(s),
        SqlParam::Bytes(b) => PgValue::Bytea(b),
        SqlParam::Json(s) => PgValue::Json(s),
    }
}

/// Convert SqlParam to SqliteValue
#[inline]
fn sql_param_to_sqlite(param: SqlParam) -> SqliteValue {
    match param {
        SqlParam::Null => SqliteValue::Null,
        SqlParam::Bool(b) => SqliteValue::Integer(if b { 1 } else { 0 }),
        SqlParam::Int(i) => SqliteValue::Integer(i),
        SqlParam::Float(f) => SqliteValue::Real(f),
        SqlParam::String(s) => SqliteValue::Text(s),
        SqlParam::Bytes(b) => SqliteValue::Blob(b),
        // SQLite stores JSON as TEXT
        SqlParam::Json(s) => SqliteValue::Text(s),
    }
}

// ============================================================================
// PyO3 Interface
// ============================================================================

#[pymethods]
impl ConnectionPool {
    /// Get the database URL (with password masked)
    #[getter]
    fn url(&self) -> String {
        // Mask password in URL for security
        if let Some(at_pos) = self.url.find('@') {
            if let Some(colon_pos) = self.url[..at_pos].rfind(':') {
                let scheme_end = self.url.find("://").map(|p| p + 3).unwrap_or(0);
                if colon_pos > scheme_end {
                    return format!(
                        "{}:****{}",
                        &self.url[..colon_pos],
                        &self.url[at_pos..]
                    );
                }
            }
        }
        self.url.clone()
    }

    /// Check if this is a PostgreSQL connection
    fn is_postgres(&self) -> bool {
        matches!(self.inner.as_ref(), PoolInner::Postgres(_))
    }

    /// Check if this is a SQLite connection
    fn is_sqlite(&self) -> bool {
        matches!(self.inner.as_ref(), PoolInner::Sqlite(_))
    }

    /// Execute a SQL query and return results
    #[pyo3(signature = (sql, params=None))]
    fn execute<'py>(&self, py: Python<'py>, sql: String, params: Option<Vec<PyObject>>) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.clone();
        let sql_params = convert_py_params(py, params.unwrap_or_default())?;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = pool.execute_query(&sql, sql_params).await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(result)
        })
    }

    /// Execute a statement that doesn't return rows
    #[pyo3(signature = (sql, params=None))]
    fn execute_statement_py<'py>(&self, py: Python<'py>, sql: String, params: Option<Vec<PyObject>>) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.clone();
        let sql_params = convert_py_params(py, params.unwrap_or_default())?;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let rows_affected = pool.execute_statement(&sql, sql_params).await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(rows_affected)
        })
    }

    /// Start a new transaction - returns a Transaction context manager
    fn transaction<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pool_inner = Arc::clone(&self.inner);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            match pool_inner.as_ref() {
                PoolInner::Postgres(pool) => {
                    let mut conn = pool.acquire().await
                        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

                    // Buffer BEGIN without flushing - will be sent with first query
                    conn.begin_deferred().await
                        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

                    Ok(Transaction {
                        conn: Arc::new(tokio::sync::Mutex::new(Some(conn))),
                        begun: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                    })
                }
                PoolInner::Sqlite(_) => {
                    Err(pyo3::exceptions::PyRuntimeError::new_err(
                        "SQLite transactions not yet implemented"
                    ))
                }
            }
        })
    }

    /// Close the connection pool
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            match pool.inner.as_ref() {
                PoolInner::Postgres(p) => p.close().await,
                PoolInner::Sqlite(p) => p.close().await,
            }
            Ok(())
        })
    }

    // ========================================================================
    // Schema Introspection - Python Interface
    // ========================================================================

    /// Get all table names in the database
    fn get_tables<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let tables = pool
                .get_tables_impl()
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(tables)
        })
    }

    /// Get column information for a table
    fn get_columns<'py>(&self, py: Python<'py>, table: String) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let columns = pool
                .get_columns_impl(&table)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(columns)
        })
    }

    /// Get index information for a table
    fn get_indexes<'py>(&self, py: Python<'py>, table: String) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let indexes = pool
                .get_indexes_impl(&table)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(indexes)
        })
    }

    /// Get constraint information for a table
    fn get_constraints<'py>(&self, py: Python<'py>, table: String) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let constraints = pool
                .get_constraints_impl(&table)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(constraints)
        })
    }

    /// Get full table information (columns, indexes, constraints)
    fn get_table_info<'py>(&self, py: Python<'py>, table: String) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let info = pool
                .get_table_info_impl(&table)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(info)
        })
    }
}

/// SQL parameter types
#[derive(Clone, Debug)]
pub enum SqlParam {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    /// JSON value - pre-serialized string from Python dict/list
    /// We serialize directly to string to avoid the intermediate serde_json::Value
    Json(String),
}

// ============================================================================
// Transaction Support
// ============================================================================

/// A database transaction context manager.
///
/// This is used as an async context manager in Python:
/// ```python
/// async with pool.transaction() as tx:
///     await tx.execute("INSERT ...")
///     await tx.execute("UPDATE ...")
/// ```
///
/// Performance optimization: Uses deferred BEGIN - the BEGIN is buffered
/// but not sent until the first query, saving a round trip. This means:
/// - transaction() returns immediately (no network I/O)
/// - First execute() sends BEGIN + query together
/// - Subsequent queries skip ReadyForQuery wait (use Flush not Sync)
/// - Only COMMIT/ROLLBACK sends Sync to finalize
#[pyclass]
pub struct Transaction {
    /// The dedicated connection for this transaction
    conn: Arc<tokio::sync::Mutex<Option<PgPooledConnection>>>,
    /// Whether BEGIN response has been consumed
    begun: Arc<std::sync::atomic::AtomicBool>,
}

#[pymethods]
impl Transaction {
    /// Enter the async context manager
    fn __aenter__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        // Return self - BEGIN is buffered but not sent yet
        let tx = Transaction {
            conn: Arc::clone(&slf.conn),
            begun: Arc::clone(&slf.begun),
        };
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(tx) })
    }

    /// Exit the async context manager - commits or rolls back
    #[pyo3(signature = (exc_type, _exc_val, _exc_tb))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        exc_type: Option<PyObject>,
        _exc_val: Option<PyObject>,
        _exc_tb: Option<PyObject>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let has_exception = exc_type.is_some();
        let conn = Arc::clone(&self.conn);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = conn.lock().await;
            if let Some(ref mut c) = *guard {
                if has_exception {
                    // Rollback on exception - includes Sync
                    let _ = c.rollback().await;
                } else {
                    // Commit - includes Sync
                    c.commit().await
                        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(
                            format!("Failed to commit: {}", e)
                        ))?;
                }
            }
            // Take the connection out of the Option so it gets dropped,
            // returning the semaphore permit to the pool
            let _ = guard.take();
            // Return False to not suppress exceptions
            Ok(false)
        })
    }

    /// Execute a query within the transaction
    ///
    /// First call sends buffered BEGIN + query together (deferred BEGIN).
    /// Subsequent calls use query_no_sync() - skips ReadyForQuery wait.
    #[pyo3(signature = (sql, params=None))]
    fn execute<'py>(&self, py: Python<'py>, sql: String, params: Option<Vec<PyObject>>) -> PyResult<Bound<'py, PyAny>> {
        let sql_params = convert_py_params(py, params.unwrap_or_default())?;
        let conn = Arc::clone(&self.conn);
        let begun = Arc::clone(&self.begun);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = conn.lock().await;
            let c = guard.as_mut()
                .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Transaction not active"))?;

            // On first query, we need to consume BEGIN response after flush
            let is_first = !begun.swap(true, std::sync::atomic::Ordering::SeqCst);

            let pg_params: Vec<PgValue> = sql_params.into_iter().map(sql_param_to_pg).collect();

            // Execute query, consuming deferred BEGIN on first call
            let result = c.query_in_transaction(&sql, &pg_params, is_first).await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

            // Convert to our QueryResult format - extract column names from Arc<Vec<FieldDescription>>
            let columns: Vec<String> = result.columns.iter().map(|f| f.name.clone()).collect();

            let lazy_rows: Vec<LazyRow> = result
                .rows
                .into_iter()
                .map(|row| {
                    let values: SmallVec<[RowValue; 16]> = row.into_iter().map(pg_value_to_row).collect();
                    LazyRow { values }
                })
                .collect();

            Ok(QueryResult::from_lazy(lazy_rows, columns))
        })
    }

    /// Execute many queries in a pipelined fashion (for bulk operations).
    ///
    /// This is much faster than calling execute() in a loop because it
    /// sends all queries without waiting for responses, then collects
    /// all results at once.
    #[pyo3(signature = (sql, params_list))]
    fn execute_many<'py>(&self, py: Python<'py>, sql: String, params_list: Vec<Vec<PyObject>>) -> PyResult<Bound<'py, PyAny>> {
        // Convert all params upfront
        let all_params: Vec<Vec<SqlParam>> = params_list
            .into_iter()
            .map(|params| convert_py_params(py, params))
            .collect::<PyResult<Vec<_>>>()?;

        let conn = Arc::clone(&self.conn);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = conn.lock().await;
            let c = guard.as_mut()
                .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Transaction not active"))?;

            let count = all_params.len();

            // Send all queries without syncing
            let mut results = Vec::with_capacity(count);
            for params in all_params {
                let pg_params: Vec<PgValue> = params.into_iter().map(sql_param_to_pg).collect();
                let result = c.query_no_sync(&sql, &pg_params).await
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                results.push(result);
            }

            // Sync to ensure all commands are processed
            c.sync().await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

            Ok(count as u64)
        })
    }
}

/// Convert Python objects to SQL parameters using type-dispatch.
///
/// This uses direct Python type object comparison instead of sequential extract() attempts,
/// which is significantly faster (single type check vs up to 6 extract attempts).
fn convert_py_params(py: Python<'_>, params: Vec<PyObject>) -> PyResult<Vec<SqlParam>> {
    use pyo3::types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString};

    let mut result = Vec::with_capacity(params.len());

    for param in params {
        let bound = param.bind(py);

        if bound.is_none() {
            result.push(SqlParam::Null);
            continue;
        }

        // Get the type once for fast dispatch
        // Note: PyBool must be checked before PyInt because bool is a subclass of int in Python
        if bound.is_instance_of::<PyBool>() {
            // Use extract for bool since we need the actual value
            result.push(SqlParam::Bool(bound.extract()?));
        } else if bound.is_instance_of::<PyInt>() {
            result.push(SqlParam::Int(bound.extract()?));
        } else if bound.is_instance_of::<PyFloat>() {
            result.push(SqlParam::Float(bound.extract()?));
        } else if bound.is_instance_of::<PyString>() {
            result.push(SqlParam::String(bound.extract()?));
        } else if bound.is_instance_of::<PyBytes>() {
            result.push(SqlParam::Bytes(bound.extract()?));
        } else if bound.is_instance_of::<PyDict>() || bound.is_instance_of::<PyList>() {
            // Convert Python dict/list to JSON string via serde_json::Value
            // Two steps: pythonize (Python → Value) then to_vec (Value → bytes → String)
            // Using to_vec is faster than to_string as it skips UTF-8 validation
            let json_value: serde_json::Value = pythonize::depythonize(bound)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(
                    format!("Failed to serialize to JSON: {}", e)
                ))?;
            // Use to_vec for speed, then unsafe convert to String (JSON is always valid UTF-8)
            let json_bytes = serde_json::to_vec(&json_value)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(
                    format!("Failed to serialize JSON: {}", e)
                ))?;
            // SAFETY: serde_json always produces valid UTF-8
            let json_string = unsafe { String::from_utf8_unchecked(json_bytes) };
            result.push(SqlParam::Json(json_string));
        } else {
            // Fallback: convert to string representation
            let s = bound.str()?.to_string();
            result.push(SqlParam::String(s));
        }
    }

    Ok(result)
}
