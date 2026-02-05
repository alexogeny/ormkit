//! Database schema introspection for migrations.
//!
//! Provides functions to query database metadata:
//! - Table names
//! - Column information
//! - Index information
//! - Constraint information

use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

/// Information about a database column.
#[pyclass]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColumnInfo {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub data_type: String,
    #[pyo3(get)]
    pub nullable: bool,
    #[pyo3(get)]
    pub default: Option<String>,
    #[pyo3(get)]
    pub is_primary_key: bool,
}

#[pymethods]
impl ColumnInfo {
    fn __repr__(&self) -> String {
        format!(
            "ColumnInfo(name='{}', type='{}', nullable={}, pk={})",
            self.name, self.data_type, self.nullable, self.is_primary_key
        )
    }
}

/// Information about a database index.
#[pyclass]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexInfo {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub columns: Vec<String>,
    #[pyo3(get)]
    pub unique: bool,
}

#[pymethods]
impl IndexInfo {
    fn __repr__(&self) -> String {
        format!(
            "IndexInfo(name='{}', columns={:?}, unique={})",
            self.name, self.columns, self.unique
        )
    }
}

/// Information about a database constraint.
#[pyclass]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConstraintInfo {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub constraint_type: String, // "PRIMARY KEY", "FOREIGN KEY", "UNIQUE", "CHECK"
    #[pyo3(get)]
    pub columns: Vec<String>,
    #[pyo3(get)]
    pub references_table: Option<String>, // For FK: referenced table
    #[pyo3(get)]
    pub references_column: Option<String>, // For FK: referenced column
}

#[pymethods]
impl ConstraintInfo {
    fn __repr__(&self) -> String {
        format!(
            "ConstraintInfo(name='{}', type='{}', columns={:?})",
            self.name, self.constraint_type, self.columns
        )
    }
}

/// Information about a database table.
#[pyclass]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableInfo {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub columns: Vec<ColumnInfo>,
    #[pyo3(get)]
    pub indexes: Vec<IndexInfo>,
    #[pyo3(get)]
    pub constraints: Vec<ConstraintInfo>,
}

#[pymethods]
impl TableInfo {
    fn __repr__(&self) -> String {
        format!(
            "TableInfo(name='{}', {} columns, {} indexes)",
            self.name,
            self.columns.len(),
            self.indexes.len()
        )
    }
}

// ============================================================================
// PostgreSQL Schema Introspection
// ============================================================================

/// Query to get all table names in PostgreSQL
pub const PG_TABLES_QUERY: &str = r#"
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_type = 'BASE TABLE'
ORDER BY table_name
"#;

/// Query to get column information for a PostgreSQL table
pub const PG_COLUMNS_QUERY: &str = r#"
SELECT
    c.column_name as name,
    c.data_type,
    c.is_nullable = 'YES' as nullable,
    c.column_default as default_value,
    COALESCE(
        (SELECT true FROM information_schema.table_constraints tc
         JOIN information_schema.key_column_usage kcu
           ON tc.constraint_name = kcu.constraint_name
         WHERE tc.table_name = c.table_name
           AND tc.constraint_type = 'PRIMARY KEY'
           AND kcu.column_name = c.column_name
         LIMIT 1),
        false
    ) as is_primary_key
FROM information_schema.columns c
WHERE c.table_schema = 'public'
  AND c.table_name = $1
ORDER BY c.ordinal_position
"#;

/// Query to get index information for a PostgreSQL table
pub const PG_INDEXES_QUERY: &str = r#"
SELECT
    i.relname as index_name,
    array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)) as columns,
    ix.indisunique as is_unique
FROM pg_class t
JOIN pg_index ix ON t.oid = ix.indrelid
JOIN pg_class i ON i.oid = ix.indexrelid
JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
WHERE t.relkind = 'r'
  AND t.relname = $1
  AND NOT ix.indisprimary
GROUP BY i.relname, ix.indisunique
ORDER BY i.relname
"#;

/// Query to get constraint information for a PostgreSQL table
pub const PG_CONSTRAINTS_QUERY: &str = r#"
SELECT
    tc.constraint_name as name,
    tc.constraint_type,
    array_agg(kcu.column_name ORDER BY kcu.ordinal_position) as columns,
    ccu.table_name as references_table,
    ccu.column_name as references_column
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
LEFT JOIN information_schema.constraint_column_usage ccu
    ON tc.constraint_name = ccu.constraint_name
    AND tc.constraint_type = 'FOREIGN KEY'
WHERE tc.table_schema = 'public'
  AND tc.table_name = $1
GROUP BY tc.constraint_name, tc.constraint_type, ccu.table_name, ccu.column_name
ORDER BY tc.constraint_name
"#;

// ============================================================================
// SQLite Schema Introspection
// ============================================================================

/// Query to get all table names in SQLite
pub const SQLITE_TABLES_QUERY: &str = r#"
SELECT name
FROM sqlite_master
WHERE type = 'table'
  AND name NOT LIKE 'sqlite_%'
ORDER BY name
"#;

/// SQLite PRAGMA for table info - returns columns with type, notnull, pk, dflt_value
pub fn sqlite_table_info_pragma(table: &str) -> String {
    format!("PRAGMA table_info('{}')", table)
}

/// SQLite PRAGMA for index list
pub fn sqlite_index_list_pragma(table: &str) -> String {
    format!("PRAGMA index_list('{}')", table)
}

/// SQLite PRAGMA for index info
pub fn sqlite_index_info_pragma(index: &str) -> String {
    format!("PRAGMA index_info('{}')", index)
}

/// SQLite PRAGMA for foreign key list
pub fn sqlite_foreign_key_list_pragma(table: &str) -> String {
    format!("PRAGMA foreign_key_list('{}')", table)
}
