//! SQLite type encoding and decoding.

use rusqlite::types::{FromSql, FromSqlResult, ToSql, ToSqlOutput, ValueRef};

/// A SQLite value.
#[derive(Debug, Clone, PartialEq)]
pub enum SqliteValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl SqliteValue {
    /// Check if this value is NULL.
    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, SqliteValue::Null)
    }

    /// Convert from rusqlite ValueRef.
    pub fn from_value_ref(value: ValueRef<'_>) -> Self {
        match value {
            ValueRef::Null => SqliteValue::Null,
            ValueRef::Integer(i) => SqliteValue::Integer(i),
            ValueRef::Real(f) => SqliteValue::Real(f),
            ValueRef::Text(s) => SqliteValue::Text(String::from_utf8_lossy(s).into_owned()),
            ValueRef::Blob(b) => SqliteValue::Blob(b.to_vec()),
        }
    }

    /// Try to get as i64.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            SqliteValue::Integer(i) => Some(*i),
            SqliteValue::Real(f) => Some(*f as i64),
            _ => None,
        }
    }

    /// Try to get as f64.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            SqliteValue::Real(f) => Some(*f),
            SqliteValue::Integer(i) => Some(*i as f64),
            _ => None,
        }
    }

    /// Try to get as string.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            SqliteValue::Text(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as bytes.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            SqliteValue::Blob(b) => Some(b),
            _ => None,
        }
    }
}

impl ToSql for SqliteValue {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        match self {
            SqliteValue::Null => Ok(ToSqlOutput::Owned(rusqlite::types::Value::Null)),
            SqliteValue::Integer(i) => Ok(ToSqlOutput::Owned(rusqlite::types::Value::Integer(*i))),
            SqliteValue::Real(f) => Ok(ToSqlOutput::Owned(rusqlite::types::Value::Real(*f))),
            // Use Borrowed for Text and Blob to avoid cloning - significant optimization
            // for queries with string/blob parameters
            SqliteValue::Text(s) => Ok(ToSqlOutput::Borrowed(ValueRef::Text(s.as_bytes()))),
            SqliteValue::Blob(b) => Ok(ToSqlOutput::Borrowed(ValueRef::Blob(b))),
        }
    }
}

impl FromSql for SqliteValue {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        Ok(SqliteValue::from_value_ref(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_null() {
        let v = SqliteValue::Null;
        assert!(v.is_null());
    }

    #[test]
    fn test_value_integer() {
        let v = SqliteValue::Integer(42);
        assert_eq!(v.as_i64(), Some(42));
        assert_eq!(v.as_f64(), Some(42.0));
    }

    #[test]
    fn test_value_real() {
        let v = SqliteValue::Real(1.5);
        assert_eq!(v.as_f64(), Some(1.5));
        assert_eq!(v.as_i64(), Some(1));
    }

    #[test]
    fn test_value_text() {
        let v = SqliteValue::Text("hello".to_string());
        assert_eq!(v.as_str(), Some("hello"));
    }

    #[test]
    fn test_value_blob() {
        let v = SqliteValue::Blob(vec![1, 2, 3]);
        assert_eq!(v.as_bytes(), Some(&[1u8, 2, 3][..]));
    }
}
