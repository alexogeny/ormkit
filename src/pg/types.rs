//! PostgreSQL type encoding and decoding.
//!
//! This module provides binary format encoding/decoding for PostgreSQL types.
//! Reference: https://www.postgresql.org/docs/current/protocol-overview.html#PROTOCOL-FORMAT-CODES

use super::error::{PgError, PgResult};

// ============================================================================
// Type OIDs
// ============================================================================

/// PostgreSQL type object identifiers (OIDs).
///
/// These are the built-in type OIDs from PostgreSQL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Oid(pub i32);

impl Oid {
    // Boolean types
    pub const BOOL: Oid = Oid(16);

    // Binary data
    pub const BYTEA: Oid = Oid(17);

    // Character types
    pub const CHAR: Oid = Oid(18);
    pub const NAME: Oid = Oid(19);

    // Integer types
    pub const INT8: Oid = Oid(20);
    pub const INT2: Oid = Oid(21);
    pub const INT4: Oid = Oid(23);

    // Text types
    pub const TEXT: Oid = Oid(25);

    // OID type
    pub const OID_TYPE: Oid = Oid(26);

    // Floating point types
    pub const FLOAT4: Oid = Oid(700);
    pub const FLOAT8: Oid = Oid(701);

    // Money
    pub const MONEY: Oid = Oid(790);

    // String types
    pub const VARCHAR: Oid = Oid(1043);
    pub const BPCHAR: Oid = Oid(1042);

    // Date/time types
    pub const DATE: Oid = Oid(1082);
    pub const TIME: Oid = Oid(1083);
    pub const TIMESTAMP: Oid = Oid(1114);
    pub const TIMESTAMPTZ: Oid = Oid(1184);
    pub const INTERVAL: Oid = Oid(1186);
    pub const TIMETZ: Oid = Oid(1266);

    // Network types
    pub const INET: Oid = Oid(869);
    pub const CIDR: Oid = Oid(650);
    pub const MACADDR: Oid = Oid(829);

    // UUID
    pub const UUID: Oid = Oid(2950);

    // JSON types
    pub const JSON: Oid = Oid(114);
    pub const JSONB: Oid = Oid(3802);

    // Array types (some common ones)
    pub const INT4_ARRAY: Oid = Oid(1007);
    pub const TEXT_ARRAY: Oid = Oid(1009);

    // Numeric
    pub const NUMERIC: Oid = Oid(1700);

    /// Create from raw i32 value
    #[inline]
    pub fn from_i32(oid: i32) -> Self {
        Oid(oid)
    }

    /// Get the raw i32 value
    #[inline]
    pub fn as_i32(self) -> i32 {
        self.0
    }

    /// Check if this is a text-like type
    pub fn is_text_like(self) -> bool {
        matches!(
            self,
            Oid::TEXT | Oid::VARCHAR | Oid::BPCHAR | Oid::CHAR | Oid::NAME
        )
    }

    /// Check if this is an integer type
    pub fn is_integer(self) -> bool {
        matches!(self, Oid::INT2 | Oid::INT4 | Oid::INT8)
    }

    /// Check if this is a floating point type
    pub fn is_float(self) -> bool {
        matches!(self, Oid::FLOAT4 | Oid::FLOAT8)
    }
}

// ============================================================================
// PostgreSQL Values
// ============================================================================

/// A PostgreSQL value with type information.
#[derive(Debug, Clone, PartialEq)]
pub enum PgValue {
    Null,
    Bool(bool),
    Int2(i16),
    Int4(i32),
    Int8(i64),
    Float4(f32),
    Float8(f64),
    Text(String),
    Bytea(Vec<u8>),
    Uuid([u8; 16]),
    // Timestamps stored as microseconds since 2000-01-01
    Timestamp(i64),
    Date(i32),
    Time(i64),
    Json(String),
    // For types we don't handle specially - store raw bytes
    Raw { oid: Oid, data: Vec<u8> },
}

impl PgValue {
    /// Check if this value is NULL
    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, PgValue::Null)
    }

    /// Encode this value to binary format.
    pub fn encode_binary(&self) -> Vec<u8> {
        match self {
            PgValue::Null => vec![],
            PgValue::Bool(v) => vec![if *v { 1 } else { 0 }],
            PgValue::Int2(v) => v.to_be_bytes().to_vec(),
            PgValue::Int4(v) => v.to_be_bytes().to_vec(),
            PgValue::Int8(v) => v.to_be_bytes().to_vec(),
            PgValue::Float4(v) => v.to_be_bytes().to_vec(),
            PgValue::Float8(v) => v.to_be_bytes().to_vec(),
            PgValue::Text(v) => v.as_bytes().to_vec(),
            PgValue::Bytea(v) => v.clone(),
            PgValue::Uuid(v) => v.to_vec(),
            PgValue::Timestamp(v) => v.to_be_bytes().to_vec(),
            PgValue::Date(v) => v.to_be_bytes().to_vec(),
            PgValue::Time(v) => v.to_be_bytes().to_vec(),
            PgValue::Json(v) => v.as_bytes().to_vec(),
            PgValue::Raw { data, .. } => data.clone(),
        }
    }

    /// Get the OID for this value's type
    pub fn type_oid(&self) -> Oid {
        match self {
            PgValue::Null => Oid::TEXT, // NULL doesn't have a specific type
            PgValue::Bool(_) => Oid::BOOL,
            PgValue::Int2(_) => Oid::INT2,
            PgValue::Int4(_) => Oid::INT4,
            PgValue::Int8(_) => Oid::INT8,
            PgValue::Float4(_) => Oid::FLOAT4,
            PgValue::Float8(_) => Oid::FLOAT8,
            PgValue::Text(_) => Oid::TEXT,
            PgValue::Bytea(_) => Oid::BYTEA,
            PgValue::Uuid(_) => Oid::UUID,
            PgValue::Timestamp(_) => Oid::TIMESTAMP,
            PgValue::Date(_) => Oid::DATE,
            PgValue::Time(_) => Oid::TIME,
            PgValue::Json(_) => Oid::JSONB,
            PgValue::Raw { oid, .. } => *oid,
        }
    }

    /// Decode a value from binary format.
    pub fn decode_binary(oid: Oid, data: &[u8]) -> PgResult<Self> {
        match oid {
            Oid::BOOL => {
                if data.is_empty() {
                    return Err(PgError::Type("Empty data for BOOL".to_string()));
                }
                Ok(PgValue::Bool(data[0] != 0))
            }

            Oid::INT2 => {
                if data.len() != 2 {
                    return Err(PgError::Type(format!(
                        "Invalid INT2 length: {}",
                        data.len()
                    )));
                }
                Ok(PgValue::Int2(i16::from_be_bytes(data.try_into().unwrap())))
            }

            Oid::INT4 => {
                if data.len() != 4 {
                    return Err(PgError::Type(format!(
                        "Invalid INT4 length: {}",
                        data.len()
                    )));
                }
                Ok(PgValue::Int4(i32::from_be_bytes(data.try_into().unwrap())))
            }

            Oid::INT8 => {
                if data.len() != 8 {
                    return Err(PgError::Type(format!(
                        "Invalid INT8 length: {}",
                        data.len()
                    )));
                }
                Ok(PgValue::Int8(i64::from_be_bytes(data.try_into().unwrap())))
            }

            Oid::FLOAT4 => {
                if data.len() != 4 {
                    return Err(PgError::Type(format!(
                        "Invalid FLOAT4 length: {}",
                        data.len()
                    )));
                }
                Ok(PgValue::Float4(f32::from_be_bytes(
                    data.try_into().unwrap(),
                )))
            }

            Oid::FLOAT8 => {
                if data.len() != 8 {
                    return Err(PgError::Type(format!(
                        "Invalid FLOAT8 length: {}",
                        data.len()
                    )));
                }
                Ok(PgValue::Float8(f64::from_be_bytes(
                    data.try_into().unwrap(),
                )))
            }

            Oid::TEXT | Oid::VARCHAR | Oid::BPCHAR | Oid::CHAR | Oid::NAME => {
                // Validate UTF-8 in place, then convert to String
                // Using from_utf8 validates, then we can use from_utf8_unchecked to avoid double validation
                match std::str::from_utf8(data) {
                    Ok(_) => {
                        // SAFETY: We just validated that data is valid UTF-8
                        let s = unsafe { String::from_utf8_unchecked(data.to_vec()) };
                        Ok(PgValue::Text(s))
                    }
                    Err(e) => Err(PgError::Type(format!("Invalid UTF-8 in TEXT: {}", e))),
                }
            }

            Oid::BYTEA => Ok(PgValue::Bytea(data.to_vec())),

            Oid::UUID => {
                if data.len() != 16 {
                    return Err(PgError::Type(format!(
                        "Invalid UUID length: {}",
                        data.len()
                    )));
                }
                let mut uuid = [0u8; 16];
                uuid.copy_from_slice(data);
                Ok(PgValue::Uuid(uuid))
            }

            Oid::TIMESTAMP | Oid::TIMESTAMPTZ => {
                if data.len() != 8 {
                    return Err(PgError::Type(format!(
                        "Invalid TIMESTAMP length: {}",
                        data.len()
                    )));
                }
                Ok(PgValue::Timestamp(i64::from_be_bytes(
                    data.try_into().unwrap(),
                )))
            }

            Oid::DATE => {
                if data.len() != 4 {
                    return Err(PgError::Type(format!(
                        "Invalid DATE length: {}",
                        data.len()
                    )));
                }
                Ok(PgValue::Date(i32::from_be_bytes(data.try_into().unwrap())))
            }

            Oid::TIME | Oid::TIMETZ => {
                if data.len() < 8 {
                    return Err(PgError::Type(format!(
                        "Invalid TIME length: {}",
                        data.len()
                    )));
                }
                Ok(PgValue::Time(i64::from_be_bytes(
                    data[..8].try_into().unwrap(),
                )))
            }

            Oid::JSON | Oid::JSONB => {
                // JSONB has a version byte prefix
                let json_data = if oid == Oid::JSONB && !data.is_empty() {
                    &data[1..]
                } else {
                    data
                };
                // Validate UTF-8 and convert without double allocation
                match std::str::from_utf8(json_data) {
                    Ok(_) => {
                        // SAFETY: We just validated that json_data is valid UTF-8
                        let s = unsafe { String::from_utf8_unchecked(json_data.to_vec()) };
                        Ok(PgValue::Json(s))
                    }
                    Err(e) => Err(PgError::Type(format!("Invalid UTF-8 in JSON: {}", e))),
                }
            }

            // For unknown types, store raw bytes
            _ => Ok(PgValue::Raw {
                oid,
                data: data.to_vec(),
            }),
        }
    }

    /// Decode from text format (fallback for simple query protocol)
    pub fn decode_text(oid: Oid, data: &[u8]) -> PgResult<Self> {
        let text = String::from_utf8_lossy(data).to_string();

        match oid {
            Oid::BOOL => {
                let v = text == "t" || text == "true" || text == "1";
                Ok(PgValue::Bool(v))
            }

            Oid::INT2 => text
                .parse::<i16>()
                .map(PgValue::Int2)
                .map_err(|e| PgError::Type(format!("Invalid INT2: {}", e))),

            Oid::INT4 => text
                .parse::<i32>()
                .map(PgValue::Int4)
                .map_err(|e| PgError::Type(format!("Invalid INT4: {}", e))),

            Oid::INT8 => text
                .parse::<i64>()
                .map(PgValue::Int8)
                .map_err(|e| PgError::Type(format!("Invalid INT8: {}", e))),

            Oid::FLOAT4 => text
                .parse::<f32>()
                .map(PgValue::Float4)
                .map_err(|e| PgError::Type(format!("Invalid FLOAT4: {}", e))),

            Oid::FLOAT8 => text
                .parse::<f64>()
                .map(PgValue::Float8)
                .map_err(|e| PgError::Type(format!("Invalid FLOAT8: {}", e))),

            // Text types
            _ if oid.is_text_like() => Ok(PgValue::Text(text)),

            // Default: treat as text
            _ => Ok(PgValue::Text(text)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_oid_classification() {
        assert!(Oid::TEXT.is_text_like());
        assert!(Oid::VARCHAR.is_text_like());
        assert!(!Oid::INT4.is_text_like());

        assert!(Oid::INT4.is_integer());
        assert!(Oid::INT8.is_integer());
        assert!(!Oid::FLOAT8.is_integer());

        assert!(Oid::FLOAT4.is_float());
        assert!(Oid::FLOAT8.is_float());
        assert!(!Oid::INT4.is_float());
    }

    #[test]
    fn test_int4_roundtrip() {
        let original = PgValue::Int4(12345);
        let encoded = original.encode_binary();
        let decoded = PgValue::decode_binary(Oid::INT4, &encoded).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_text_roundtrip() {
        let original = PgValue::Text("hello world".to_string());
        let encoded = original.encode_binary();
        let decoded = PgValue::decode_binary(Oid::TEXT, &encoded).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_bool_roundtrip() {
        let true_val = PgValue::Bool(true);
        let false_val = PgValue::Bool(false);

        assert_eq!(
            PgValue::decode_binary(Oid::BOOL, &true_val.encode_binary()).unwrap(),
            true_val
        );
        assert_eq!(
            PgValue::decode_binary(Oid::BOOL, &false_val.encode_binary()).unwrap(),
            false_val
        );
    }
}
