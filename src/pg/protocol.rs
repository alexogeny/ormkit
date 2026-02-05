//! PostgreSQL wire protocol message encoding and decoding.
//!
//! This module implements the PostgreSQL v3 protocol messages.
//! Reference: https://www.postgresql.org/docs/current/protocol-message-formats.html

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::collections::HashMap;

use super::error::{PgError, PgResult};
use super::types::Oid;

// ============================================================================
// Protocol Constants
// ============================================================================

/// PostgreSQL protocol version 3.0
pub const PROTOCOL_VERSION: i32 = 196608; // (3 << 16) | 0

/// Format codes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum Format {
    Text = 0,
    Binary = 1,
}

/// Transaction status indicators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionStatus {
    /// Idle (not in a transaction block)
    Idle,
    /// In a transaction block
    InTransaction,
    /// In a failed transaction block
    Failed,
}

impl From<u8> for TransactionStatus {
    fn from(b: u8) -> Self {
        match b {
            b'I' => TransactionStatus::Idle,
            b'T' => TransactionStatus::InTransaction,
            b'E' => TransactionStatus::Failed,
            _ => TransactionStatus::Idle,
        }
    }
}

// ============================================================================
// Frontend (Client -> Server) Messages
// ============================================================================

/// Trait for encoding frontend messages
pub trait FrontendMessage {
    fn encode(&self) -> BytesMut;
}

/// Startup message sent at connection start
#[derive(Debug, Clone)]
pub struct StartupMessage {
    pub user: String,
    pub database: Option<String>,
    pub options: Vec<(String, String)>,
}

impl FrontendMessage for StartupMessage {
    fn encode(&self) -> BytesMut {
        let mut buf = BytesMut::new();

        // Placeholder for length (will be filled in at the end)
        buf.put_i32(0);

        // Protocol version
        buf.put_i32(PROTOCOL_VERSION);

        // Parameters
        buf.put_slice(b"user\0");
        buf.put_slice(self.user.as_bytes());
        buf.put_u8(0);

        if let Some(ref db) = self.database {
            buf.put_slice(b"database\0");
            buf.put_slice(db.as_bytes());
            buf.put_u8(0);
        }

        for (key, value) in &self.options {
            buf.put_slice(key.as_bytes());
            buf.put_u8(0);
            buf.put_slice(value.as_bytes());
            buf.put_u8(0);
        }

        // Terminator
        buf.put_u8(0);

        // Fill in length (includes length field itself)
        let len = buf.len() as i32;
        buf[0..4].copy_from_slice(&len.to_be_bytes());

        buf
    }
}

/// Password message (for MD5 or plaintext auth)
#[derive(Debug, Clone)]
pub struct PasswordMessage {
    pub password: String,
}

impl FrontendMessage for PasswordMessage {
    fn encode(&self) -> BytesMut {
        let mut buf = BytesMut::new();
        buf.put_u8(b'p');

        // Length (4 bytes) + password + null terminator
        let len = 4 + self.password.len() as i32 + 1;
        buf.put_i32(len);

        buf.put_slice(self.password.as_bytes());
        buf.put_u8(0);

        buf
    }
}

/// Simple query message ('Q')
#[derive(Debug, Clone)]
pub struct QueryMessage {
    pub query: String,
}

impl FrontendMessage for QueryMessage {
    fn encode(&self) -> BytesMut {
        let mut buf = BytesMut::new();
        buf.put_u8(b'Q');

        // Length (4 bytes) + query + null terminator
        let len = 4 + self.query.len() as i32 + 1;
        buf.put_i32(len);

        buf.put_slice(self.query.as_bytes());
        buf.put_u8(0);

        buf
    }
}

/// Parse message ('P') - Creates a prepared statement
#[derive(Debug, Clone)]
pub struct ParseMessage {
    pub name: String,
    pub query: String,
    pub param_types: Vec<Oid>,
}

impl FrontendMessage for ParseMessage {
    fn encode(&self) -> BytesMut {
        let mut buf = BytesMut::new();
        buf.put_u8(b'P');

        // Build body first to calculate length
        let mut body = BytesMut::new();

        // Statement name (null-terminated)
        body.put_slice(self.name.as_bytes());
        body.put_u8(0);

        // Query string (null-terminated)
        body.put_slice(self.query.as_bytes());
        body.put_u8(0);

        // Number of parameter types
        body.put_i16(self.param_types.len() as i16);

        // Parameter type OIDs
        for oid in &self.param_types {
            body.put_i32(oid.as_i32());
        }

        // Length (includes self)
        buf.put_i32(body.len() as i32 + 4);
        buf.put_slice(&body);

        buf
    }
}

/// Bind message ('B') - Binds parameters to a prepared statement
#[derive(Debug, Clone)]
pub struct BindMessage {
    pub portal: String,
    pub statement: String,
    pub param_formats: Vec<Format>,
    pub params: Vec<super::types::PgValue>,
    pub result_formats: Vec<Format>,
}

impl FrontendMessage for BindMessage {
    fn encode(&self) -> BytesMut {
        let mut buf = BytesMut::new();
        buf.put_u8(b'B');

        let mut body = BytesMut::new();

        // Portal name (null-terminated)
        body.put_slice(self.portal.as_bytes());
        body.put_u8(0);

        // Statement name (null-terminated)
        body.put_slice(self.statement.as_bytes());
        body.put_u8(0);

        // Number of parameter format codes
        body.put_i16(self.param_formats.len() as i16);
        for fmt in &self.param_formats {
            body.put_i16(*fmt as i16);
        }

        // Number of parameter values
        body.put_i16(self.params.len() as i16);
        for param in &self.params {
            if param.is_null() {
                body.put_i32(-1);
            } else {
                let encoded = param.encode_binary();
                body.put_i32(encoded.len() as i32);
                body.put_slice(&encoded);
            }
        }

        // Number of result format codes
        body.put_i16(self.result_formats.len() as i16);
        for fmt in &self.result_formats {
            body.put_i16(*fmt as i16);
        }

        buf.put_i32(body.len() as i32 + 4);
        buf.put_slice(&body);

        buf
    }
}

/// Execute message ('E') - Executes a bound portal
#[derive(Debug, Clone)]
pub struct ExecuteMessage {
    pub portal: String,
    pub max_rows: i32,
}

impl FrontendMessage for ExecuteMessage {
    fn encode(&self) -> BytesMut {
        let mut buf = BytesMut::new();
        buf.put_u8(b'E');

        let mut body = BytesMut::new();

        // Portal name (null-terminated)
        body.put_slice(self.portal.as_bytes());
        body.put_u8(0);

        // Max rows (0 = no limit)
        body.put_i32(self.max_rows);

        buf.put_i32(body.len() as i32 + 4);
        buf.put_slice(&body);

        buf
    }
}

/// Describe message ('D') - Request description of statement or portal
#[derive(Debug, Clone)]
pub struct DescribeMessage {
    /// 'S' for statement, 'P' for portal
    pub kind: u8,
    pub name: String,
}

impl FrontendMessage for DescribeMessage {
    fn encode(&self) -> BytesMut {
        let mut buf = BytesMut::new();
        buf.put_u8(b'D');

        let len = 4 + 1 + self.name.len() as i32 + 1;
        buf.put_i32(len);

        buf.put_u8(self.kind);
        buf.put_slice(self.name.as_bytes());
        buf.put_u8(0);

        buf
    }
}

/// Sync message ('S') - Marks end of an extended query
#[derive(Debug, Clone, Copy)]
pub struct SyncMessage;

impl FrontendMessage for SyncMessage {
    fn encode(&self) -> BytesMut {
        let mut buf = BytesMut::new();
        buf.put_u8(b'S');
        buf.put_i32(4);
        buf
    }
}

/// Flush message ('H') - Request server to flush output buffer
#[derive(Debug, Clone, Copy)]
pub struct FlushMessage;

impl FrontendMessage for FlushMessage {
    fn encode(&self) -> BytesMut {
        let mut buf = BytesMut::new();
        buf.put_u8(b'H');
        buf.put_i32(4);
        buf
    }
}

/// Terminate message ('X') - Close the connection
#[derive(Debug, Clone, Copy)]
pub struct TerminateMessage;

impl FrontendMessage for TerminateMessage {
    fn encode(&self) -> BytesMut {
        let mut buf = BytesMut::new();
        buf.put_u8(b'X');
        buf.put_i32(4);
        buf
    }
}

/// SASL Initial Response message ('p') - First SCRAM message
#[derive(Debug, Clone)]
pub struct SaslInitialResponseMessage {
    /// SASL mechanism name (e.g., "SCRAM-SHA-256")
    pub mechanism: String,
    /// Initial client response data
    pub data: Vec<u8>,
}

impl FrontendMessage for SaslInitialResponseMessage {
    fn encode(&self) -> BytesMut {
        let mut buf = BytesMut::new();
        buf.put_u8(b'p');

        // Calculate length: 4 (len) + mechanism + null + 4 (data len) + data
        let len = 4 + self.mechanism.len() as i32 + 1 + 4 + self.data.len() as i32;
        buf.put_i32(len);

        // Mechanism name (null-terminated)
        buf.put_slice(self.mechanism.as_bytes());
        buf.put_u8(0);

        // Data length (-1 for no data, otherwise length)
        buf.put_i32(self.data.len() as i32);

        // Data
        buf.put_slice(&self.data);

        buf
    }
}

/// SASL Response message ('p') - Subsequent SCRAM messages
#[derive(Debug, Clone)]
pub struct SaslResponseMessage {
    /// SASL response data
    pub data: Vec<u8>,
}

impl FrontendMessage for SaslResponseMessage {
    fn encode(&self) -> BytesMut {
        let mut buf = BytesMut::new();
        buf.put_u8(b'p');

        // Length: 4 (len) + data
        let len = 4 + self.data.len() as i32;
        buf.put_i32(len);

        // Data
        buf.put_slice(&self.data);

        buf
    }
}

// ============================================================================
// Backend (Server -> Client) Messages
// ============================================================================

/// Field description in a RowDescription message
#[derive(Debug, Clone)]
pub struct FieldDescription {
    pub name: String,
    pub table_oid: i32,
    pub column_attr: i16,
    pub type_oid: Oid,
    pub type_size: i16,
    pub type_modifier: i32,
    pub format: Format,
}

/// Backend message types
#[derive(Debug, Clone)]
pub enum BackendMessage {
    // Authentication
    AuthenticationOk,
    AuthenticationCleartextPassword,
    AuthenticationMD5Password {
        salt: [u8; 4],
    },
    AuthenticationSASL {
        mechanisms: Vec<String>,
    },
    AuthenticationSASLContinue {
        data: Bytes,
    },
    AuthenticationSASLFinal {
        data: Bytes,
    },

    // Query responses
    RowDescription {
        fields: Vec<FieldDescription>,
    },
    DataRow {
        values: Vec<Option<Bytes>>,
    },
    CommandComplete {
        tag: String,
    },
    EmptyQueryResponse,

    // Extended query protocol
    ParseComplete,
    BindComplete,
    CloseComplete,
    NoData,
    PortalSuspended,

    // Status
    ReadyForQuery {
        status: TransactionStatus,
    },
    ParameterStatus {
        name: String,
        value: String,
    },
    BackendKeyData {
        process_id: i32,
        secret_key: i32,
    },

    // Errors and notices
    ErrorResponse {
        fields: HashMap<u8, String>,
    },
    NoticeResponse {
        fields: HashMap<u8, String>,
    },

    // Other
    NotificationResponse {
        process_id: i32,
        channel: String,
        payload: String,
    },
    ParameterDescription {
        type_oids: Vec<Oid>,
    },
}

impl BackendMessage {
    /// Decode a backend message from bytes.
    ///
    /// The input buffer should start with the message type byte.
    pub fn decode(buf: &mut Bytes) -> PgResult<Self> {
        if buf.remaining() < 5 {
            return Err(PgError::Protocol("Incomplete message header".to_string()));
        }

        let msg_type = buf.get_u8();
        let len = buf.get_i32() as usize;

        if buf.remaining() < len - 4 {
            return Err(PgError::Protocol("Incomplete message body".to_string()));
        }

        let body = buf.split_to(len - 4);

        match msg_type {
            b'R' => Self::decode_auth(body),
            b'T' => Self::decode_row_description(body),
            b'D' => Self::decode_data_row(body),
            b'C' => Self::decode_command_complete(body),
            b'Z' => Self::decode_ready_for_query(body),
            b'E' => Self::decode_error_response(body),
            b'N' => Self::decode_notice_response(body),
            b'S' => Self::decode_parameter_status(body),
            b'K' => Self::decode_backend_key_data(body),
            b'1' => Ok(BackendMessage::ParseComplete),
            b'2' => Ok(BackendMessage::BindComplete),
            b'3' => Ok(BackendMessage::CloseComplete),
            b'I' => Ok(BackendMessage::EmptyQueryResponse),
            b'n' => Ok(BackendMessage::NoData),
            b's' => Ok(BackendMessage::PortalSuspended),
            b't' => Self::decode_parameter_description(body),
            b'A' => Self::decode_notification_response(body),
            _ => Err(PgError::Protocol(format!(
                "Unknown message type: {}",
                msg_type as char
            ))),
        }
    }

    fn decode_auth(mut body: Bytes) -> PgResult<Self> {
        let auth_type = body.get_i32();

        match auth_type {
            0 => Ok(BackendMessage::AuthenticationOk),
            3 => Ok(BackendMessage::AuthenticationCleartextPassword),
            5 => {
                let mut salt = [0u8; 4];
                salt.copy_from_slice(&body[..4]);
                Ok(BackendMessage::AuthenticationMD5Password { salt })
            }
            10 => {
                // SASL
                let mut mechanisms = Vec::new();
                while body.remaining() > 0 {
                    let mech = read_cstring(&mut body)?;
                    if mech.is_empty() {
                        break;
                    }
                    mechanisms.push(mech);
                }
                Ok(BackendMessage::AuthenticationSASL { mechanisms })
            }
            11 => Ok(BackendMessage::AuthenticationSASLContinue { data: body }),
            12 => Ok(BackendMessage::AuthenticationSASLFinal { data: body }),
            _ => Err(PgError::Protocol(format!(
                "Unknown authentication type: {}",
                auth_type
            ))),
        }
    }

    fn decode_row_description(mut body: Bytes) -> PgResult<Self> {
        let num_fields = body.get_i16() as usize;
        let mut fields = Vec::with_capacity(num_fields);

        for _ in 0..num_fields {
            let name = read_cstring(&mut body)?;
            let table_oid = body.get_i32();
            let column_attr = body.get_i16();
            let type_oid = Oid::from_i32(body.get_i32());
            let type_size = body.get_i16();
            let type_modifier = body.get_i32();
            let format = if body.get_i16() == 0 {
                Format::Text
            } else {
                Format::Binary
            };

            fields.push(FieldDescription {
                name,
                table_oid,
                column_attr,
                type_oid,
                type_size,
                type_modifier,
                format,
            });
        }

        Ok(BackendMessage::RowDescription { fields })
    }

    fn decode_data_row(mut body: Bytes) -> PgResult<Self> {
        let num_cols = body.get_i16() as usize;
        let mut values = Vec::with_capacity(num_cols);

        for _ in 0..num_cols {
            let len = body.get_i32();
            if len < 0 {
                values.push(None);
            } else {
                let data = body.split_to(len as usize);
                values.push(Some(data));
            }
        }

        Ok(BackendMessage::DataRow { values })
    }

    fn decode_command_complete(mut body: Bytes) -> PgResult<Self> {
        let tag = read_cstring(&mut body)?;
        Ok(BackendMessage::CommandComplete { tag })
    }

    fn decode_ready_for_query(mut body: Bytes) -> PgResult<Self> {
        let status = TransactionStatus::from(body.get_u8());
        Ok(BackendMessage::ReadyForQuery { status })
    }

    fn decode_error_response(body: Bytes) -> PgResult<Self> {
        let fields = read_error_fields(body)?;
        Ok(BackendMessage::ErrorResponse { fields })
    }

    fn decode_notice_response(body: Bytes) -> PgResult<Self> {
        let fields = read_error_fields(body)?;
        Ok(BackendMessage::NoticeResponse { fields })
    }

    fn decode_parameter_status(mut body: Bytes) -> PgResult<Self> {
        let name = read_cstring(&mut body)?;
        let value = read_cstring(&mut body)?;
        Ok(BackendMessage::ParameterStatus { name, value })
    }

    fn decode_backend_key_data(mut body: Bytes) -> PgResult<Self> {
        let process_id = body.get_i32();
        let secret_key = body.get_i32();
        Ok(BackendMessage::BackendKeyData {
            process_id,
            secret_key,
        })
    }

    fn decode_parameter_description(mut body: Bytes) -> PgResult<Self> {
        let num_params = body.get_i16() as usize;
        let mut type_oids = Vec::with_capacity(num_params);

        for _ in 0..num_params {
            type_oids.push(Oid::from_i32(body.get_i32()));
        }

        Ok(BackendMessage::ParameterDescription { type_oids })
    }

    fn decode_notification_response(mut body: Bytes) -> PgResult<Self> {
        let process_id = body.get_i32();
        let channel = read_cstring(&mut body)?;
        let payload = read_cstring(&mut body)?;

        Ok(BackendMessage::NotificationResponse {
            process_id,
            channel,
            payload,
        })
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Read a null-terminated string from the buffer.
/// Optimized to avoid double conversion: tries direct UTF-8 first, falls back to lossy only if invalid.
fn read_cstring(buf: &mut Bytes) -> PgResult<String> {
    let mut end = 0;
    while end < buf.remaining() && buf[end] != 0 {
        end += 1;
    }

    if end >= buf.remaining() {
        return Err(PgError::Protocol(
            "Missing null terminator in string".to_string(),
        ));
    }

    // Optimized: try direct UTF-8 conversion first (common case for PostgreSQL)
    // Only fall back to lossy conversion if UTF-8 is invalid
    let s = std::str::from_utf8(&buf[..end])
        .map(|s| s.to_owned())
        .unwrap_or_else(|_| String::from_utf8_lossy(&buf[..end]).into_owned());

    buf.advance(end + 1); // Skip the null terminator
    Ok(s)
}

/// Read error/notice response fields
fn read_error_fields(mut body: Bytes) -> PgResult<HashMap<u8, String>> {
    let mut fields = HashMap::new();

    while body.remaining() > 0 {
        let field_type = body.get_u8();
        if field_type == 0 {
            break;
        }
        let value = read_cstring(&mut body)?;
        fields.insert(field_type, value);
    }

    Ok(fields)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_startup_message_structure() {
        let msg = StartupMessage {
            user: "test".to_string(),
            database: Some("testdb".to_string()),
            options: vec![],
        };

        let encoded = msg.encode();

        // Length should be at least 4 (length) + 4 (version) + some params
        assert!(encoded.len() >= 8);

        // Check length field
        let len = i32::from_be_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]);
        assert_eq!(len as usize, encoded.len());
    }

    #[test]
    fn test_query_message_structure() {
        let msg = QueryMessage {
            query: "SELECT 1".to_string(),
        };

        let encoded = msg.encode();

        assert_eq!(encoded[0], b'Q');

        let len = i32::from_be_bytes([encoded[1], encoded[2], encoded[3], encoded[4]]);
        assert_eq!(len as usize, encoded.len() - 1); // -1 for message type
    }
}
