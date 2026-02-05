//! Tests for PostgreSQL protocol implementation.
//!
//! Following TDD: These tests define the expected behavior BEFORE implementation.

use super::protocol::*;
use super::types::*;
use bytes::{Bytes, BytesMut};

// ============================================================================
// Protocol Message Encoding Tests
// ============================================================================

mod message_encoding {
    use super::*;

    #[test]
    fn test_startup_message_encoding() {
        // Startup message format:
        // - Int32: Length (including self)
        // - Int32: Protocol version (196608 = 3.0)
        // - String pairs: parameter name, value (null-terminated)
        // - Byte: 0 (terminator)
        let msg = StartupMessage {
            user: "testuser".to_string(),
            database: Some("testdb".to_string()),
            options: vec![],
        };

        let encoded = msg.encode();

        // Check protocol version at bytes 4-7
        let version = i32::from_be_bytes([encoded[4], encoded[5], encoded[6], encoded[7]]);
        assert_eq!(version, 196608, "Protocol version should be 3.0 (196608)");

        // Check that user parameter is present
        let encoded_str = String::from_utf8_lossy(&encoded);
        assert!(
            encoded_str.contains("user"),
            "Should contain 'user' parameter"
        );
    }

    #[test]
    fn test_query_message_encoding() {
        // Simple query message:
        // - Byte: 'Q'
        // - Int32: Length (including self, not including message type)
        // - String: Query (null-terminated)
        let msg = QueryMessage {
            query: "SELECT 1".to_string(),
        };

        let encoded = msg.encode();

        assert_eq!(encoded[0], b'Q', "Query message should start with 'Q'");
        // Length = 4 (length field) + 8 (query) + 1 (null terminator) = 13
        let length = i32::from_be_bytes([encoded[1], encoded[2], encoded[3], encoded[4]]);
        assert_eq!(length, 13, "Length should be 13 for 'SELECT 1'");
    }

    #[test]
    fn test_parse_message_encoding() {
        // Parse message (prepared statement creation):
        // - Byte: 'P'
        // - Int32: Length
        // - String: Statement name (empty = unnamed)
        // - String: Query
        // - Int16: Number of parameter types
        // - Int32[]: Parameter type OIDs
        let msg = ParseMessage {
            name: "stmt_1".to_string(),
            query: "SELECT * FROM users WHERE id = $1".to_string(),
            param_types: vec![Oid::INT4],
        };

        let encoded = msg.encode();

        assert_eq!(encoded[0], b'P', "Parse message should start with 'P'");

        // Verify statement name is in the message
        let encoded_str = String::from_utf8_lossy(&encoded);
        assert!(
            encoded_str.contains("stmt_1"),
            "Should contain statement name"
        );
    }

    #[test]
    fn test_bind_message_encoding() {
        // Bind message:
        // - Byte: 'B'
        // - Int32: Length
        // - String: Portal name (empty = unnamed)
        // - String: Statement name
        // - Int16: Number of parameter format codes
        // - Int16[]: Format codes (0=text, 1=binary)
        // - Int16: Number of parameter values
        // - For each parameter:
        //   - Int32: Length (-1 for NULL)
        //   - Byte[]: Value
        // - Int16: Number of result format codes
        // - Int16[]: Result format codes
        let msg = BindMessage {
            portal: "".to_string(),
            statement: "stmt_1".to_string(),
            param_formats: vec![Format::Binary],
            params: vec![PgValue::Int4(42)],
            result_formats: vec![Format::Binary],
        };

        let encoded = msg.encode();

        assert_eq!(encoded[0], b'B', "Bind message should start with 'B'");
    }

    #[test]
    fn test_execute_message_encoding() {
        // Execute message:
        // - Byte: 'E'
        // - Int32: Length
        // - String: Portal name
        // - Int32: Max rows (0 = no limit)
        let msg = ExecuteMessage {
            portal: "".to_string(),
            max_rows: 0,
        };

        let encoded = msg.encode();

        assert_eq!(encoded[0], b'E', "Execute message should start with 'E'");
    }

    #[test]
    fn test_sync_message_encoding() {
        // Sync message:
        // - Byte: 'S'
        // - Int32: Length (always 4)
        let msg = SyncMessage;
        let encoded = msg.encode();

        assert_eq!(encoded[0], b'S', "Sync message should start with 'S'");
        assert_eq!(encoded.len(), 5, "Sync message should be 5 bytes");
    }

    #[test]
    fn test_terminate_message_encoding() {
        // Terminate message:
        // - Byte: 'X'
        // - Int32: Length (always 4)
        let msg = TerminateMessage;
        let encoded = msg.encode();

        assert_eq!(encoded[0], b'X', "Terminate message should start with 'X'");
        assert_eq!(encoded.len(), 5, "Terminate message should be 5 bytes");
    }
}

// ============================================================================
// Protocol Message Decoding Tests
// ============================================================================

mod message_decoding {
    use super::*;

    #[test]
    fn test_authentication_ok_decoding() {
        // AuthenticationOk:
        // - Byte: 'R'
        // - Int32: Length (8)
        // - Int32: Auth type (0 = OK)
        let data: &[u8] = &[b'R', 0, 0, 0, 8, 0, 0, 0, 0];
        let msg = BackendMessage::decode(&mut Bytes::copy_from_slice(data)).unwrap();

        assert!(
            matches!(msg, BackendMessage::AuthenticationOk),
            "Should decode as AuthenticationOk"
        );
    }

    #[test]
    fn test_authentication_md5_decoding() {
        // AuthenticationMD5Password:
        // - Byte: 'R'
        // - Int32: Length (12)
        // - Int32: Auth type (5 = MD5)
        // - Byte[4]: Salt
        let data: &[u8] = &[b'R', 0, 0, 0, 12, 0, 0, 0, 5, 0x12, 0x34, 0x56, 0x78];
        let msg = BackendMessage::decode(&mut Bytes::copy_from_slice(data)).unwrap();

        match msg {
            BackendMessage::AuthenticationMD5Password { salt } => {
                assert_eq!(salt, [0x12, 0x34, 0x56, 0x78]);
            }
            _ => panic!("Should decode as AuthenticationMD5Password"),
        }
    }

    #[test]
    fn test_ready_for_query_decoding() {
        // ReadyForQuery:
        // - Byte: 'Z'
        // - Int32: Length (5)
        // - Byte: Transaction status ('I' = idle)
        let data: &[u8] = &[b'Z', 0, 0, 0, 5, b'I'];
        let msg = BackendMessage::decode(&mut Bytes::copy_from_slice(data)).unwrap();

        match msg {
            BackendMessage::ReadyForQuery { status } => {
                assert_eq!(status, TransactionStatus::Idle);
            }
            _ => panic!("Should decode as ReadyForQuery"),
        }
    }

    #[test]
    fn test_row_description_decoding() {
        // RowDescription:
        // - Byte: 'T'
        // - Int32: Length
        // - Int16: Number of fields
        // For each field:
        //   - String: Column name
        //   - Int32: Table OID (0 if not a table column)
        //   - Int16: Column attribute number
        //   - Int32: Type OID
        //   - Int16: Type size
        //   - Int32: Type modifier
        //   - Int16: Format code

        // Build a simple row description with one column "id" of type INT4
        let mut data = vec![b'T'];
        let mut body = BytesMut::new();

        // Number of fields
        body.extend_from_slice(&1i16.to_be_bytes());

        // Column name (null-terminated)
        body.extend_from_slice(b"id\0");

        // Table OID
        body.extend_from_slice(&0i32.to_be_bytes());

        // Column attribute number
        body.extend_from_slice(&0i16.to_be_bytes());

        // Type OID (INT4 = 23)
        body.extend_from_slice(&23i32.to_be_bytes());

        // Type size
        body.extend_from_slice(&4i16.to_be_bytes());

        // Type modifier
        body.extend_from_slice(&(-1i32).to_be_bytes());

        // Format code (binary = 1)
        body.extend_from_slice(&1i16.to_be_bytes());

        // Add length
        let length = (body.len() + 4) as i32;
        data.extend_from_slice(&length.to_be_bytes());
        data.extend_from_slice(&body);

        let msg = BackendMessage::decode(&mut Bytes::copy_from_slice(&data)).unwrap();

        match msg {
            BackendMessage::RowDescription { fields } => {
                assert_eq!(fields.len(), 1);
                assert_eq!(fields[0].name, "id");
                assert_eq!(fields[0].type_oid, Oid::INT4);
            }
            _ => panic!("Should decode as RowDescription"),
        }
    }

    #[test]
    fn test_data_row_decoding() {
        // DataRow:
        // - Byte: 'D'
        // - Int32: Length
        // - Int16: Number of columns
        // For each column:
        //   - Int32: Length (-1 for NULL)
        //   - Byte[]: Value

        let mut data = vec![b'D'];
        let mut body = BytesMut::new();

        // Number of columns
        body.extend_from_slice(&2i16.to_be_bytes());

        // Column 1: INT4 value 42
        body.extend_from_slice(&4i32.to_be_bytes()); // length
        body.extend_from_slice(&42i32.to_be_bytes()); // value

        // Column 2: NULL
        body.extend_from_slice(&(-1i32).to_be_bytes());

        // Add length
        let length = (body.len() + 4) as i32;
        data.extend_from_slice(&length.to_be_bytes());
        data.extend_from_slice(&body);

        let msg = BackendMessage::decode(&mut Bytes::copy_from_slice(&data)).unwrap();

        match msg {
            BackendMessage::DataRow { values } => {
                assert_eq!(values.len(), 2);
                assert_eq!(
                    values[0],
                    Some(Bytes::copy_from_slice(&42i32.to_be_bytes()))
                );
                assert_eq!(values[1], None); // NULL
            }
            _ => panic!("Should decode as DataRow"),
        }
    }

    #[test]
    fn test_command_complete_decoding() {
        // CommandComplete:
        // - Byte: 'C'
        // - Int32: Length
        // - String: Command tag (null-terminated)
        let tag = b"SELECT 1\0";
        let mut data = vec![b'C'];
        let length = (tag.len() + 4) as i32;
        data.extend_from_slice(&length.to_be_bytes());
        data.extend_from_slice(tag);

        let msg = BackendMessage::decode(&mut Bytes::copy_from_slice(&data)).unwrap();

        match msg {
            BackendMessage::CommandComplete { tag } => {
                assert_eq!(tag, "SELECT 1");
            }
            _ => panic!("Should decode as CommandComplete"),
        }
    }

    #[test]
    fn test_error_response_decoding() {
        // ErrorResponse:
        // - Byte: 'E'
        // - Int32: Length
        // - Field type (Byte) + Value (String, null-terminated) pairs
        // - Byte: 0 (terminator)

        let mut data = vec![b'E'];
        let mut body = BytesMut::new();

        // Severity
        body.extend_from_slice(b"SERROR\0");
        // Code
        body.extend_from_slice(b"C42P01\0");
        // Message
        body.extend_from_slice(b"Mrelation \"foo\" does not exist\0");
        // Terminator
        body.extend_from_slice(&[0u8]);

        let length = (body.len() + 4) as i32;
        data.extend_from_slice(&length.to_be_bytes());
        data.extend_from_slice(&body);

        let msg = BackendMessage::decode(&mut Bytes::copy_from_slice(&data)).unwrap();

        match msg {
            BackendMessage::ErrorResponse { fields } => {
                assert!(fields.contains_key(&b'S'));
                assert!(fields.contains_key(&b'C'));
                assert!(fields.contains_key(&b'M'));
                assert_eq!(fields.get(&b'C'), Some(&"42P01".to_string()));
            }
            _ => panic!("Should decode as ErrorResponse"),
        }
    }

    #[test]
    fn test_parse_complete_decoding() {
        // ParseComplete:
        // - Byte: '1'
        // - Int32: Length (4)
        let data: &[u8] = &[b'1', 0, 0, 0, 4];
        let msg = BackendMessage::decode(&mut Bytes::copy_from_slice(data)).unwrap();

        assert!(
            matches!(msg, BackendMessage::ParseComplete),
            "Should decode as ParseComplete"
        );
    }

    #[test]
    fn test_bind_complete_decoding() {
        // BindComplete:
        // - Byte: '2'
        // - Int32: Length (4)
        let data: &[u8] = &[b'2', 0, 0, 0, 4];
        let msg = BackendMessage::decode(&mut Bytes::copy_from_slice(data)).unwrap();

        assert!(
            matches!(msg, BackendMessage::BindComplete),
            "Should decode as BindComplete"
        );
    }
}

// ============================================================================
// Type Encoding/Decoding Tests
// ============================================================================

mod type_encoding {
    use super::*;

    #[test]
    fn test_int4_binary_encoding() {
        let value = PgValue::Int4(42);
        let encoded = value.encode_binary();

        assert_eq!(encoded.len(), 4);
        assert_eq!(i32::from_be_bytes(encoded[..].try_into().unwrap()), 42);
    }

    #[test]
    fn test_int8_binary_encoding() {
        let value = PgValue::Int8(9_223_372_036_854_775_807i64);
        let encoded = value.encode_binary();

        assert_eq!(encoded.len(), 8);
        assert_eq!(
            i64::from_be_bytes(encoded[..].try_into().unwrap()),
            9_223_372_036_854_775_807i64
        );
    }

    #[test]
    fn test_float8_binary_encoding() {
        let value = PgValue::Float8(123.456789);
        let encoded = value.encode_binary();

        assert_eq!(encoded.len(), 8);
        let decoded = f64::from_be_bytes(encoded[..].try_into().unwrap());
        assert!((decoded - 123.456789).abs() < 1e-10);
    }

    #[test]
    fn test_bool_binary_encoding() {
        let true_val = PgValue::Bool(true);
        let false_val = PgValue::Bool(false);

        assert_eq!(true_val.encode_binary(), vec![1u8]);
        assert_eq!(false_val.encode_binary(), vec![0u8]);
    }

    #[test]
    fn test_text_binary_encoding() {
        let value = PgValue::Text("hello world".to_string());
        let encoded = value.encode_binary();

        assert_eq!(encoded, b"hello world");
    }

    #[test]
    fn test_bytea_binary_encoding() {
        let value = PgValue::Bytea(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        let encoded = value.encode_binary();

        assert_eq!(encoded, vec![0xDE, 0xAD, 0xBE, 0xEF]);
    }

    #[test]
    fn test_uuid_binary_encoding() {
        // UUID: 550e8400-e29b-41d4-a716-446655440000
        let uuid_bytes: [u8; 16] = [
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x00,
        ];
        let value = PgValue::Uuid(uuid_bytes);
        let encoded = value.encode_binary();

        assert_eq!(encoded.len(), 16);
        assert_eq!(encoded, uuid_bytes.to_vec());
    }
}

mod type_decoding {
    use super::*;

    #[test]
    fn test_int4_binary_decoding() {
        let data = 42i32.to_be_bytes();
        let value = PgValue::decode_binary(Oid::INT4, &data).unwrap();

        assert_eq!(value, PgValue::Int4(42));
    }

    #[test]
    fn test_int8_binary_decoding() {
        let data = 9_223_372_036_854_775_807i64.to_be_bytes();
        let value = PgValue::decode_binary(Oid::INT8, &data).unwrap();

        assert_eq!(value, PgValue::Int8(9_223_372_036_854_775_807i64));
    }

    #[test]
    fn test_float8_binary_decoding() {
        let data = 123.456789f64.to_be_bytes();
        let value = PgValue::decode_binary(Oid::FLOAT8, &data).unwrap();

        match value {
            PgValue::Float8(v) => assert!((v - 123.456789).abs() < 1e-10),
            _ => panic!("Expected Float8"),
        }
    }

    #[test]
    fn test_bool_binary_decoding() {
        let true_val = PgValue::decode_binary(Oid::BOOL, &[1]).unwrap();
        let false_val = PgValue::decode_binary(Oid::BOOL, &[0]).unwrap();

        assert_eq!(true_val, PgValue::Bool(true));
        assert_eq!(false_val, PgValue::Bool(false));
    }

    #[test]
    fn test_text_binary_decoding() {
        let data = b"hello world";
        let value = PgValue::decode_binary(Oid::TEXT, data).unwrap();

        assert_eq!(value, PgValue::Text("hello world".to_string()));
    }

    #[test]
    fn test_null_handling() {
        // NULL is represented as length -1, so the value is None
        let value = PgValue::Null;
        assert!(value.is_null());
    }
}

// ============================================================================
// Statement Cache Tests
// ============================================================================

mod statement_cache {
    use super::super::statement::*;
    use std::sync::Arc;

    #[test]
    fn test_cache_insert_and_get() {
        let mut cache = StatementCache::new(10);

        let stmt = PreparedStatement {
            name: "stmt_1".to_string(),
            query: "SELECT * FROM users WHERE id = $1".to_string(),
            param_types: vec![],
            columns: Arc::new(vec![]),
        };

        cache.insert(
            "SELECT * FROM users WHERE id = $1".to_string(),
            stmt.clone(),
        );

        let found = cache.get("SELECT * FROM users WHERE id = $1");
        assert!(found.is_some());
        assert_eq!(found.unwrap().name, "stmt_1");
    }

    #[test]
    fn test_cache_miss() {
        let cache = StatementCache::new(10);
        let found = cache.get("SELECT 1");
        assert!(found.is_none());
    }

    #[test]
    fn test_cache_eviction() {
        let mut cache = StatementCache::new(2);

        cache.insert(
            "q1".to_string(),
            PreparedStatement {
                name: "s1".to_string(),
                query: "q1".to_string(),
                param_types: vec![],
                columns: Arc::new(vec![]),
            },
        );
        cache.insert(
            "q2".to_string(),
            PreparedStatement {
                name: "s2".to_string(),
                query: "q2".to_string(),
                param_types: vec![],
                columns: Arc::new(vec![]),
            },
        );
        cache.insert(
            "q3".to_string(),
            PreparedStatement {
                name: "s3".to_string(),
                query: "q3".to_string(),
                param_types: vec![],
                columns: Arc::new(vec![]),
            },
        );

        // q1 should have been evicted (LRU)
        assert!(cache.get("q1").is_none());
        assert!(cache.get("q2").is_some());
        assert!(cache.get("q3").is_some());
    }

    #[test]
    fn test_cache_generates_unique_names() {
        let mut cache = StatementCache::new(10);

        let name1 = cache.next_statement_name();
        let name2 = cache.next_statement_name();

        assert_ne!(name1, name2);
        assert!(name1.starts_with("__fk_"));
        assert!(name2.starts_with("__fk_"));
    }
}

// ============================================================================
// Integration Tests (require running PostgreSQL)
// ============================================================================

#[cfg(feature = "postgres-integration-tests")]
mod integration {
    use super::super::connection::*;
    use super::*;

    const TEST_URL: &str = "postgresql://postgres:test@localhost:5432/postgres";

    #[tokio::test]
    async fn test_connect_and_simple_query() {
        let mut conn = PgConnection::connect(TEST_URL).await.unwrap();

        let result = conn.simple_query("SELECT 1 as num").await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].columns.len(), 1);
        assert_eq!(result[0].columns[0].name, "num");
    }

    #[tokio::test]
    async fn test_prepared_statement() {
        let mut conn = PgConnection::connect(TEST_URL).await.unwrap();

        // Prepare
        let stmt = conn
            .prepare("SELECT $1::int4 as num", &[Oid::INT4])
            .await
            .unwrap();

        // Execute with parameter
        let result = conn.execute(&stmt, &[PgValue::Int4(42)]).await.unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], PgValue::Int4(42));
    }

    #[tokio::test]
    async fn test_statement_caching() {
        let mut conn = PgConnection::connect(TEST_URL).await.unwrap();

        // First execution - should create prepared statement
        let result1 = conn
            .query("SELECT $1::int4 as num", &[PgValue::Int4(1)])
            .await
            .unwrap();

        // Second execution - should reuse prepared statement
        let result2 = conn
            .query("SELECT $1::int4 as num", &[PgValue::Int4(2)])
            .await
            .unwrap();

        assert_eq!(result1.rows[0][0], PgValue::Int4(1));
        assert_eq!(result2.rows[0][0], PgValue::Int4(2));

        // Verify statement was cached
        assert!(conn
            .statement_cache()
            .get("SELECT $1::int4 as num")
            .is_some());
    }

    #[tokio::test]
    async fn test_transaction() {
        let mut conn = PgConnection::connect(TEST_URL).await.unwrap();

        // Setup
        conn.simple_query("DROP TABLE IF EXISTS test_tx").await.ok();
        conn.simple_query("CREATE TABLE test_tx (id INT)")
            .await
            .unwrap();

        // Begin transaction
        conn.simple_query("BEGIN").await.unwrap();
        conn.simple_query("INSERT INTO test_tx VALUES (1)")
            .await
            .unwrap();

        // Rollback
        conn.simple_query("ROLLBACK").await.unwrap();

        // Verify rollback
        let result = conn
            .simple_query("SELECT COUNT(*) FROM test_tx")
            .await
            .unwrap();
        assert_eq!(result[0].rows[0][0], PgValue::Int8(0));

        // Cleanup
        conn.simple_query("DROP TABLE test_tx").await.ok();
    }

    #[tokio::test]
    async fn test_binary_types() {
        let mut conn = PgConnection::connect(TEST_URL).await.unwrap();

        // Test various types
        let result = conn
            .query(
                "SELECT $1::int4, $2::int8, $3::float8, $4::bool, $5::text",
                &[
                    PgValue::Int4(42),
                    PgValue::Int8(9_000_000_000i64),
                    PgValue::Float8(3.14),
                    PgValue::Bool(true),
                    PgValue::Text("hello".to_string()),
                ],
            )
            .await
            .unwrap();

        assert_eq!(result.rows[0][0], PgValue::Int4(42));
        assert_eq!(result.rows[0][1], PgValue::Int8(9_000_000_000i64));
        // Float comparison with tolerance
        match &result.rows[0][2] {
            PgValue::Float8(v) => assert!((v - 3.14).abs() < 0.001),
            _ => panic!("Expected Float8"),
        }
        assert_eq!(result.rows[0][3], PgValue::Bool(true));
        assert_eq!(result.rows[0][4], PgValue::Text("hello".to_string()));
    }

    #[tokio::test]
    async fn test_null_values() {
        let mut conn = PgConnection::connect(TEST_URL).await.unwrap();

        let result = conn
            .query("SELECT NULL::int4, $1::int4", &[PgValue::Null])
            .await
            .unwrap();

        assert!(result.rows[0][0].is_null());
        assert!(result.rows[0][1].is_null());
    }

    #[tokio::test]
    async fn test_large_result_set() {
        let mut conn = PgConnection::connect(TEST_URL).await.unwrap();

        let result = conn
            .simple_query("SELECT generate_series(1, 10000)")
            .await
            .unwrap();

        assert_eq!(result[0].rows.len(), 10000);
    }

    #[tokio::test]
    async fn test_connection_close() {
        let mut conn = PgConnection::connect(TEST_URL).await.unwrap();

        conn.close().await.unwrap();

        // Should fail after close
        let result = conn.simple_query("SELECT 1").await;
        assert!(result.is_err());
    }
}
