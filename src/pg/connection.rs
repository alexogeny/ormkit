//! PostgreSQL connection implementation.
//!
//! This module provides the main connection type that handles:
//! - TCP/TLS connection establishment
//! - Startup and authentication
//! - Simple and extended query protocols
//! - Prepared statement management

use std::collections::HashMap;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;

use super::error::{PgError, PgResult};
use super::protocol::*;
use super::scram::ScramClient;
use super::statement::{PreparedStatement, SharedColumns, StatementCache};
use super::types::{Oid, PgValue};

// ============================================================================
// Connection Configuration
// ============================================================================

/// PostgreSQL connection configuration.
#[derive(Debug, Clone)]
pub struct PgConfig {
    /// Hostname or IP address
    pub host: String,
    /// Port number (default: 5432)
    pub port: u16,
    /// Database name
    pub database: String,
    /// Username
    pub user: String,
    /// Password (optional)
    pub password: Option<String>,
    /// Application name (optional)
    pub application_name: Option<String>,
    /// Statement cache capacity (default: 100)
    pub statement_cache_capacity: usize,
}

impl PgConfig {
    /// Parse a connection URL.
    ///
    /// Format: `postgresql://user:password@host:port/database`
    pub fn from_url(url: &str) -> PgResult<Self> {
        // Remove postgresql:// or postgres:// prefix
        let url = url
            .strip_prefix("postgresql://")
            .or_else(|| url.strip_prefix("postgres://"))
            .ok_or_else(|| PgError::Protocol("Invalid URL scheme".to_string()))?;

        // Split by @ to separate credentials from host
        let (credentials, host_part) = if let Some(at_pos) = url.rfind('@') {
            (&url[..at_pos], &url[at_pos + 1..])
        } else {
            ("", url)
        };

        // Parse credentials
        let (user, password) = if !credentials.is_empty() {
            if let Some(colon_pos) = credentials.find(':') {
                (
                    credentials[..colon_pos].to_string(),
                    Some(credentials[colon_pos + 1..].to_string()),
                )
            } else {
                (credentials.to_string(), None)
            }
        } else {
            ("postgres".to_string(), None)
        };

        // Split host_part by / to separate host:port from database
        let (host_port, database) = if let Some(slash_pos) = host_part.find('/') {
            (&host_part[..slash_pos], &host_part[slash_pos + 1..])
        } else {
            (host_part, "postgres")
        };

        // Parse host and port
        let (host, port) = if let Some(colon_pos) = host_port.rfind(':') {
            let port_str = &host_port[colon_pos + 1..];
            let port = port_str
                .parse::<u16>()
                .map_err(|_| PgError::Protocol(format!("Invalid port: {}", port_str)))?;
            (host_port[..colon_pos].to_string(), port)
        } else {
            (host_port.to_string(), 5432)
        };

        // Handle query parameters (e.g., ?application_name=foo)
        let (database, _params) = if let Some(q_pos) = database.find('?') {
            (&database[..q_pos], Some(&database[q_pos + 1..]))
        } else {
            (database, None)
        };

        Ok(Self {
            host,
            port,
            database: database.to_string(),
            user,
            password,
            application_name: Some("ormkit".to_string()),
            statement_cache_capacity: 100,
        })
    }
}

// ============================================================================
// Query Result
// ============================================================================

/// Result of a query execution.
#[derive(Debug)]
pub struct QueryResult {
    /// Column descriptions - Arc-wrapped to avoid cloning from PreparedStatement.
    /// For simple_query results, a new Arc is created.
    pub columns: SharedColumns,
    /// Row data
    pub rows: Vec<Vec<PgValue>>,
    /// Command tag (e.g., "SELECT 5" or "INSERT 0 1")
    pub command_tag: String,
}

impl QueryResult {
    fn new() -> Self {
        Self {
            columns: Arc::new(Vec::new()),
            rows: Vec::new(),
            command_tag: String::new(),
        }
    }
}

// ============================================================================
// Connection
// ============================================================================

/// A PostgreSQL connection.
pub struct PgConnection {
    /// TCP stream reader
    reader: BufReader<tokio::io::ReadHalf<TcpStream>>,
    /// TCP stream writer
    writer: BufWriter<tokio::io::WriteHalf<TcpStream>>,
    /// Connection configuration
    config: PgConfig,
    /// Prepared statement cache
    statement_cache: StatementCache,
    /// Current transaction status
    transaction_status: TransactionStatus,
    /// Backend process ID
    backend_pid: i32,
    /// Backend secret key (for cancellation)
    backend_secret_key: i32,
    /// Server parameters (e.g., server_version, client_encoding)
    parameters: HashMap<String, String>,
    /// Whether the connection is closed
    closed: bool,
    /// Read buffer for incoming messages
    read_buffer: BytesMut,
}

impl PgConnection {
    /// Connect to a PostgreSQL server.
    pub async fn connect(url: &str) -> PgResult<Self> {
        let config = PgConfig::from_url(url)?;
        Self::connect_with_config(config).await
    }

    /// Connect with explicit configuration.
    pub async fn connect_with_config(config: PgConfig) -> PgResult<Self> {
        // Establish TCP connection
        let addr = format!("{}:{}", config.host, config.port);
        let stream = TcpStream::connect(&addr).await.map_err(PgError::Io)?;

        // Set TCP options
        stream.set_nodelay(true).map_err(PgError::Io)?;

        // Split into read/write halves
        let (read_half, write_half) = tokio::io::split(stream);
        let reader = BufReader::new(read_half);
        let writer = BufWriter::new(write_half);

        let mut conn = Self {
            reader,
            writer,
            statement_cache: StatementCache::new(config.statement_cache_capacity),
            config,
            transaction_status: TransactionStatus::Idle,
            backend_pid: 0,
            backend_secret_key: 0,
            parameters: HashMap::new(),
            closed: false,
            read_buffer: BytesMut::with_capacity(32768), // 32KB buffer for better throughput
        };

        // Perform startup handshake
        conn.startup().await?;

        Ok(conn)
    }

    /// Perform the startup handshake (authentication).
    async fn startup(&mut self) -> PgResult<()> {
        // Send startup message
        let startup = StartupMessage {
            user: self.config.user.clone(),
            database: Some(self.config.database.clone()),
            options: self
                .config
                .application_name
                .as_ref()
                .map(|name| vec![("application_name".to_string(), name.clone())])
                .unwrap_or_default(),
        };

        self.send_message(&startup).await?;

        // Handle authentication
        loop {
            let msg = self.receive_message().await?;

            match msg {
                BackendMessage::AuthenticationOk => {
                    // Authentication successful, continue to ReadyForQuery
                }
                BackendMessage::AuthenticationCleartextPassword => {
                    let password = self
                        .config
                        .password
                        .as_ref()
                        .ok_or_else(|| PgError::Auth("Password required".to_string()))?;

                    let pwd_msg = PasswordMessage {
                        password: password.clone(),
                    };
                    self.send_message(&pwd_msg).await?;
                }
                BackendMessage::AuthenticationMD5Password { salt } => {
                    let password = self
                        .config
                        .password
                        .as_ref()
                        .ok_or_else(|| PgError::Auth("Password required".to_string()))?;

                    let hash = md5_password(&self.config.user, password, &salt);
                    let pwd_msg = PasswordMessage { password: hash };
                    self.send_message(&pwd_msg).await?;
                }
                BackendMessage::AuthenticationSASL { mechanisms } => {
                    // Check for SCRAM-SHA-256 support
                    if !mechanisms.iter().any(|m| m == "SCRAM-SHA-256") {
                        return Err(PgError::Auth(format!(
                            "Server requires unsupported SASL mechanisms: {:?}",
                            mechanisms
                        )));
                    }

                    let password = self
                        .config
                        .password
                        .as_ref()
                        .ok_or_else(|| PgError::Auth("Password required".to_string()))?;

                    // Create SCRAM client and send initial response
                    let mut scram = ScramClient::new(&self.config.user, password);
                    let client_first = scram.client_first_message();

                    let sasl_initial = SaslInitialResponseMessage {
                        mechanism: "SCRAM-SHA-256".to_string(),
                        data: client_first,
                    };
                    self.send_message(&sasl_initial).await?;

                    // Wait for server challenge (AuthenticationSASLContinue)
                    loop {
                        let sasl_msg = self.receive_message().await?;
                        match sasl_msg {
                            BackendMessage::AuthenticationSASLContinue { data } => {
                                // Process server-first-message and send client-final-message
                                let client_final = scram
                                    .process_server_first(&data)
                                    .map_err(|e| PgError::Auth(e.to_string()))?;

                                let sasl_response = SaslResponseMessage { data: client_final };
                                self.send_message(&sasl_response).await?;
                            }
                            BackendMessage::AuthenticationSASLFinal { data } => {
                                // Verify server signature
                                scram
                                    .verify_server_final(&data)
                                    .map_err(|e| PgError::Auth(e.to_string()))?;
                                break;
                            }
                            BackendMessage::ErrorResponse { fields } => {
                                return Err(error_from_fields(&fields));
                            }
                            _ => {
                                // Continue waiting for SASL messages
                            }
                        }
                    }
                }
                BackendMessage::ParameterStatus { name, value } => {
                    self.parameters.insert(name, value);
                }
                BackendMessage::BackendKeyData {
                    process_id,
                    secret_key,
                } => {
                    self.backend_pid = process_id;
                    self.backend_secret_key = secret_key;
                }
                BackendMessage::ReadyForQuery { status } => {
                    self.transaction_status = status;
                    return Ok(());
                }
                BackendMessage::ErrorResponse { fields } => {
                    return Err(error_from_fields(&fields));
                }
                _ => {
                    // Ignore other messages during startup
                }
            }
        }
    }

    /// Execute a simple query (text protocol).
    ///
    /// This is simpler but less efficient than prepared statements.
    /// Use for DDL, transaction control, or one-off queries.
    pub async fn simple_query(&mut self, query: &str) -> PgResult<Vec<QueryResult>> {
        if self.closed {
            return Err(PgError::ConnectionClosed);
        }

        let msg = QueryMessage {
            query: query.to_string(),
        };
        self.send_message(&msg).await?;

        let mut results = Vec::new();
        let mut current_result = QueryResult::new();
        let mut current_columns: Vec<FieldDescription> = Vec::new();

        loop {
            let msg = self.receive_message().await?;

            match msg {
                BackendMessage::RowDescription { fields } => {
                    current_columns = fields.clone();
                    current_result.columns = Arc::new(fields);
                }
                BackendMessage::DataRow { values } => {
                    // Decode row values using text format
                    let row = self.decode_row_text(&values, &current_columns)?;
                    current_result.rows.push(row);
                }
                BackendMessage::CommandComplete { tag } => {
                    current_result.command_tag = tag;
                    results.push(current_result);
                    current_result = QueryResult::new();
                }
                BackendMessage::EmptyQueryResponse => {
                    results.push(QueryResult::new());
                }
                BackendMessage::ReadyForQuery { status } => {
                    self.transaction_status = status;
                    return Ok(results);
                }
                BackendMessage::ErrorResponse { fields } => {
                    // Drain until ReadyForQuery
                    self.drain_until_ready().await?;
                    return Err(error_from_fields(&fields));
                }
                _ => {
                    // Ignore notices, etc.
                }
            }
        }
    }

    /// Execute BEGIN using simple query protocol.
    /// Returns after ReadyForQuery - this is the baseline approach.
    pub async fn begin(&mut self) -> PgResult<()> {
        self.simple_query("BEGIN").await?;
        Ok(())
    }

    /// Buffer BEGIN without flushing (for deferred/lazy BEGIN).
    ///
    /// The BEGIN will be sent with the first actual query, saving a round trip.
    /// Returns immediately without any network I/O.
    pub async fn begin_deferred(&mut self) -> PgResult<()> {
        if self.closed {
            return Err(PgError::ConnectionClosed);
        }

        // Buffer BEGIN using extended protocol - will be flushed with first query
        let parse = ParseMessage {
            name: String::new(),
            query: "BEGIN".to_string(),
            param_types: vec![],
        };
        self.buffer_message(&parse).await?;

        let bind = BindMessage {
            portal: String::new(),
            statement: String::new(),
            param_formats: vec![],
            params: vec![],
            result_formats: vec![],
        };
        self.buffer_message(&bind).await?;

        let execute = ExecuteMessage {
            portal: String::new(),
            max_rows: 0,
        };
        self.buffer_message(&execute).await?;

        // Don't flush! Let the first query flush it.
        Ok(())
    }

    /// Consume buffered BEGIN response (call after first query flushes).
    pub async fn consume_begin_response(&mut self) -> PgResult<()> {
        loop {
            let msg = self.receive_message().await?;
            match msg {
                BackendMessage::ParseComplete => {}
                BackendMessage::BindComplete => {}
                BackendMessage::CommandComplete { .. } => {
                    return Ok(());
                }
                BackendMessage::ErrorResponse { fields } => {
                    return Err(error_from_fields(&fields));
                }
                _ => {}
            }
        }
    }

    /// Execute COMMIT using simple query protocol (minimal overhead).
    pub async fn commit(&mut self) -> PgResult<()> {
        if self.closed {
            return Err(PgError::ConnectionClosed);
        }

        // Use simple query protocol for COMMIT - just one message, one response
        let msg = QueryMessage {
            query: "COMMIT".to_string(),
        };
        self.send_message(&msg).await?;

        loop {
            let msg = self.receive_message().await?;
            match msg {
                BackendMessage::CommandComplete { .. } => {}
                BackendMessage::ReadyForQuery { status } => {
                    self.transaction_status = status;
                    return Ok(());
                }
                BackendMessage::ErrorResponse { fields } => {
                    self.drain_until_ready().await?;
                    return Err(error_from_fields(&fields));
                }
                _ => {}
            }
        }
    }

    /// Execute ROLLBACK using simple query protocol.
    pub async fn rollback(&mut self) -> PgResult<()> {
        if self.closed {
            return Err(PgError::ConnectionClosed);
        }

        let msg = QueryMessage {
            query: "ROLLBACK".to_string(),
        };
        self.send_message(&msg).await?;

        loop {
            let msg = self.receive_message().await?;
            match msg {
                BackendMessage::CommandComplete { .. } => {}
                BackendMessage::ReadyForQuery { status } => {
                    self.transaction_status = status;
                    return Ok(());
                }
                BackendMessage::ErrorResponse { fields } => {
                    self.drain_until_ready().await?;
                    return Err(error_from_fields(&fields));
                }
                _ => {}
            }
        }
    }

    /// Execute a query with the extended protocol (binary format).
    ///
    /// This method automatically uses prepared statement caching.
    pub async fn query(&mut self, query: &str, params: &[PgValue]) -> PgResult<QueryResult> {
        self.query_internal(query, params, true).await
    }

    /// Execute a query without syncing (for pipelining within transactions).
    ///
    /// WARNING: Caller must call sync() after all pipelined operations.
    pub async fn query_no_sync(
        &mut self,
        query: &str,
        params: &[PgValue],
    ) -> PgResult<QueryResult> {
        self.query_internal(query, params, false).await
    }

    /// Execute a query within a transaction, optionally consuming deferred BEGIN first.
    ///
    /// When `consume_begin` is true, BEGIN was deferred (buffered but not flushed).
    ///
    /// For cold cache queries (not yet prepared), this pipelines BEGIN with Parse+Describe
    /// to save a round trip:
    /// - Old flow: Flush BEGIN → wait → Prepare → wait → Execute → wait (3 RT)
    /// - New flow: Flush BEGIN+Parse+Describe → wait → Execute → wait (2 RT)
    pub async fn query_in_transaction(
        &mut self,
        query: &str,
        params: &[PgValue],
        consume_begin: bool,
    ) -> PgResult<QueryResult> {
        if self.closed {
            return Err(PgError::ConnectionClosed);
        }

        // Check if statement is already prepared BEFORE we potentially prepare it
        let was_cached = self.statement_cache.contains(query);

        // Get or prepare statement (Arc clone is cheap - just reference count increment)
        let stmt = if was_cached {
            self.statement_cache.get(query).unwrap()
        } else {
            // Cold cache path: Pipeline BEGIN with Parse+Describe
            if consume_begin {
                // BEGIN is already buffered. Add Parse+Describe directly to writer, then Flush.
                let param_types: Vec<Oid> = params.iter().map(|p| p.type_oid()).collect();
                let stmt_name = self.statement_cache.next_statement_name();

                // Buffer Parse message
                let parse = ParseMessage {
                    name: stmt_name.clone(),
                    query: query.to_string(),
                    param_types: param_types.clone(),
                };
                self.buffer_message(&parse).await?;

                // Buffer Describe message
                let describe = DescribeMessage {
                    kind: b'S',
                    name: stmt_name.clone(),
                };
                self.buffer_message(&describe).await?;

                // Add Flush and send BEGIN + Parse + Describe together
                self.buffer_message(&FlushMessage).await?;
                self.flush().await?;

                // Read responses in order: BEGIN responses, then Prepare responses
                self.consume_begin_response().await?;

                // Consume prepare response
                self.consume_prepare_response(query, stmt_name, param_types)
                    .await?
            } else {
                // No BEGIN pending, prepare normally
                self.prepare_internal(query, params).await?
            }
        };

        // Statement is prepared. Now execute it.
        // BEGIN is still pending only if it was_cached (i.e., we didn't prepare)
        let begin_still_pending = consume_begin && was_cached;

        // Buffer Bind + Execute + Flush
        let bind = BindMessage {
            portal: String::new(),
            statement: stmt.name.clone(),
            param_formats: vec![Format::Binary; params.len()],
            params: params.to_vec(),
            result_formats: vec![Format::Binary],
        };
        self.buffer_message(&bind).await?;

        let execute = ExecuteMessage {
            portal: String::new(),
            max_rows: 0,
        };
        self.buffer_message(&execute).await?;

        // Flush to get responses
        self.buffer_message(&FlushMessage).await?;
        self.flush().await?;

        // If BEGIN was deferred and statement was cached, consume BEGIN response first
        if begin_still_pending {
            self.consume_begin_response().await?;
        }

        // Now read query response
        let mut result = QueryResult::new();
        let columns = &stmt.columns;
        result.columns = Arc::clone(columns); // Cheap refcount increment, no data clone

        loop {
            let msg = self.receive_message().await?;

            match msg {
                BackendMessage::BindComplete => {}
                BackendMessage::DataRow { values } => {
                    let row = self.decode_row_binary(&values, columns)?;
                    result.rows.push(row);
                }
                BackendMessage::CommandComplete { tag } => {
                    result.command_tag = tag;
                    return Ok(result);
                }
                BackendMessage::EmptyQueryResponse => {
                    return Ok(result);
                }
                BackendMessage::ErrorResponse { fields } => {
                    return Err(error_from_fields(&fields));
                }
                _ => {}
            }
        }
    }

    /// Internal query implementation.
    async fn query_internal(
        &mut self,
        query: &str,
        params: &[PgValue],
        sync: bool,
    ) -> PgResult<QueryResult> {
        if self.closed {
            return Err(PgError::ConnectionClosed);
        }

        // Check if statement is already prepared (Arc clone is cheap)
        let stmt = if let Some(cached) = self.statement_cache.get(query) {
            cached
        } else {
            // Prepare the statement
            self.prepare_internal(query, params).await?
        };

        // Execute the prepared statement
        self.execute_internal(&stmt, params, sync).await
    }

    /// Prepare a statement explicitly.
    ///
    /// Returns an Arc-wrapped statement for efficient sharing and cache retrieval.
    pub async fn prepare(
        &mut self,
        query: &str,
        param_types: &[Oid],
    ) -> PgResult<Arc<PreparedStatement>> {
        if self.closed {
            return Err(PgError::ConnectionClosed);
        }

        let name = self.statement_cache.next_statement_name();

        // Send Parse
        let parse = ParseMessage {
            name: name.clone(),
            query: query.to_string(),
            param_types: param_types.to_vec(),
        };
        self.send_message(&parse).await?;

        // Send Describe (Statement)
        let describe = DescribeMessage {
            kind: b'S',
            name: name.clone(),
        };
        self.send_message(&describe).await?;

        // Send Sync
        self.send_message(&SyncMessage).await?;

        let mut stmt = PreparedStatement::new(name, query.to_string());
        stmt.set_param_types(param_types.to_vec());

        // Process responses
        loop {
            let msg = self.receive_message().await?;

            match msg {
                BackendMessage::ParseComplete => {}
                BackendMessage::ParameterDescription { type_oids } => {
                    stmt.set_param_types(type_oids);
                }
                BackendMessage::RowDescription { fields } => {
                    stmt.set_columns(fields);
                }
                BackendMessage::NoData => {
                    // Query doesn't return rows
                }
                BackendMessage::ReadyForQuery { status } => {
                    self.transaction_status = status;

                    // Cache the statement (Arc-wrapped for cheap cloning)
                    let stmt = Arc::new(stmt);
                    self.statement_cache
                        .insert_arc(query.to_string(), Arc::clone(&stmt));

                    return Ok(stmt);
                }
                BackendMessage::ErrorResponse { fields } => {
                    self.drain_until_ready().await?;
                    return Err(error_from_fields(&fields));
                }
                _ => {}
            }
        }
    }

    /// Prepare a statement internally (infer types from params).
    async fn prepare_internal(
        &mut self,
        query: &str,
        params: &[PgValue],
    ) -> PgResult<Arc<PreparedStatement>> {
        let param_types: Vec<Oid> = params.iter().map(|p| p.type_oid()).collect();
        self.prepare(query, &param_types).await
    }

    /// Consume Parse+Describe responses after pipelined prepare.
    ///
    /// Call this after flushing buffered Parse+Describe messages.
    /// Returns an Arc-wrapped statement for efficient sharing.
    async fn consume_prepare_response(
        &mut self,
        query: &str,
        stmt_name: String,
        param_types: Vec<Oid>,
    ) -> PgResult<Arc<PreparedStatement>> {
        let mut stmt = PreparedStatement::new(stmt_name, query.to_string());
        stmt.set_param_types(param_types);

        loop {
            let msg = self.receive_message().await?;

            match msg {
                BackendMessage::ParseComplete => {}
                BackendMessage::ParameterDescription { type_oids } => {
                    stmt.set_param_types(type_oids);
                }
                BackendMessage::RowDescription { fields } => {
                    stmt.set_columns(fields);
                    // RowDescription is the last response for a SELECT-like query
                    let stmt = Arc::new(stmt);
                    self.statement_cache
                        .insert_arc(query.to_string(), Arc::clone(&stmt));
                    return Ok(stmt);
                }
                BackendMessage::NoData => {
                    // Query doesn't return rows - NoData is the last response
                    let stmt = Arc::new(stmt);
                    self.statement_cache
                        .insert_arc(query.to_string(), Arc::clone(&stmt));
                    return Ok(stmt);
                }
                BackendMessage::ErrorResponse { fields } => {
                    return Err(error_from_fields(&fields));
                }
                _ => {}
            }
        }
    }

    /// Execute a prepared statement.
    pub async fn execute(
        &mut self,
        stmt: &PreparedStatement,
        params: &[PgValue],
    ) -> PgResult<QueryResult> {
        self.execute_internal(stmt, params, true).await
    }

    /// Execute without syncing (for pipelining within transactions).
    ///
    /// WARNING: Caller must call sync() after all pipelined operations.
    pub async fn execute_no_sync(
        &mut self,
        stmt: &PreparedStatement,
        params: &[PgValue],
    ) -> PgResult<QueryResult> {
        self.execute_internal(stmt, params, false).await
    }

    /// Internal execute implementation.
    async fn execute_internal(
        &mut self,
        stmt: &PreparedStatement,
        params: &[PgValue],
        sync: bool,
    ) -> PgResult<QueryResult> {
        if self.closed {
            return Err(PgError::ConnectionClosed);
        }

        // Pipeline: Bind + Execute (+ optional Sync) in single flush
        let bind = BindMessage {
            portal: String::new(),
            statement: stmt.name.clone(),
            param_formats: vec![Format::Binary; params.len()],
            params: params.to_vec(),
            result_formats: vec![Format::Binary],
        };
        self.buffer_message(&bind).await?;

        let execute = ExecuteMessage {
            portal: String::new(),
            max_rows: 0, // No limit
        };
        self.buffer_message(&execute).await?;

        if sync {
            // Sync for full round-trip with ReadyForQuery
            self.buffer_message(&SyncMessage).await?;
        } else {
            // Flush to get responses without ReadyForQuery
            self.buffer_message(&FlushMessage).await?;
        }
        self.flush().await?;

        let mut result = QueryResult::new();
        // Use cached columns from prepared statement - cheap Arc clone (refcount only)
        let columns = &stmt.columns;
        result.columns = Arc::clone(columns);

        // Process responses
        loop {
            let msg = self.receive_message().await?;

            match msg {
                BackendMessage::BindComplete => {}
                BackendMessage::DataRow { values } => {
                    let row = self.decode_row_binary(&values, columns)?;
                    result.rows.push(row);
                }
                BackendMessage::CommandComplete { tag } => {
                    result.command_tag = tag;
                    if !sync {
                        // Without sync, CommandComplete is our terminator
                        return Ok(result);
                    }
                }
                BackendMessage::EmptyQueryResponse => {
                    if !sync {
                        return Ok(result);
                    }
                }
                BackendMessage::ReadyForQuery { status } => {
                    self.transaction_status = status;
                    return Ok(result);
                }
                BackendMessage::ErrorResponse { fields } => {
                    if sync {
                        self.drain_until_ready().await?;
                    }
                    return Err(error_from_fields(&fields));
                }
                _ => {}
            }
        }
    }

    /// Send a Sync message and wait for ReadyForQuery.
    ///
    /// Use this after pipelined execute_no_sync() calls to ensure
    /// the server has processed all commands.
    pub async fn sync(&mut self) -> PgResult<()> {
        if self.closed {
            return Err(PgError::ConnectionClosed);
        }

        self.send_message(&SyncMessage).await?;

        // Wait for ReadyForQuery
        loop {
            let msg = self.receive_message().await?;
            match msg {
                BackendMessage::ReadyForQuery { status } => {
                    self.transaction_status = status;
                    return Ok(());
                }
                BackendMessage::ErrorResponse { fields } => {
                    self.drain_until_ready().await?;
                    return Err(error_from_fields(&fields));
                }
                _ => {}
            }
        }
    }

    /// Get a reference to the statement cache.
    pub fn statement_cache(&self) -> &StatementCache {
        &self.statement_cache
    }

    /// Close the connection.
    pub async fn close(&mut self) -> PgResult<()> {
        if self.closed {
            return Ok(());
        }

        self.send_message(&TerminateMessage).await?;
        self.closed = true;
        Ok(())
    }

    /// Check if the connection is closed.
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Get the current transaction status.
    pub fn transaction_status(&self) -> TransactionStatus {
        self.transaction_status
    }

    /// Get backend process ID.
    pub fn backend_pid(&self) -> i32 {
        self.backend_pid
    }

    /// Get a server parameter.
    pub fn parameter(&self, name: &str) -> Option<&str> {
        self.parameters.get(name).map(|s| s.as_str())
    }

    // ========================================================================
    // Private helpers
    // ========================================================================

    /// Send a frontend message (with flush).
    async fn send_message<M: FrontendMessage>(&mut self, msg: &M) -> PgResult<()> {
        let encoded = msg.encode();
        self.writer.write_all(&encoded).await.map_err(PgError::Io)?;
        self.writer.flush().await.map_err(PgError::Io)?;
        Ok(())
    }

    /// Buffer a frontend message without flushing (for pipelining).
    #[inline]
    async fn buffer_message<M: FrontendMessage>(&mut self, msg: &M) -> PgResult<()> {
        let encoded = msg.encode();
        self.writer.write_all(&encoded).await.map_err(PgError::Io)?;
        Ok(())
    }

    /// Flush buffered messages.
    #[inline]
    async fn flush(&mut self) -> PgResult<()> {
        self.writer.flush().await.map_err(PgError::Io)?;
        Ok(())
    }

    /// Receive a backend message.
    async fn receive_message(&mut self) -> PgResult<BackendMessage> {
        // Read message header (type + length)
        loop {
            // Try to decode from buffer first
            if self.read_buffer.len() >= 5 {
                let _msg_type = self.read_buffer[0];
                let length = i32::from_be_bytes([
                    self.read_buffer[1],
                    self.read_buffer[2],
                    self.read_buffer[3],
                    self.read_buffer[4],
                ]) as usize;

                let total_len = 1 + length; // type byte + length field value (includes length field itself)

                if self.read_buffer.len() >= total_len {
                    let msg_bytes = self.read_buffer.split_to(total_len);
                    return BackendMessage::decode(&mut Bytes::from(msg_bytes));
                }
            }

            // Need more data
            let mut buf = [0u8; 4096];
            let n = self.reader.read(&mut buf).await.map_err(PgError::Io)?;

            if n == 0 {
                return Err(PgError::ConnectionClosed);
            }

            self.read_buffer.extend_from_slice(&buf[..n]);
        }
    }

    /// Drain messages until ReadyForQuery (after error).
    async fn drain_until_ready(&mut self) -> PgResult<()> {
        loop {
            let msg = self.receive_message().await?;
            if let BackendMessage::ReadyForQuery { status } = msg {
                self.transaction_status = status;
                return Ok(());
            }
        }
    }

    /// Decode a row from binary format.
    fn decode_row_binary(
        &self,
        values: &[Option<Bytes>],
        columns: &[FieldDescription],
    ) -> PgResult<Vec<PgValue>> {
        let mut row = Vec::with_capacity(values.len());

        for (i, value) in values.iter().enumerate() {
            let pg_value = match value {
                Some(data) => {
                    let oid = if i < columns.len() {
                        columns[i].type_oid
                    } else {
                        Oid::TEXT
                    };
                    PgValue::decode_binary(oid, data)?
                }
                None => PgValue::Null,
            };
            row.push(pg_value);
        }

        Ok(row)
    }

    /// Decode a row from text format (simple query protocol).
    fn decode_row_text(
        &self,
        values: &[Option<Bytes>],
        columns: &[FieldDescription],
    ) -> PgResult<Vec<PgValue>> {
        let mut row = Vec::with_capacity(values.len());

        for (i, value) in values.iter().enumerate() {
            let pg_value = match value {
                Some(data) => {
                    let oid = if i < columns.len() {
                        columns[i].type_oid
                    } else {
                        Oid::TEXT
                    };
                    PgValue::decode_text(oid, data)?
                }
                None => PgValue::Null,
            };
            row.push(pg_value);
        }

        Ok(row)
    }
}

// ============================================================================
// Helper functions
// ============================================================================

/// Compute MD5 password hash.
fn md5_password(user: &str, password: &str, salt: &[u8; 4]) -> String {
    // MD5(MD5(password + user) + salt)
    let inner = format!("{}{}", password, user);
    let inner_hash = md5::compute(inner.as_bytes());

    // Convert inner hash to hex string, then append salt bytes
    let inner_hex = format!("{:x}", inner_hash);
    let mut hasher_input = inner_hex.as_bytes().to_vec();
    hasher_input.extend_from_slice(salt);

    let outer_hash = md5::compute(&hasher_input);
    format!("md5{:x}", outer_hash)
}

/// Create a PgError from error response fields.
fn error_from_fields(fields: &HashMap<u8, String>) -> PgError {
    PgError::Server {
        severity: fields.get(&b'S').cloned().unwrap_or_default(),
        code: fields.get(&b'C').cloned().unwrap_or_default(),
        message: fields.get(&b'M').cloned().unwrap_or_default(),
        detail: fields.get(&b'D').cloned(),
        hint: fields.get(&b'H').cloned(),
    }
}
