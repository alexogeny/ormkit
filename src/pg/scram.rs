//! SCRAM-SHA-256 authentication implementation.
//!
//! Implements RFC 5802 (SCRAM) and RFC 7677 (SCRAM-SHA-256) for PostgreSQL.

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use hmac::{Hmac, Mac};
use rand::Rng;
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

/// SCRAM-SHA-256 client state machine.
pub struct ScramClient {
    /// Username
    username: String,
    /// Password
    password: String,
    /// Client nonce
    client_nonce: String,
    /// Combined nonce (client + server)
    combined_nonce: Option<String>,
    /// Server salt
    salt: Option<Vec<u8>>,
    /// Iteration count
    iterations: Option<u32>,
    /// Auth message for final verification
    auth_message: Option<String>,
    /// Salted password (cached for final step)
    salted_password: Option<[u8; 32]>,
}

impl ScramClient {
    /// Create a new SCRAM client.
    pub fn new(username: &str, password: &str) -> Self {
        // Generate 18 bytes of random data, then base64 encode (24 chars)
        let mut rng = rand::thread_rng();
        let nonce_bytes: [u8; 18] = rng.gen();
        let client_nonce = BASE64.encode(nonce_bytes);

        Self {
            username: username.to_string(),
            password: password.to_string(),
            client_nonce,
            combined_nonce: None,
            salt: None,
            iterations: None,
            auth_message: None,
            salted_password: None,
        }
    }

    /// Generate the initial client message (client-first-message).
    ///
    /// Format: `n,,n=<username>,r=<client-nonce>`
    pub fn client_first_message(&self) -> Vec<u8> {
        // GS2 header: n,, (no channel binding, no authzid)
        // Then: n=<saslname>,r=<nonce>
        let bare = format!("n={},r={}", sasl_prep(&self.username), self.client_nonce);
        format!("n,,{}", bare).into_bytes()
    }

    /// Process the server's first message and generate the client's final message.
    ///
    /// Server message format: `r=<nonce>,s=<salt>,i=<iterations>`
    /// Returns: client-final-message
    pub fn process_server_first(&mut self, server_msg: &[u8]) -> Result<Vec<u8>, ScramError> {
        let server_str =
            std::str::from_utf8(server_msg).map_err(|_| ScramError::InvalidServerMessage)?;

        // Parse server-first-message
        let mut nonce = None;
        let mut salt = None;
        let mut iterations = None;

        for part in server_str.split(',') {
            if let Some(value) = part.strip_prefix("r=") {
                nonce = Some(value.to_string());
            } else if let Some(value) = part.strip_prefix("s=") {
                salt = Some(BASE64.decode(value).map_err(|_| ScramError::InvalidSalt)?);
            } else if let Some(value) = part.strip_prefix("i=") {
                iterations = Some(
                    value
                        .parse::<u32>()
                        .map_err(|_| ScramError::InvalidIterations)?,
                );
            }
        }

        let combined_nonce = nonce.ok_or(ScramError::MissingNonce)?;
        let salt = salt.ok_or(ScramError::MissingSalt)?;
        let iterations = iterations.ok_or(ScramError::MissingIterations)?;

        // Verify nonce starts with our client nonce
        if !combined_nonce.starts_with(&self.client_nonce) {
            return Err(ScramError::NonceVerificationFailed);
        }

        // Calculate SaltedPassword using PBKDF2
        let salted_password = hi(&self.password, &salt, iterations);

        // Calculate keys
        let client_key = hmac_sha256(&salted_password, b"Client Key");
        let stored_key = sha256(&client_key);

        // Build auth message
        let client_first_bare = format!("n={},r={}", sasl_prep(&self.username), self.client_nonce);
        let server_first = server_str;
        let client_final_without_proof = format!("c=biws,r={}", combined_nonce);

        let auth_message = format!(
            "{},{},{}",
            client_first_bare, server_first, client_final_without_proof
        );

        // Calculate proof
        let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes());
        let client_proof = xor_bytes(&client_key, &client_signature);
        let proof_b64 = BASE64.encode(client_proof);

        // Store state for verification
        self.combined_nonce = Some(combined_nonce.clone());
        self.salt = Some(salt);
        self.iterations = Some(iterations);
        self.auth_message = Some(auth_message);
        self.salted_password = Some(salted_password);

        // Build client-final-message
        let client_final = format!("c=biws,r={},p={}", combined_nonce, proof_b64);
        Ok(client_final.into_bytes())
    }

    /// Verify the server's final message (server signature).
    ///
    /// Server message format: `v=<verifier>`
    pub fn verify_server_final(&self, server_msg: &[u8]) -> Result<(), ScramError> {
        let server_str =
            std::str::from_utf8(server_msg).map_err(|_| ScramError::InvalidServerMessage)?;

        let verifier_b64 = server_str
            .strip_prefix("v=")
            .ok_or(ScramError::InvalidServerSignature)?;

        let server_signature = BASE64
            .decode(verifier_b64)
            .map_err(|_| ScramError::InvalidServerSignature)?;

        // Calculate expected server signature
        let salted_password = self.salted_password.ok_or(ScramError::InvalidState)?;
        let auth_message = self.auth_message.as_ref().ok_or(ScramError::InvalidState)?;

        let server_key = hmac_sha256(&salted_password, b"Server Key");
        let expected_signature = hmac_sha256(&server_key, auth_message.as_bytes());

        if server_signature != expected_signature {
            return Err(ScramError::ServerSignatureVerificationFailed);
        }

        Ok(())
    }
}

/// SCRAM authentication errors.
#[derive(Debug, Clone)]
pub enum ScramError {
    InvalidServerMessage,
    InvalidSalt,
    InvalidIterations,
    MissingNonce,
    MissingSalt,
    MissingIterations,
    NonceVerificationFailed,
    InvalidServerSignature,
    ServerSignatureVerificationFailed,
    InvalidState,
}

impl std::fmt::Display for ScramError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidServerMessage => write!(f, "Invalid server message"),
            Self::InvalidSalt => write!(f, "Invalid salt encoding"),
            Self::InvalidIterations => write!(f, "Invalid iteration count"),
            Self::MissingNonce => write!(f, "Missing nonce in server message"),
            Self::MissingSalt => write!(f, "Missing salt in server message"),
            Self::MissingIterations => write!(f, "Missing iterations in server message"),
            Self::NonceVerificationFailed => write!(f, "Server nonce verification failed"),
            Self::InvalidServerSignature => write!(f, "Invalid server signature"),
            Self::ServerSignatureVerificationFailed => {
                write!(f, "Server signature verification failed")
            }
            Self::InvalidState => write!(f, "Invalid SCRAM state"),
        }
    }
}

impl std::error::Error for ScramError {}

// ============================================================================
// Helper Functions
// ============================================================================

/// Hi() function - PBKDF2 with HMAC-SHA-256
fn hi(password: &str, salt: &[u8], iterations: u32) -> [u8; 32] {
    let mut output = [0u8; 32];
    pbkdf2::pbkdf2::<HmacSha256>(password.as_bytes(), salt, iterations, &mut output)
        .expect("valid output length");
    output
}

/// HMAC-SHA-256
fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

/// SHA-256 hash
fn sha256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// XOR two byte arrays
fn xor_bytes(a: &[u8; 32], b: &[u8; 32]) -> [u8; 32] {
    let mut result = [0u8; 32];
    for i in 0..32 {
        result[i] = a[i] ^ b[i];
    }
    result
}

/// SASLprep normalization (simplified - just handles basic cases)
///
/// Full SASLprep (RFC 4013) is complex. PostgreSQL is lenient, so we do minimal processing.
fn sasl_prep(s: &str) -> String {
    // For now, just return as-is. PostgreSQL handles most usernames fine.
    // A full implementation would normalize Unicode and handle prohibited characters.
    s.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scram_flow() {
        // Test with known values from PostgreSQL documentation
        let mut client = ScramClient::new("user", "pencil");

        // Client first message should start with "n,,"
        let first = client.client_first_message();
        let first_str = String::from_utf8(first.clone()).unwrap();
        assert!(first_str.starts_with("n,,n=user,r="));

        // Simulate server response (using the client's nonce + server nonce)
        let client_nonce = &first_str[9..]; // Extract nonce from "n,,n=user,r=NONCE"
        let server_first = format!(
            "r={}SERVER_NONCE,s={},i=4096",
            client_nonce,
            BASE64.encode(b"salt1234salt1234")
        );

        // Process server first
        let final_msg = client
            .process_server_first(server_first.as_bytes())
            .unwrap();
        let final_str = String::from_utf8(final_msg).unwrap();

        // Client final should have channel binding, nonce, and proof
        assert!(final_str.starts_with("c=biws,r="));
        assert!(final_str.contains(",p="));
    }
}
