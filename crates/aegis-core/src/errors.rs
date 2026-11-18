//! Domain error hierarchy for Aegis Quant v2.0 (`aegis-core::errors`).

use thiserror::Error;

pub type Result<T> = std::result::Result<T, AegisError>;

#[derive(Debug, Error)]
pub enum AegisError {
    #[error("Database error: {0}")]
    Database(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Cryptographic error: {0}")]
    Crypto(String),

    #[error("Security violation: {0}")]
    SecurityViolation(String),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Execution error: {0}")]
    Execution(String),

    #[error("Signal computation error: {0}")]
    Signal(String),

    #[error("Alert dispatch error: {0}")]
    Alert(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),
}
