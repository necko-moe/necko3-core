use alloy_primitives::utils::UnitsError;
use thiserror::Error;
use necko3_database::error::DbError;

#[derive(Debug, Error)]
pub enum InvoiceCreationError {
    #[error("Database error: {0}")]
    Database(#[from] DbError),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Network '{0}' not found")]
    NetworkNotFound(String),

    #[error("Token '{symbol}' not found on network '{network}'")]
    TokenNotFound { symbol: String, network: String },

    #[error("Worker for network '{0}' is not initialized")]
    WorkerNotInitialized(String),

    #[error("Missing XPUB for network '{0}'. Cannot generate new addresses.")]
    MissingXpub(String),

    #[error("Failed to parse or format units: {0}")]
    UnitConversion(#[from] UnitsError),

    #[error("Blockchain adapter error: {0}")]
    Adapter(String),
}