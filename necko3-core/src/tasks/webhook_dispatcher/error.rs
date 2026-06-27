use thiserror::Error;
use necko3_database::error::DbError;

#[derive(Debug, Error)]
pub enum WebhookError {
    #[error("Database error while processing webhook: {0}")]
    Database(#[from] DbError),

    #[error("Failed to serialize webhook payload: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Crypto error: {0}")]
    Crypto(String),
}