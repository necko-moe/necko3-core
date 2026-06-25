use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum DbError {
    #[error("Database driver error: {0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("Failed to run migrations: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),

    #[error("{entity} '{id}' not found")]
    NotFound { entity: &'static str, id: String },

    #[error("Corrupted data in DB: {0}")]
    DataCorruption(String),
}

#[derive(Debug, Error)]
pub enum DbExtError {
    #[error(transparent)]
    Db(#[from] DbError),

    #[error("Database driver error: {0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("Asset mismatch for invoice {invoice_id}: expected {expected_token} ({expected_network}), got {got_token} ({got_network})")]
    AssetMismatch {
        invoice_id: Uuid,
        expected_token: String,
        expected_network: String,
        got_token: String,
        got_network: String,
    },
}

pub type DbResult<T> = Result<T, DbError>;
pub type DbExtResult<T> = Result<T, DbExtError>;
