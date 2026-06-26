use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum DbError {
    #[error("Database driver error: {0}")]
    Sqlx(#[from] DbQueryError),

    #[error("Failed to run migrations: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),

    #[error("{entity} '{id}' not found")]
    NotFound { entity: &'static str, id: String },

    #[error("Corrupted data in DB: {0}")]
    DataCorruption(String),
}

#[derive(Debug, Error)]
pub enum DbQueryError {
    #[error("Chain with name '{0}' already exists")]
    ChainAlreadyExists(String),

    #[error("Token with symbol '{symbol}' already exists on chain '{chain}'")]
    TokenSymbolAlreadyExists { symbol: String, chain: String },

    #[error("Token with contract address '{0}' already exists")]
    TokenContractConflict(String),

    #[error("Invoice with address '{0}' already exists")]
    InvoiceAddressConflict(String),

    #[error("Database driver error: {0}")]
    Driver(sqlx::Error),
}

impl From<sqlx::Error> for DbError {
    fn from(err: sqlx::Error) -> Self {
        DbError::Sqlx(DbQueryError::Driver(err))
    }
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
