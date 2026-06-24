use thiserror::Error;
use necko3_blockchain::error::BoxedError;

#[derive(Debug, Error)]
pub enum UnitConversionError {
    #[error(transparent)]
    Boxed(#[from] BoxedError),

    #[error("Worker for network '{0}' is not initialized")]
    WorkerNotInitialized(String),
}