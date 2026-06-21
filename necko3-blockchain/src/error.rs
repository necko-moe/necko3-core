use crate::backends::evm::error::EvmAdapterError;

#[derive(Debug)]
pub struct BoxedError(pub Box<dyn std::error::Error + Send + Sync + 'static>);

impl std::fmt::Display for BoxedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for BoxedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

impl From<EvmAdapterError> for BoxedError {
    fn from(err: EvmAdapterError) -> Self {
        BoxedError(Box::new(err))
    }
}