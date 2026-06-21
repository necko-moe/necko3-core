pub use crate::traits::chain::ChainStore;
pub use crate::traits::ext::DatabaseExt;
pub use crate::traits::invoice::InvoiceStore;
pub use crate::traits::payment::PaymentStore;
pub use crate::traits::token::TokenStore;
pub use crate::traits::webhook::WebhookStore;
pub use crate::traits::xpub::XPubStore;
pub use crate::traits::indexed_blocks::IndexedBlocksStore;

pub use async_trait::async_trait;
use crate::error::DbResult;

pub mod chain;
pub mod token;
pub mod invoice;
pub mod ext;
pub mod payment;
pub mod webhook;
pub mod xpub;
pub mod indexed_blocks;

#[async_trait]
pub trait DatabaseAdapter: Send + Sync {
    async fn new(database_url: &str, max_connections: u32) -> DbResult<Self> where Self: Sized;
}

pub trait DatabaseStore:
    ChainStore +
    TokenStore +
    InvoiceStore +
    PaymentStore +
    WebhookStore +
    XPubStore +
    IndexedBlocksStore
{}

impl<T> DatabaseStore for T
where
    T: ChainStore + TokenStore + InvoiceStore + PaymentStore + WebhookStore + XPubStore + IndexedBlocksStore
{}