pub use crate::traits::chain::ChainStore;
pub use crate::traits::invoice::InvoiceStore;
pub use crate::traits::payment::PaymentStore;
pub use crate::traits::token::TokenStore;
pub use crate::traits::webhook::WebhookStore;
pub use crate::traits::ext::DatabaseExt;

pub use async_trait::async_trait;

pub mod chain;
pub mod token;
pub mod invoice;
pub mod ext;
pub mod payment;
pub mod webhook;

pub trait DatabaseAdapter:
    ChainStore +
    TokenStore +
    InvoiceStore +
    PaymentStore +
    WebhookStore
{}

impl<T> DatabaseAdapter for T
where
    T: ChainStore + TokenStore + InvoiceStore + PaymentStore + WebhookStore
{}