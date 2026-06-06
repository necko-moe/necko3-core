use crate::traits::chain::ChainStore;
use crate::traits::invoice::InvoiceStore;
use crate::traits::payment::PaymentStore;
use crate::traits::token::TokenStore;
use crate::traits::webhook::WebhookStore;

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