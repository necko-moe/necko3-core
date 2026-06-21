pub mod chain_store;
pub mod token_store;
pub mod invoice_store;
pub mod payment_store;
pub mod webhook_store;
pub mod xpub_store;
pub mod indexed_blocks_store;
pub mod database_ext;

use crate::traits::DatabaseAdapter;
use alloy_primitives::BlockNumber;
use async_trait::async_trait;
use dashmap::DashMap;
use necko3_types::{ChainData, Invoice, Payment, TokenData, Webhook};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, AtomicU64};
use uuid::Uuid;

#[derive(Default)]
pub struct InMemoryAdapter {
    chains: RwLock<HashMap<String, ChainData>>,
    indexed_blocks: DashMap<(i32, BlockNumber), String>,
    tokens: RwLock<HashMap<String, HashMap<String, TokenData>>>,
    
    chains_last_id: AtomicI32,
    tokens_last_id: AtomicI32,

    invoices: DashMap<Uuid, Invoice>,
    payments: DashMap<Uuid, Payment>,
    webhooks: DashMap<Uuid, Webhook>,

    xpub_states: DashMap<String, AtomicU64>,
}

#[async_trait]
impl DatabaseAdapter for InMemoryAdapter {
    async fn new(_database_url: &str, _max_connections: u32) -> anyhow::Result<Self>
    where
        Self: Sized
    {
        Ok(Default::default())
    }
}