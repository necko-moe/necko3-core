use dashmap::DashMap;
use necko3_database::traits::DatabaseExt;
use necko3_types::blockchain::{StateCommand, TrackTransaction};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use alloy_primitives::U256;
use alloy_primitives::utils::{format_units, parse_units};
use chrono::Utc;
use tokio::sync::mpsc;
use tokio::task::AbortHandle;
use tracing::warn;
use uuid::Uuid;
use necko3_blockchain::error::BoxedError;
use necko3_blockchain::traits::adapter::BlockchainAdapter;
use necko3_database::backends::in_memory::InMemoryAdapter;
use necko3_database::error::DbResult;
use necko3_database::traits::chain::DbChainId;
use necko3_database::traits::token::DbTokenId;
use necko3_types::{Invoice, InvoiceStatus};
use crate::builder::chain_config::ChainConfig;
use crate::builder::invoice_config::{ExpirationTime, PaymentAddress, PaymentAmount, PaymentAsset, PaymentSpec, WebhookConfig};
use crate::builder::invoice_config::error::InvoiceCreationError;
use crate::builder::NeckoCoreBuilder;
use crate::builder::token_config::TokenConfig;
use crate::error::UnitConversionError;
use crate::types::NeckoEvent;

pub struct Worker {
    pub adapter: Box<dyn BlockchainAdapter<Error = BoxedError>>,
    pub abort_handle: AbortHandle,
    pub state_tx: mpsc::Sender<StateCommand>,
    pub transaction_tx: mpsc::Sender<TrackTransaction>,
}

pub struct NeckoCore<D, E> {
    db: Arc<D>,

    last_channel_id: Arc<AtomicUsize>,
    channels: Arc<DashMap<usize, mpsc::Sender<E>>>,

    workers: Arc<DashMap<String, Worker>>,
}

impl<D, E> Clone for NeckoCore<D, E> {
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),

            last_channel_id: Arc::clone(&self.last_channel_id),
            channels: Arc::clone(&self.channels),

            workers: Arc::clone(&self.workers),
        }
    }
}

impl NeckoCore<InMemoryAdapter, NeckoEvent> {
    pub fn builder() -> NeckoCoreBuilder<InMemoryAdapter> {
        NeckoCoreBuilder::default()
    }
}

impl<D, E> NeckoCore<D, E> {
    pub fn subscribe(&self, buffer_size: usize) -> (usize, mpsc::Receiver<E>) {
        let (tx, rx) = mpsc::channel(buffer_size);
        let id = self.last_channel_id.fetch_add(1, Ordering::Relaxed);
        self.channels.insert(id, tx);
        (id, rx)
    }

    pub fn unsubscribe(&self, id: usize) {
        self.channels.remove(&id);
    }

    fn broadcast(&self, event: E)
    where
        E: Clone
    {
        self.channels.retain(|_id, tx| {
            match tx.try_send(event.clone()) {
                Ok(_) => true,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    warn!("One of the receivers is too slow. Event dropped for this channel.");
                    true
                },
                Err(mpsc::error::TrySendError::Closed(_)) => false,
            }
        });
    }
}

impl<D, E> NeckoCore<D, E>
where
    E: Clone,
    NeckoEvent: TryInto<E>,
{
    pub async fn listen_channels(&self, mut rx: mpsc::Receiver<NeckoEvent>) {
        while let Some(event) = rx.recv().await {
            if let Ok(mapped_event) = event.try_into() {
                self.broadcast(mapped_event);
            }
        }
    }
}

impl<D, E> NeckoCore<D, E>
where
    D: DatabaseExt,
{
    pub fn new(
        db: Arc<D>,
        workers: Arc<DashMap<String, Worker>>
    ) -> Self {
        Self {
            db,
            last_channel_id: Arc::new(AtomicUsize::new(0)),
            channels: Arc::new(DashMap::new()),
            workers,
        }
    }

    pub fn db(&self) -> Arc<D> {
        self.db.clone()
    }

    pub async fn add_chain(&self, chain_config: ChainConfig) -> DbResult<DbChainId> {
        let (tokens, chain_data) = chain_config.into();
        let id = self.db.add_chain(&chain_data).await?;

        for token in tokens {
            self.db.add_token(&chain_data.name, &token).await?;
        }

        Ok(id)
    }

    pub async fn add_token(&self, chain_name: impl Into<String>, token: TokenConfig) -> DbResult<DbTokenId> {
        let token = token.into();
        self.db.add_token(&chain_name.into(), &token).await
    }

    pub async fn create_invoice(
        &self,
        payment_spec: PaymentSpec,
        payment_address: PaymentAddress,
        webhook_config: Option<WebhookConfig>,
        expiration_time: ExpirationTime,
    ) -> Result<Invoice, InvoiceCreationError> {
        let now = Utc::now();

        let expires_at = match expiration_time {
            ExpirationTime::Timestamp(timestamp) => timestamp,
            ExpirationTime::Duration(duration) => now + duration,
        };

        if expires_at < now {
            return Err(InvoiceCreationError::Validation("The expiration time has already passed".into()));
        }

        let (symbol, decimals) = match payment_spec.asset {
            PaymentAsset::Native => {
                let chain = self.db.get_chain(&payment_spec.network).await?
                    .ok_or_else(|| InvoiceCreationError::NetworkNotFound(payment_spec.network.clone()))?;

                (chain.native_symbol, chain.decimals)
            }
            PaymentAsset::Token(symbol) => {
                let decimals = self.db.get_token_decimals(&payment_spec.network, &symbol).await?
                    .ok_or_else(|| InvoiceCreationError::TokenNotFound {
                        symbol: symbol.clone(),
                        network: payment_spec.network.clone()
                    })?;

                (symbol, decimals)
            }
        };

        let (amount_human, amount_raw) = match payment_spec.amount {
            PaymentAmount::Raw(raw_amount) => {
                let amount_human = format_units(raw_amount, decimals)?;
                (amount_human, raw_amount)
            },
            PaymentAmount::Human(amount_str) => {
                let amount_raw = parse_units(&amount_str, decimals)?;
                (amount_str, amount_raw.into())
            }
        };

        if amount_raw == 0 {
            return Err(InvoiceCreationError::Validation("Payment amount is zero".into()));
        }

        let (address, address_index) = match payment_address {
            PaymentAddress::UseExisting(addr) => (addr, 0),
            PaymentAddress::GenerateNew => {
                let xpub = self.db.get_xpub(&payment_spec.network).await?
                    .ok_or_else(|| InvoiceCreationError::MissingXpub(payment_spec.network.clone()))?;

                let index = self.db.next_derivation_index(&xpub).await?;

                let worker = self.workers.get(&payment_spec.network)
                    .ok_or_else(|| InvoiceCreationError::WorkerNotInitialized(payment_spec.network.clone()))?;

                let address = worker.adapter.derive_address(xpub, index as u32)
                    .map_err(|e| InvoiceCreationError::Adapter(e.to_string()))?;

                (address, index as u32)
            }
            PaymentAddress::WithXpub { xpub, derivation_index } => {
                let worker = self.workers.get(&payment_spec.network)
                    .ok_or_else(|| InvoiceCreationError::WorkerNotInitialized(payment_spec.network.clone()))?;

                let address = worker.adapter.derive_address(xpub, derivation_index)
                    .map_err(|e| InvoiceCreationError::Adapter(e.to_string()))?;

                (address, derivation_index)
            }
        };

        let (webhook_url, webhook_secret, webhook_max_retries) = match webhook_config {
            Some(cfg) => (Some(cfg.url), Some(cfg.secret), Some(cfg.max_retries)),
            None => (None, None, None)
        };

        let invoice = Invoice {
            id: Uuid::new_v4(),
            address_index,
            address,
            amount: amount_human,
            amount_raw,
            paid: "0".to_string(),
            paid_raw: U256::from(0),
            token: symbol,
            network: payment_spec.network,
            decimals,
            webhook_url,
            webhook_secret,
            webhook_max_retries,
            created_at: now,
            expires_at,
            status: InvoiceStatus::Pending,
        };

        self.db.add_invoice(&invoice).await?;

        Ok(invoice)
    }
}

impl<D, E> NeckoCore<D, E> {
    pub fn parse_u256_as_string(&self, network: &str, value: U256, decimals: u8) -> Result<String, UnitConversionError> {
        let worker = self.workers.get(network)
            .ok_or_else(|| UnitConversionError::WorkerNotInitialized(network.to_string()))?;
        
        let res = worker.adapter.parse_u256_as_string(value, decimals)?;
        
        Ok(res)
    }

    pub fn parse_string_as_u256(&self, network: &str, value: &str, decimals: u8) -> Result<U256, UnitConversionError> {
        let worker = self.workers.get(network)
            .ok_or_else(|| UnitConversionError::WorkerNotInitialized(network.to_string()))?;

        let res = worker.adapter.parse_string_as_u256(value, decimals)?;

        Ok(res)
    }
}