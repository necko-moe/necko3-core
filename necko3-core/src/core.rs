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
use necko3_blockchain::traits::adapter::BlockchainAdapter;
use necko3_database::backends::in_memory::InMemoryAdapter;
use necko3_types::{Invoice, InvoiceStatus};
use crate::builder::chain_config::ChainConfig;
use crate::builder::invoice_config::{ExpirationTime, PaymentAddress, PaymentAmount, PaymentAsset, PaymentSpec, WebhookConfig};
use crate::builder::NeckoCoreBuilder;
use crate::builder::token_config::TokenConfig;
use crate::types::NeckoEvent;

pub struct Worker {
    pub adapter: Box<dyn BlockchainAdapter>,
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

    pub async fn add_chain(&self, chain_config: ChainConfig) -> anyhow::Result<()> {
        let (tokens, chain_data) = chain_config.into();
        self.db.add_chain(&chain_data).await?;

        for token in tokens {
            self.db.add_token(&chain_data.name, &token).await?;
        }

        Ok(())
    }

    pub async fn add_token(&self, chain_name: impl Into<String>, token: TokenConfig) -> anyhow::Result<()> {
        let token = token.into();
        self.db.add_token(&chain_name.into(), &token).await
    }

    pub async fn create_invoice(
        &self,
        payment_spec: PaymentSpec,
        payment_address: PaymentAddress,
        webhook_config: Option<WebhookConfig>,
        expiration_time: ExpirationTime,
    ) -> anyhow::Result<Invoice> {
        let now = Utc::now();

        let expires_at = match expiration_time {
            ExpirationTime::Timestamp(timestamp) => timestamp,
            ExpirationTime::Duration(duration) => now + duration,
        };

        if expires_at < now {
            anyhow::bail!("The expiration time has already passed")
        }

        let (symbol, decimals) = match payment_spec.asset {
            PaymentAsset::Native => {
                let chain = match self.db.get_chain(&payment_spec.network).await {
                    Ok(Some(chain)) => chain,
                    Ok(None) => anyhow::bail!("Unknown network: {}", payment_spec.network),
                    Err(e) => {
                        anyhow::bail!("Failed to get chain: {}", e);
                    }
                };

                (chain.native_symbol, chain.decimals)
            }
            PaymentAsset::Token(symbol) => {
                let decimals = match self.db.get_token_decimals(&payment_spec.network, &symbol).await {
                    Ok(Some(decimals)) => decimals,
                    Ok(None) => anyhow::bail!("Unknown token symbol {} on network: {}", symbol, payment_spec.network),
                    Err(e) => {
                        anyhow::bail!("Failed to get token decimals: {}", e)
                    }
                };

                (symbol, decimals)
            }
        };

        let (amount_human, amount_raw) = match payment_spec.amount {
            PaymentAmount::Raw(raw_amount) => {
                let amount_human = match format_units(raw_amount, decimals) {
                    Ok(amt) => amt,
                    Err(e) => {
                        anyhow::bail!("Failed to format payment amount: {}", e);
                    }
                };

                (amount_human, raw_amount)
            },
            PaymentAmount::Human(amount_str) => {
                let amount_raw = match parse_units(&amount_str, decimals) {
                    Ok(amt) => amt,
                    Err(e) => {
                        anyhow::bail!("Failed to parse payment amount: {}", e)
                    }
                };

                (amount_str, amount_raw.into())
            }
        };

        if amount_raw == 0 {
            anyhow::bail!("Payment amount is zero")
        }

        let (address, address_index) = match payment_address {
            PaymentAddress::UseExisting(addr) => (addr, 0),
            PaymentAddress::GenerateNew => {
                let xpub = match self.db.get_xpub(&payment_spec.network).await {
                    Ok(Some(xpub)) => xpub,
                    Ok(None) => anyhow::bail!("Unknown network: {}", payment_spec.network),
                    Err(e) => {
                        anyhow::bail!("Failed to get xpub: {}", e);
                    },
                };

                let index = match self.db.next_derivation_index(&xpub).await {
                    Ok(index) => index,
                    Err(e) => {
                        anyhow::bail!("Failed to get next derivation index: {}", e)
                    },
                };

                let address_res = match self.workers.get(&payment_spec.network) {
                    Some(worker) => worker.adapter.derive_address(xpub, index as u32),
                    None => {
                        anyhow::bail!("Worker for chain '{}' is not initialized", payment_spec.network)
                    },
                };

                let address = match address_res {
                    Ok(address) => address,
                    Err(e) => {
                        anyhow::bail!("Failed to derive address: {}", e);
                    },
                };

                (address, index as u32)
            }
            PaymentAddress::WithXpub { xpub, derivation_index } => {
                let address_res = match self.workers.get(&payment_spec.network) {
                    Some(worker) => worker.adapter.derive_address(xpub, derivation_index),
                    None => {
                        anyhow::bail!("Worker for chain '{}' is not initialized", payment_spec.network)
                    },
                };

                let address = match address_res {
                    Ok(address) => address,
                    Err(e) => {
                        anyhow::bail!("Failed to derive address: {}", e);
                    },
                };

                (address, derivation_index)
            }
        };

        let (webhook_url, webhook_secret, webhook_max_retries) = match webhook_config {
            Some(webhook_config) => {
                (Some(webhook_config.url),
                 Some(webhook_config.secret),
                 Some(webhook_config.max_retries))
            }
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

        if let Err(e) = self.db.add_invoice(&invoice).await {
            anyhow::bail!("Failed to insert invoice: {}", e);
        }

        Ok(invoice)
    }
}